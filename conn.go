package gatling

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type recvData struct {
	data   []byte
	doneCh chan bool
}

type portAndSender struct {
	port      uint16
	udpSender *net.UDPConn
}

type Conn struct {
	id   uuid.UUID
	lcPr *Peer
	mtx  sync.Mutex

	rmtIP          string
	rmtPorts       []uint16
	rmtPortPtr     uint
	rmtLimitedPort *portAndSender

	rtt        int64
	sendCount  int64
	lastSendTS time.Time

	reliableCount        uint64
	reliableMap          map[uint64]*reliableCache
	reliableSenderCaller sync.Once

	recvReliableIDAppender *idAppender

	recvPktCache [][]byte
	recvPktErr   error
	recvPktMtx   sync.Mutex
	recvPktCond  *sync.Cond

	recvStrmIDAppender *idAppender
	recvStrmBuf        []byte
	recvStrmErr        error
	recvStrmMtx        sync.Mutex
	recvStrmCond       *sync.Cond

	sendStrmPktCount uint64

	dialErrCh    chan error
	hasDialErrCh uint32

	isClosed  bool
	closeCond *sync.Cond
}

func newConn(id uuid.UUID, pr *Peer, rmtIP string) *Conn {
	con := &Conn{
		id:                     id,
		lcPr:                   pr,
		rmtIP:                  rmtIP,
		rtt:                    int64(500 * time.Millisecond),
		reliableMap:            map[uint64]*reliableCache{},
		recvReliableIDAppender: newIDAppender(nil),
	}
	con.recvStrmIDAppender = newIDAppender(func(iads []idAndData) {
		for _, iad := range iads {
			con.putRecvStrm(iad.data.([]byte), nil)
		}
	})
	return con
}

var errIllegalAddr = errors.New("gatling: Illegal address.")
var errTimeout = errors.New("gatling: Connection is timeout.")

func dial(pr *Peer, addr string) (*Conn, error) {
	ipAndPort := strings.Split(addr, ":")
	if len(ipAndPort) != 2 {
		return nil, errIllegalAddr
	}

	port64, err := strconv.ParseUint(ipAndPort[1], 10, 16)
	if err != nil {
		return nil, err
	}

	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	dialErrCh := make(chan error, 1)

	con := newConn(id, pr, ipAndPort[0])
	con.dialErrCh = dialErrCh
	con.hasDialErrCh = 1
	con.setRmtPorts(uint16(port64))

	pr.conMap.Store(id, con)
	con.send(pktRequestPorts)

	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(15 * time.Second)
		timeoutCh <- true
	}()

	select {
	case err = <-dialErrCh:
		if err != nil {
			pr.conMap.Delete(id)
			return nil, err
		}
	case <-timeoutCh:
		pr.conMap.Delete(id)
		return nil, errTimeout
	}

	return con, nil
}

func (con *Conn) close(isActive bool) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.isClosed {
		return errClosed
	}

	if isActive {
		if con.closeCond != nil {
			con.closeCond.Wait()
			return nil
		}
		con.sendUS(pktClosed)
		con.closeCond = sync.NewCond(&con.mtx)
		con.closeCond.Wait()
	} else if con.closeCond != nil {
		return nil
	}

	con.lcPr.conMap.Delete(con.id)

	con.putRecvPkt(nil, errClosed)
	con.putRecvStrm(nil, errClosed)
	con.isClosed = true

	return nil
}

func (con *Conn) Close() error {
	return con.close(true)
}

func (con *Conn) setRmtPorts(ports ...uint16) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.rmtPorts = ports
}

func (con *Conn) setRmtLimitedPort(port uint16, udpSender *net.UDPConn) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.rmtLimitedPort = &portAndSender{port, udpSender}
}

/*
func (con *Conn) addRmtPortInfos(portInfos ...portInfo) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	for _, portInfo := range portInfos {
		rmtPortInfoPtr, isExist := con.rmtPortInfoMap[portInfo.port]
		if isExist {
			if portInfo.udpSender == nil && rmtPortInfoPtr.udpSender != nil {
				rmtPortInfoPtr.udpSender = nil
			}
			continue
		}
		con.rmtPortInfos = append(con.rmtPortInfos)
		con.rmtPortInfoMap[portInfo.port] = &con.rmtPortInfos[len(con.rmtPortInfos)-1]
	}
}

func (con *Conn) addRmtPorts(ports ...uint16) {
	portInfos := make([]portInfo, len(ports))
	for i, port := range ports {
		portInfos[i].port = port
	}
	con.addRmtPortInfos(portInfos...)
}
*/

func (con *Conn) rmtPort() uint16 {
	con.rmtPortPtr++
	return con.rmtPorts[int(con.rmtPortPtr%uint(len(con.rmtPorts)))]
}

func (con *Conn) rmtUDPAddrAndUDPSender() (*net.UDPAddr, *net.UDPConn, error) {
	var pas portAndSender

	if con.rmtLimitedPort != nil {
		pas = *con.rmtLimitedPort
	} else {
		pas.port = con.rmtPort()
		pas.udpSender = con.lcPr.udpLnrs[con.lcPr.lnIx()]
	}

	udpAddr, err := net.ResolveUDPAddr("udp", con.rmtIP+":"+strconv.FormatUint(uint64(pas.port), 10))
	if err != nil {
		return nil, nil, err
	}
	return udpAddr, pas.udpSender, nil
}

func (con *Conn) RTT() time.Duration {
	return time.Duration(atomic.LoadInt64(&con.rtt))
}

func (con *Conn) PacketLoss() float32 {
	return float32(1) - float32(1)/float32(atomic.LoadInt64(&con.sendCount))
}

func (con *Conn) writeUS(b []byte) (int, error) {
	if con.isClosed {
		return 0, errClosed
	}
	udpAddr, udpSender, err := con.rmtUDPAddrAndUDPSender()
	if err != nil {
		return 0, err
	}
	con.lastSendTS = time.Now()
	return udpSender.WriteToUDP(b, udpAddr)
}

func (con *Conn) write(b []byte) (int, error) {
	con.mtx.Lock()
	defer con.mtx.Unlock()
	return con.writeUS(b)
}

var errIntervalTooBrief = errors.New("gatling: Interval too brief.")

type reliableCache struct {
	ts        time.Time
	sendCount int64
	data      []byte
}

func (con *Conn) sendUS(typ byte, others ...interface{}) error {
	isReliable := isReliableType[typ]

	var rc uint64
	if isReliable {
		rc = atomic.AddUint64(&con.reliableCount, 1)
	} else {
		rc = atomic.LoadUint64(&con.reliableCount)
	}
	h := header{
		con.id,
		rc,
		typ,
	}
	p := makePacket(&h, others...)

	if isReliable {
		con.reliableMap[rc] = &reliableCache{
			time.Now(),
			1,
			p,
		}
		con.reliableSenderCaller.Do(func() {
			go func() {
				for {
					time.Sleep(con.RTT() * time.Duration(2))

					con.mtx.Lock()

					var cache *reliableCache
					for _, cache = range con.reliableMap {
						now := time.Now()
						dRTT := con.RTT() * time.Duration(2)
						dur := now.Sub(cache.ts)
						if dur >= dRTT {
							break
						}
					}
					if cache == nil {
						con.mtx.Unlock()
						continue
					}

					_, err := con.writeUS(cache.data)
					if err != nil {
						con.mtx.Unlock()
						return
					}
					cache.sendCount++

					con.mtx.Unlock()
				}
			}()
		})
	}

	_, err := con.writeUS(p)
	return err
}

func (con *Conn) send(typ byte, others ...interface{}) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()
	return con.sendUS(typ, others...)
}

func (con *Conn) UnreliableSend(data []byte) error {
	return con.send(pktUnreliable, data)
}

func (con *Conn) Send(data []byte) error {
	return con.send(pktReliable, data)
}

func (con *Conn) Pace() error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if time.Now().Sub(con.lastSendTS.Add(con.RTT()*2)) > 0 {
		return errIntervalTooBrief
	}
	return con.sendUS(pktHeartbeat)
}

var errClosed = errors.New("gatling: Connection is closed.")

func (con *Conn) handleRecvPacket(h *header, other []byte) {
	if isReliableType[h.Type] && h.ReliableCount > 0 {
		con.send(pktReceivedReliables, h.ReliableCount)
		if !con.recvReliableIDAppender.TryAdd(h.ReliableCount, nil) {
			return
		}
	}

	switch h.Type {

	case pktUnreliable:
		con.putRecvPkt(other, nil)

	case pktReliable:
		con.putRecvPkt(other, nil)

	case pktReceivedReliables:
		ids := make([]uint64, len(other)/8)
		binary.Read(bytes.NewReader(other), binary.LittleEndian, ids)
		con.mtx.Lock()
		for _, id := range ids {
			cache, loaded := con.reliableMap[id]
			if loaded {
				atomic.StoreInt64(&con.rtt, int64(time.Now().Sub(cache.ts)))
				atomic.StoreInt64(&con.sendCount, cache.sendCount)
			}
			delete(con.reliableMap, id)
		}
		if len(con.reliableMap) == 0 && con.closeCond != nil {
			con.mtx.Unlock()
			con.closeCond.Broadcast()
			return
		}
		con.mtx.Unlock()

	case pktRequestReliables:

	case pktRequestPorts:
		con.send(pktUpdatePorts, con.lcPr.lnPorts)

	case pktUpdatePorts:
		ports := make([]uint16, len(other)/2)
		binary.Read(bytes.NewReader(other), binary.LittleEndian, ports)
		con.setRmtPorts(ports...)
		if atomic.SwapUint32(&con.hasDialErrCh, 0) == 1 {
			con.dialErrCh <- nil
			con.dialErrCh = nil
		}

	case pktClosed:
		fallthrough
	case pktInvalid:
		con.close(false)

	case pktStream:
		var strmPktID uint64
		binary.Read(bytes.NewReader(other), binary.LittleEndian, &strmPktID)
		con.recvStrmIDAppender.TryAdd(strmPktID, other[8:])
	}
}

func (con *Conn) putRecvPkt(data []byte, err error) {
	con.recvPktMtx.Lock()
	if err == nil {
		dataCpy := make([]byte, len(data))
		copy(dataCpy, data)
		con.recvPktCache = append(con.recvPktCache, dataCpy)
	} else {
		con.recvPktErr = err
	}
	if con.recvPktCond != nil {
		con.recvPktMtx.Unlock()
		con.recvPktCond.Broadcast()
		return
	}
	con.recvPktMtx.Unlock()
}

func (con *Conn) Recv() ([]byte, error) {
	con.recvPktMtx.Lock()
	defer con.recvPktMtx.Unlock()

	for {
		if len(con.recvPktCache) > 0 {
			data := con.recvPktCache[0]
			con.recvPktCache = con.recvPktCache[1:]
			return data, nil
		} else if con.recvPktErr != nil {
			return nil, con.recvPktErr
		}
		if con.recvPktCond == nil {
			con.recvPktCond = sync.NewCond(&con.recvPktMtx)
		}
		con.recvPktCond.Wait()
	}
}

func (con *Conn) Write(b []byte) (int, error) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	sz := 0
	for {
		if len(b) == 0 {
			return sz, nil
		}
		var data []byte
		if len(b) > 1024 {
			data = b[:1024]
			b = b[1024:]
		} else {
			data = b
			b = nil
		}
		con.sendStrmPktCount++
		err := con.sendUS(pktStream, con.sendStrmPktCount, data)
		if err != nil {
			return 0, err
		}
		sz += len(data)
	}
}

func (con *Conn) putRecvStrm(data []byte, err error) {
	con.recvStrmMtx.Lock()
	if err == nil {
		con.recvStrmBuf = append(con.recvStrmBuf, data...)
	} else {
		con.recvStrmErr = err
	}
	if con.recvStrmCond != nil {
		con.recvStrmMtx.Unlock()
		con.recvStrmCond.Broadcast()
		return
	}
	con.recvStrmMtx.Unlock()
}

func (con *Conn) Read(b []byte) (int, error) {
	con.recvStrmMtx.Lock()
	defer con.recvStrmMtx.Unlock()

	for {
		if len(con.recvStrmBuf) > 0 {
			sz := copy(b, con.recvStrmBuf)
			if sz < len(con.recvStrmBuf) {
				con.recvStrmBuf = con.recvStrmBuf[sz:]
			} else {
				con.recvStrmBuf = nil
			}
			return sz, nil
		} else if con.recvStrmErr != nil {
			con.recvStrmMtx.Unlock()
			return 0, con.recvStrmErr
		}
		if con.recvStrmCond == nil {
			con.recvStrmCond = sync.NewCond(&con.recvStrmMtx)
		}
		con.recvStrmCond.Wait()
	}
}
