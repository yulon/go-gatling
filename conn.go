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

	sendPktIDCount           uint64
	sendPktCaches            []*sendPktCache
	sendPktIDAppender        *idAppender
	sendPktCachesNoMaxCond   *sync.Cond
	preSendPktCount          uint64
	sendPktResenderIsRunning bool

	recvPktIDAppender *idAppender

	recvPktCache [][]byte
	recvPktErr   error
	recvPktMtx   sync.Mutex
	recvPktCond  *sync.Cond

	wStrmPktCount uint64
	wStrmMtx      sync.Mutex

	rStrmIDAppender *idAppender
	rStrmPkts       [][]byte
	rStrmBuf        *bytes.Buffer
	rStrmErr        error
	rStrmMtx        sync.Mutex
	rStrmCond       *sync.Cond

	dialErrCh    chan error
	hasDialErrCh uint32

	isClosed  bool
	closeCond *sync.Cond
}

func newConn(id uuid.UUID, pr *Peer, rmtIP string) *Conn {
	pr.use()
	con := &Conn{
		id:                       id,
		lcPr:                     pr,
		rmtIP:                    rmtIP,
		rtt:                      int64(500 * time.Millisecond),
		sendPktIDAppender:        newIDAppender(nil, nil),
		sendPktResenderIsRunning: false,
		recvPktIDAppender:        newIDAppender(&sync.Mutex{}, nil),
	}
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

	select {
	case err = <-dialErrCh:
		if err != nil {
			con.closeUS(false)
			return nil, err
		}
	case <-time.After(5 * time.Second):
		con.closeUS(false)
		return nil, errTimeout
	}

	return con, nil
}

func (con *Conn) closeUS(isActive bool) error {
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
	con.lcPr.unuse()

	con.putRecvPkt(nil, errClosed)
	con.putReadStrmPkt(0, nil, errClosed)
	con.isClosed = true

	return nil
}

func (con *Conn) close(isActive bool) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.closeUS(isActive)
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

var errRmtAddrLoss = errors.New("gatling: Remote address loss.")

func (con *Conn) rmtUDPAddrAndUDPSender() (*net.UDPAddr, *net.UDPConn, error) {
	var pas portAndSender

	if len(con.rmtPorts) > 0 {
		pas.port = con.rmtPort()
		var err error
		pas.udpSender, err = con.lcPr.udpLnr()
		if err != nil {
			return nil, nil, err
		}
	} else if con.rmtLimitedPort != nil {
		pas = *con.rmtLimitedPort
	} else {
		return nil, nil, errRmtAddrLoss
	}

	udpAddr, err := net.ResolveUDPAddr("udp", con.rmtIP+":"+strconv.FormatUint(uint64(pas.port), 10))
	if err != nil {
		return nil, nil, err
	}
	return udpAddr, pas.udpSender, nil
}

func (con *Conn) LocalAddr() net.Addr {
	return nil
}

func (con *Conn) RemoteAddr() net.Addr {
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	return nil
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	return nil
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

type sendPktCache struct {
	id          uint64
	p           []byte
	firstSendTS time.Time
	lastSendTS  time.Time
	sendCount   int64
}

func (con *Conn) sendCacheUS(spc *sendPktCache) error {
	spc.sendCount++
	spc.lastSendTS = time.Now()
	_, err := con.writeUS(spc.p)
	return err
}

func (con *Conn) sendCache(spc *sendPktCache) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()
	return con.sendCacheUS(spc)
}

func (con *Conn) sendUS(typ byte, others ...interface{}) error {
	isReliable := isReliableType[typ]

	if isReliable {
		con.sendPktIDCount++
	}

	h := header{
		ID:    con.id,
		PktID: con.sendPktIDCount,
		Type:  typ,
	}
	p := makePacket(&h, others...)

	if !isReliable {
		_, err := con.writeUS(p)
		return err
	}

	spc := &sendPktCache{
		id: h.PktID,
		p:  p,
	}
	err := con.sendCacheUS(spc)
	spc.firstSendTS = spc.lastSendTS
	if err != nil {
		return err
	}

	for len(con.sendPktCaches) == sendPktCachesMax {
		if con.sendPktCachesNoMaxCond == nil {
			con.sendPktCachesNoMaxCond = sync.NewCond(&con.mtx)
		}
		con.preSendPktCount++
		con.sendPktCachesNoMaxCond.Wait()
		con.preSendPktCount--
	}

	if con.sendPktIDAppender.Has(spc.id) {
		return nil
	}
	con.sendPktCaches = append(con.sendPktCaches, spc)

	if con.sendPktResenderIsRunning {
		return nil
	}

	go func() {
		for {
			con.mtx.Lock()

			if len(con.sendPktCaches) == 0 {
				con.sendPktResenderIsRunning = false
				con.mtx.Unlock()
				return
			}

			/*if spc := con.sendPktCaches[len(con.sendPktCaches)-1]; time.Now().Sub(spc.lastSendTS) > con.RTT() {
				err := con.sendCacheUS(spc)
				if err != nil {
					con.mtx.Unlock()
					fmt.Println("sendPktSender4:", err)
					return
				}
			}*/

			var timeoutSendPktCaches []*sendPktCache

			for _, spc := range con.sendPktCaches {
				now := time.Now()
				/*if now.Sub(spc.firstSendTS) > 10*time.Second {
					con.closeUS(false)
					con.mtx.Unlock()
					fmt.Println("sendPktSender3: send timeout", spc.h.PktID)
					return
				}*/
				if now.Sub(spc.lastSendTS) > con.RTT() {
					timeoutSendPktCaches = append(timeoutSendPktCaches, spc)
					/*if len(timeoutSendPktCaches) >= 1024 {
						break
					}*/
				}
			}

			con.mtx.Unlock()

			if len(timeoutSendPktCaches) == 0 {
				time.Sleep(con.RTT())
				continue
			}

			for _, spc := range timeoutSendPktCaches {
				time.Sleep(con.RTT() / time.Duration(len(timeoutSendPktCaches)))

				err := con.sendCache(spc)
				if err != nil {
					return
				}
			}
		}
	}()

	con.sendPktResenderIsRunning = true

	return nil
}

func (con *Conn) send(typ byte, others ...interface{}) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()
	return con.sendUS(typ, others...)
}

func (con *Conn) UnreliableSend(data []byte) error {
	return con.send(pktUnreliableData, data)
}

func (con *Conn) Send(data []byte) error {
	return con.send(pktData, data)
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
	if isReliableType[h.Type] && h.PktID > 0 {
		con.send(pktReceivedDatas, h.PktID)
		if !con.recvPktIDAppender.TryAdd(h.PktID, nil) {
			return
		}
	}

	switch h.Type {

	case pktData:
		fallthrough
	case pktUnreliableData:
		con.putRecvPkt(other, nil)

	case pktReceivedDatas:
		pktIDs := make([]uint64, len(other)/8)
		binary.Read(bytes.NewReader(other), binary.LittleEndian, pktIDs)

		con.mtx.Lock()

		for _, pktID := range pktIDs {
			if !con.sendPktIDAppender.TryAdd(pktID, nil) {
				continue
			}
			for i, spc := range con.sendPktCaches {
				if spc.id == pktID {
					//fmt.Println("rm", pktID)

					atomic.StoreInt64(&con.rtt, int64(time.Now().Sub(spc.firstSendTS)))
					atomic.StoreInt64(&con.sendCount, spc.sendCount)

					if i == len(con.sendPktCaches)-1 {
						con.sendPktCaches = con.sendPktCaches[:i]
						break
					}
					newSendPktSPCs := make([]*sendPktCache, len(con.sendPktCaches)-1)
					copy(newSendPktSPCs, con.sendPktCaches[:i])
					copy(newSendPktSPCs[i:], con.sendPktCaches[i+1:])
					con.sendPktCaches = newSendPktSPCs
					break
				}
				/*spc.lossCount++
				if spc.lossCount > 2 {
					fmt.Println("lossCount send", spc.h.PktID)
					err := con.sendCacheUS(spc)
					if err != nil {
						con.mtx.Unlock()
						return
					}
				}*/
			}
		}
		if con.preSendPktCount > 0 && len(con.sendPktCaches) < sendPktCachesMax {
			con.mtx.Unlock()
			con.sendPktCachesNoMaxCond.Broadcast()
			return
		}

		con.mtx.Unlock()

	case pktRequestDatas:

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

	case pktStreamData:
		var strmPktIx uint64
		binary.Read(bytes.NewReader(other), binary.LittleEndian, &strmPktIx)
		con.putReadStrmPkt(strmPktIx, other[8:], nil)
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
	con.wStrmMtx.Lock()
	defer con.wStrmMtx.Unlock()

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
		con.wStrmPktCount++
		err := con.send(pktStreamData, con.wStrmPktCount, data)
		if err != nil {
			return 0, err
		}
		sz += len(data)
	}
}

func (con *Conn) putReadStrmPkt(ix uint64, data []byte, err error) {
	con.rStrmMtx.Lock()
	if err == nil {
		if con.rStrmBuf == nil {
			con.rStrmBuf = bytes.NewBuffer([]byte{})
			con.rStrmIDAppender = newIDAppender(nil, func(iads []idAndData) {
				for _, iad := range iads {
					con.rStrmBuf.Write(iad.data.([]byte))
				}
			})
		}
		if con.rStrmIDAppender.TryAdd(ix, data) == false {
			con.rStrmMtx.Unlock()
			panic("putReadStrmPkt: duplicate index")
		}
	} else {
		con.rStrmErr = err
	}
	if con.rStrmCond != nil {
		con.rStrmMtx.Unlock()
		con.rStrmCond.Broadcast()
		return
	}
	con.rStrmMtx.Unlock()
}

func (con *Conn) Read(b []byte) (int, error) {
	con.rStrmMtx.Lock()
	defer con.rStrmMtx.Unlock()

	for {
		if con.rStrmBuf != nil && con.rStrmBuf.Len() > 0 {
			sz, err := con.rStrmBuf.Read(b)
			if err != nil {
				panic(err)
			}
			return sz, nil
		} else if con.rStrmErr != nil {
			return 0, con.rStrmErr
		}
		if con.rStrmCond == nil {
			con.rStrmCond = sync.NewCond(&con.rStrmMtx)
		}
		con.rStrmCond.Wait()
	}
}
