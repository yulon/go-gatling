package gatling

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type recvData struct {
	data   []byte
	doneCh chan bool
}

type sendInfo struct {
	addr   net.Addr
	sender net.PacketConn
}

type Conn struct {
	id   uuid.UUID
	lcPr *Peer
	mtx  sync.Mutex

	sendInfos  []*sendInfo
	sendInfoIx uint

	nowRTT              int64
	nowSentPktSendCount int64
	minRTT              time.Duration

	lastSendTS time.Time
	lastRecvTS time.Time

	sendTimeout time.Duration
	recvTimeout time.Duration

	sendPktIDCount            uint64
	sendPktCaches             []*sendPktCache
	sendPktResenderIsRunning  bool
	sendPktResendErr          error
	sendPktCachesDecCond      *sync.Cond
	waitSendPktCachesDecCount uint64

	sentPktIDAppender        *idAppender
	sentPktIDBase            uint64
	sentPktIDBaseRepeatCount int

	someoneNoResendSentPktID uint64
	someoneNoResendSentTS    time.Time

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

	closeStatus byte
}

func newConn(id uuid.UUID, pr *Peer) *Conn {
	pr.use()
	now := time.Now()
	con := &Conn{
		id:                       id,
		lcPr:                     pr,
		nowRTT:                   int64(DefaultRTT),
		minRTT:                   DefaultRTT,
		lastSendTS:               now,
		lastRecvTS:               now,
		sendTimeout:              30 * time.Second,
		recvTimeout:              90 * time.Second,
		sentPktIDAppender:        newIDAppender(nil, nil),
		sendPktResenderIsRunning: false,
		recvPktIDAppender:        newIDAppender(&sync.Mutex{}, nil),
	}
	return con
}

var errIllegalAddr = errors.New("gatling: Illegal address.")
var errTimeout = errors.New("gatling: Connection is timeout.")

func (con *Conn) closeUS(recvErr error) error {
	if con.closeStatus > 1 {
		return nil
	}
	if recvErr == nil {
		recvErr = errClosed
		if con.closeStatus > 0 {
			for con.closeStatus == 1 {
				con.flushUS()
			}
			return nil
		}
		con.closeStatus = 1
		err := con.flushUS()
		if err != nil {
			return err
		}
		err = con.sendUS(pktClosed)
		if err != nil {
			return err
		}
		err = con.flushUS()
		if err != nil {
			return err
		}
	}

	con.lcPr.conMap.Delete(con.id)
	con.lcPr.unuse()

	con.sendPktResendErr = recvErr
	if con.waitSendPktCachesDecCount > 0 {
		con.sendPktCachesDecCond.Broadcast()
	}

	con.putRecvPkt(nil, recvErr)
	con.putReadStrmPkt(0, nil, recvErr)

	con.closeStatus = 2
	return nil
}

func (con *Conn) close(err error) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.closeUS(err)
}

func (con *Conn) Close() error {
	return con.close(nil)
}

func (con *Conn) IsClose() bool {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.closeStatus > 0
}

func (con *Conn) setRmtAddr(mainAddr net.Addr, ports ...uint16) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	mainUDPAddr := mainAddr.(*net.UDPAddr)
	con.sendInfos = []*sendInfo{&sendInfo{mainUDPAddr, nil}}

	for _, port := range ports {
		if port == uint16(mainUDPAddr.Port) {
			continue
		}
		con.sendInfos = append(con.sendInfos, &sendInfo{&net.UDPAddr{
			IP:   mainUDPAddr.IP,
			Port: int(port),
			Zone: mainUDPAddr.Zone,
		}, nil})
	}
}

func (con *Conn) handleRecvInfo(from net.Addr, to net.PacketConn) {
	udpAddr := from.(*net.UDPAddr)

	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.lastRecvTS = time.Now()

	for _, si := range con.sendInfos {
		siUDPAddr := si.addr.(*net.UDPAddr)
		if udpAddr.IP.Equal(siUDPAddr.IP) && udpAddr.Port == siUDPAddr.Port {
			if si.sender != nil && si.sender != to {
				si.sender = to
			}
			return
		}
	}
	con.sendInfos = append(con.sendInfos, &sendInfo{from, to})
}

var errRmtAddrLoss = errors.New("gatling: Remote address loss.")

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

func (con *Conn) ID() uuid.UUID {
	return con.id
}

func (con *Conn) RTT() time.Duration {
	return time.Duration(atomic.LoadInt64(&con.nowRTT))
}

func (con *Conn) PacketLoss() float32 {
	return float32(1) - float32(1)/float32(atomic.LoadInt64(&con.nowSentPktSendCount))
}

func (con *Conn) LastRecvTS() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.lastRecvTS
}

func (con *Conn) LastSendTS() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.lastSendTS
}

func (con *Conn) LastActivityTS() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.lastRecvTS.Sub(con.lastSendTS) > 0 {
		return con.lastRecvTS
	}
	return con.lastSendTS
}

func (con *Conn) SetSendTimeout(dur time.Duration) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.sendTimeout = dur
}

func (con *Conn) SetRecvTimeout(dur time.Duration) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.recvTimeout = dur
}

func (con *Conn) writeUS(b []byte, count int) (int, error) {
	if con.closeStatus > 1 {
		return 0, errClosed
	}

	con.sendInfoIx++
	ix := int(con.sendInfoIx % uint(len(con.sendInfos)))
	sendInfo := con.sendInfos[ix]

	var sz int
	var err error
	con.lastSendTS = time.Now()
	if sendInfo.sender == nil {
		sz, err = con.lcPr.writeTo(b, sendInfo.addr, count)
	} else {
		sz, err = sendInfo.sender.WriteTo(b, sendInfo.addr)
	}
	if err != nil {
		con.closeUS(err)
		return 0, err
	}
	return sz, nil
}

func (con *Conn) write(b []byte, count int) (int, error) {
	con.mtx.Lock()
	defer con.mtx.Unlock()
	return con.writeUS(b, count)
}

var errIntervalTooBrief = errors.New("gatling: Interval too brief.")

type sendPktCache struct {
	id          uint64
	p           []byte
	firstSendTS time.Time
	lastSendTS  time.Time
	sendCount   int64
}

func (con *Conn) sendCacheUS(spc *sendPktCache, count int) error {
	spc.sendCount++
	_, err := con.writeUS(spc.p, count)
	spc.lastSendTS = con.lastSendTS
	return err
}

func (con *Conn) sendCache(spc *sendPktCache, count int) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.sendCacheUS(spc, count)
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
		_, err := con.writeUS(p, 1)
		return err
	}

	spc := &sendPktCache{
		id: h.PktID,
		p:  p,
	}

	for {
		if con.closeStatus > 1 {
			if con.sendPktResendErr != nil {
				return con.sendPktResendErr
			}
			return errClosed
		}
		if len(con.sendPktCaches) < sendPktCachesMax {
			break
		}
		if con.sendPktCachesDecCond == nil {
			con.sendPktCachesDecCond = sync.NewCond(&con.mtx)
		}
		con.waitSendPktCachesDecCount++
		con.sendPktCachesDecCond.Wait()
		con.waitSendPktCachesDecCount--
	}

	if con.sentPktIDAppender.Has(spc.id) {
		if spc.id == con.someoneNoResendSentPktID {
			con.updateRTTAndSPSCUS(con.someoneNoResendSentTS.Sub(spc.firstSendTS), 1)
			con.someoneNoResendSentPktID = 0
		}
		return nil
	}

	con.sendPktCaches = append(con.sendPktCaches, spc)

	err := con.sendCacheUS(spc, 1)
	spc.firstSendTS = spc.lastSendTS
	if err != nil {
		return err
	}

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

			var timeoutSendPktCaches []*sendPktCache

			now := time.Now()
			minRTT := con.minRTT

			for i := len(con.sendPktCaches) - 1; i >= 0; i-- {
				spc := con.sendPktCaches[i]
				if now.Sub(spc.lastSendTS) < minRTT {
					continue
				}
				if now.Sub(spc.firstSendTS) > con.sendTimeout {
					con.closeUS(errTimeout)
					con.mtx.Unlock()
					return
				}
				err := con.sendCacheUS(spc, 1)
				if err != nil {
					con.mtx.Unlock()
					return
				}
				break
			}

			/*for _, spc := range con.sendPktCaches {
				if now.Sub(spc.firstSendTS) > 30*time.Second {
					con.closeUS(false)
					con.mtx.Unlock()
					fmt.Println("sendPktResender: send timeout", spc.id)
					return
				}
				if now.Sub(spc.lastSendTS) >= con.vRTT() {
					timeoutSendPktCaches = append(timeoutSendPktCaches, spc)
				}
			}*/

			con.mtx.Unlock()

			if len(timeoutSendPktCaches) == 0 {
				time.Sleep(minRTT)
				continue
			}

			dur := minRTT / time.Duration(len(timeoutSendPktCaches))
			for _, spc := range timeoutSendPktCaches {
				time.Sleep(dur)

				err := con.sendCache(spc, 3)
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

func (con *Conn) flushUS() error {
	for {
		if con.closeStatus > 1 {
			if con.sendPktResendErr != nil {
				return con.sendPktResendErr
			}
			return errClosed
		}
		if len(con.sendPktCaches) == 0 {
			return nil
		}
		if con.sendPktCachesDecCond == nil {
			con.sendPktCachesDecCond = sync.NewCond(&con.mtx)
		}
		con.waitSendPktCachesDecCount++
		con.sendPktCachesDecCond.Wait()
		con.waitSendPktCachesDecCount--
	}
}

func (con *Conn) flush() error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.flushUS()
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

	if time.Now().Sub(con.lastSendTS.Add(con.minRTT)) <= 0 {
		return errIntervalTooBrief
	}
	return con.sendUS(pktHeartbeat)
}

func (con *Conn) updateRTTAndSPSCUS(rtt time.Duration, sentPktSendCount int64) {
	atomic.StoreInt64(&con.nowRTT, int64(rtt))
	atomic.StoreInt64(&con.nowSentPktSendCount, sentPktSendCount)
	if rtt < con.minRTT {
		con.minRTT = rtt
	}
}

func (con *Conn) unresend(pktIDs ...uint64) {
	now := time.Now()

	con.mtx.Lock()

	sendPktCachesBeforeLen := len(con.sendPktCaches)

	isRms := false
	for _, pktID := range pktIDs {
		if !con.sentPktIDAppender.TryAdd(pktID, nil) {
			continue
		}
		isRm := false
		for i, spc := range con.sendPktCaches {
			if spc.id == pktID {
				nowRTT := now.Sub(spc.firstSendTS)
				con.updateRTTAndSPSCUS(nowRTT, spc.sendCount)

				if i == len(con.sendPktCaches)-1 {
					con.sendPktCaches = con.sendPktCaches[:i]
					break
				}
				sendPktCacheRights := make([]*sendPktCache, len(con.sendPktCaches[i+1:]))
				copy(sendPktCacheRights, con.sendPktCaches[i+1:])
				con.sendPktCaches = con.sendPktCaches[:len(con.sendPktCaches)-1]
				copy(con.sendPktCaches[i:], sendPktCacheRights)

				isRm = true
				isRms = true
				break
			}
		}
		if !isRm && con.someoneNoResendSentPktID == 0 {
			con.someoneNoResendSentPktID = pktID
			con.someoneNoResendSentTS = now
		}
	}
	if isRms {
		if con.sentPktIDBase == con.sentPktIDAppender.baseID {
			con.sentPktIDBaseRepeatCount++
			if con.sentPktIDBaseRepeatCount > 2 {
				con.sentPktIDBaseRepeatCount = 0
				c := 0
				for _, spc := range con.sendPktCaches {
					if c > 31 {
						break
					}
					err := con.sendCacheUS(spc, 1)
					if err != nil {
						con.mtx.Unlock()
						return
					}
					c++
				}
			}
		} else {
			con.sentPktIDBase = con.sentPktIDAppender.baseID
			con.sentPktIDBaseRepeatCount = 0
		}
	}
	if con.waitSendPktCachesDecCount > 0 && len(con.sendPktCaches) < sendPktCachesBeforeLen {
		con.mtx.Unlock()
		con.sendPktCachesDecCond.Broadcast()
		return
	}

	con.mtx.Unlock()
}

var errClosed = errors.New("gatling: Connection is closed.")

func (con *Conn) handleRecvPacket(from net.Addr, h *header, r *bytes.Buffer) {
	if isReliableType[h.Type] && h.PktID > 0 {
		if h.Type == pktRequestPorts {
			con.send(pktResponsePorts, h.PktID, con.lcPr.lnPorts)
			con.recvPktIDAppender.TryAdd(h.PktID, nil)
			return
		}
		con.send(pktReceiveds, h.PktID)
		if !con.recvPktIDAppender.TryAdd(h.PktID, nil) {
			return
		}
	}

	switch h.Type {

	case pktData:
		fallthrough
	case pktUnreliableData:
		con.putRecvPkt(r.Bytes(), nil)

	case pktReceiveds:
		pktIDs := make([]uint64, r.Len()/8)
		binary.Read(r, binary.LittleEndian, &pktIDs)
		con.unresend(pktIDs...)

	case pktRequests:

	case pktResponsePorts:
		var reqPrtsPktID uint64
		binary.Read(r, binary.LittleEndian, &reqPrtsPktID)
		con.unresend(reqPrtsPktID)

		ports := make([]uint16, r.Len()/2)
		binary.Read(r, binary.LittleEndian, &ports)
		con.setRmtAddr(from, ports...)

	case pktUpdatePorts:
		ports := make([]uint16, r.Len()/2)
		binary.Read(r, binary.LittleEndian, &ports)
		con.setRmtAddr(from, ports...)

	case pktClosed:
		fallthrough

	case pktInvalid:
		con.close(errClosed)

	case pktStreamData:
		var strmPktIx uint64
		binary.Read(r, binary.LittleEndian, &strmPktIx)
		con.putReadStrmPkt(strmPktIx, r.Bytes(), nil)
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
