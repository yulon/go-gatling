package gatling

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
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
	id           uuid.UUID
	lcPr         *Peer
	rmtPortsHash uint64
	mtx          sync.Mutex

	sendInfos  []*sendInfo
	sendInfoIx uint

	nowRTT              time.Duration
	nowSentPktSendCount int
	minRTT              time.Duration

	lastWriteTime time.Time
	lastReadTime  time.Time

	handshakeTimeout time.Duration
	isHandshaked     bool

	writeTimeout time.Duration
	readTimeout  time.Duration

	sendPktIDCount uint64

	resendPkts                  []*resendPktCtx
	resendPktErr                error
	pktTimeoutResenderIsRunning bool

	sentPktIDAppender *idAppender
	onSentPktCond     *sync.Cond
	WaitSentPktCount  uint64

	sentPktIDBaseCache       uint64
	sentPktIDBaseRepeatCount int

	someoneSentPktID uint64
	someonePktSentTS time.Time

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

	closeState byte
}

func newConn(pr *Peer, id uuid.UUID) *Conn {
	pr.use()
	now := time.Now()
	con := &Conn{
		id:                          id,
		lcPr:                        pr,
		nowRTT:                      DefaultRTT,
		minRTT:                      DefaultRTT,
		lastWriteTime:               now,
		lastReadTime:                now,
		handshakeTimeout:            10 * time.Second,
		writeTimeout:                30 * time.Second,
		readTimeout:                 90 * time.Second,
		sentPktIDAppender:           newIDAppender(nil, nil),
		pktTimeoutResenderIsRunning: false,
		recvPktIDAppender:           newIDAppender(&sync.Mutex{}, nil),
	}
	return con
}

var errIllegalAddr = errors.New("gatling: Illegal address.")
var errTimeout = errors.New("gatling: Connection is timeout.")

func (con *Conn) closeUS(recvErr error) error {
	if con.closeState > 1 {
		return nil
	}
	if recvErr == nil {
		recvErr = errClosed
		if con.closeState > 0 {
			for con.closeState == 1 {
				con.flushUS()
			}
			return nil
		}
		con.closeState = 1
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

	con.sendInfos = nil

	con.resendPktErr = recvErr
	if con.WaitSentPktCount > 0 {
		con.onSentPktCond.Broadcast()
	}

	con.putRecvPkt(nil, recvErr)
	con.putReadStrmPkt(0, nil, recvErr)

	con.closeState = 2
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

func (con *Conn) tryCloseWhenTimeoutUS() bool {
	if con.isHandshaked || con.rmtPortsHash == 0 {
		con.closeUS(errTimeout)
		return true
	}

	portsCache.Delete(con.sendInfos[0].addr.String(), con.rmtPortsHash)

	con.rmtPortsHash = 0
	con.sendInfos = con.sendInfos[:1]

	con.lastReadTime = con.lastWriteTime
	for _, rspc := range con.resendPkts {
		rspc.firstSendTime = rspc.lastSendTime
	}
	return false
}

func (con *Conn) IsClose() bool {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.closeState > 0
}

func (con *Conn) setRmtAddrUS(referAddr net.Addr, ports ...uint16) {
	referUDPAddr := referAddr.(*net.UDPAddr)
	if len(ports) == 0 {
		con.sendInfos = []*sendInfo{&sendInfo{referUDPAddr, nil}}
		return
	}
	for _, port := range ports {
		con.sendInfos = append(con.sendInfos, &sendInfo{&net.UDPAddr{
			IP:   referUDPAddr.IP,
			Port: int(port),
			Zone: referUDPAddr.Zone,
		}, nil})
	}
}

func (con *Conn) setRmtAddr(mainAddr net.Addr, ports ...uint16) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.setRmtAddrUS(mainAddr, ports...)
}

func (con *Conn) handleRecvInfo(from net.Addr, to net.PacketConn, h *header) {
	udpAddr := from.(*net.UDPAddr)

	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.lastReadTime = time.Now()

	if !con.isHandshaked {
		con.isHandshaked = true
	}

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

var errPortsHashMismatch = errors.New("gatling: ports hash mismatch.")

func (con *Conn) handleRecvPacket(from net.Addr, to net.PacketConn, h *header, r *bytes.Buffer) {
	if isReliableType[h.Type] && h.PktID > 0 {
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

	case pktUpdatePorts:
		ports := make([]uint16, r.Len()/2)
		binary.Read(r, binary.LittleEndian, &ports)

		con.mtx.Lock()

		if con.rmtPortsHash == h.SrcPortsHash {
			con.mtx.Unlock()
			return
		}

		portsHash := hashU16(ports)
		if portsHash != h.SrcPortsHash {
			con.closeUS(errPortsHashMismatch)
			con.mtx.Unlock()
			return
		}

		con.rmtPortsHash = portsHash
		con.setRmtAddrUS(from, ports...)

		rmtAddr := con.sendInfos[0].addr.String()

		con.mtx.Unlock()

		portsCache.Store(rmtAddr, portsHash, ports)

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

var errRmtAddrLoss = errors.New("gatling: Remote address loss.")

func (con *Conn) LocalAddr() net.Addr {
	return con.lcPr.Addr()
}

func (con *Conn) RemoteAddr() net.Addr {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if len(con.sendInfos) == 0 {
		return nil
	}
	return con.sendInfos[0].addr
}

func (con *Conn) ID() uuid.UUID {
	return con.id
}

func (con *Conn) RTT() time.Duration {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.nowRTT
}

func (con *Conn) PacketLoss() float32 {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return float32(1) - float32(1)/float32(con.nowSentPktSendCount)
}

func (con *Conn) LastReadTime() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.lastReadTime
}

func (con *Conn) LastWriteTime() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.lastWriteTime
}

func (con *Conn) LastActivityTime() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.lastReadTime.Sub(con.lastWriteTime) > 0 {
		return con.lastReadTime
	}
	return con.lastWriteTime
}

func (con *Conn) SetWriteTimeout(dur time.Duration) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.writeTimeout = dur
}

func (con *Conn) SetReadTimeout(dur time.Duration) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.readTimeout = dur
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return errClosed
	}
	con.readTimeout = t.Sub(time.Now())
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return errClosed
	}
	con.writeTimeout = t.Sub(time.Now())
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return errClosed
	}
	dur := t.Sub(time.Now())
	con.readTimeout = dur
	con.writeTimeout = dur
	return nil
}

func (con *Conn) writeUS(b []byte, count int) (int, error) {
	if con.closeState > 1 {
		return 0, errClosed
	}

	con.sendInfoIx++
	ix := int(con.sendInfoIx % uint(len(con.sendInfos)))
	sendInfo := con.sendInfos[ix]

	var sz int
	var err error
	con.lastWriteTime = time.Now()
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

func (con *Conn) sendUS(typ byte, others ...interface{}) error {
	isReliable := isReliableType[typ]

	if isReliable {
		con.sendPktIDCount++
	}

	h := header{
		SrcPortsHash:  con.lcPr.lnPortsHash,
		DestPortsHash: con.rmtPortsHash,
		ID:            con.id,
		PktID:         con.sendPktIDCount,
		Type:          typ,
	}
	p := makePacket(&h, others...)

	if !isReliable {
		_, err := con.writeUS(p, 1)
		return err
	}

	rspc := &resendPktCtx{
		id: h.PktID,
		p:  p,
	}

	err := con.resendUS(rspc, 1)
	rspc.firstSendTime = rspc.lastSendTime
	if err != nil {
		return err
	}

	for {
		if con.closeState > 1 {
			if con.resendPktErr != nil {
				return con.resendPktErr
			}
			return errClosed
		}

		if len(con.resendPkts) < resendPktsMax {
			break
		}

		if con.onSentPktCond == nil {
			con.onSentPktCond = sync.NewCond(&con.mtx)
		}
		con.WaitSentPktCount++
		con.onSentPktCond.Wait()
		con.WaitSentPktCount--

		if con.sentPktIDAppender.Has(rspc.id) {
			if rspc.id == con.someoneSentPktID {
				con.updateRTTAndSPSCUS(con.someonePktSentTS.Sub(rspc.firstSendTime), 1)
				con.someoneSentPktID = 0
			}
			return nil
		}
	}

	con.resendPkts = append(con.resendPkts, rspc)

	if con.pktTimeoutResenderIsRunning {
		return nil
	}

	go func() {
		for {
			con.mtx.Lock()

			if len(con.resendPkts) == 0 {
				con.pktTimeoutResenderIsRunning = false
				con.mtx.Unlock()
				return
			}

			dur := con.minRTT

			writeTimeout := con.writeTimeout
			if !con.isHandshaked && con.handshakeTimeout < con.writeTimeout {
				writeTimeout = con.handshakeTimeout
			}
			now := time.Now()
			for _, rspc := range con.resendPkts {
				if now.Sub(rspc.firstSendTime) > writeTimeout && con.tryCloseWhenTimeoutUS() {
					con.mtx.Unlock()
					return
				}
				diff := now.Sub(rspc.lastSendTime)
				if diff < con.minRTT {
					if diff < dur {
						dur = diff
					}
					continue
				}
				err := con.resendUS(rspc, 1)
				if err != nil {
					con.mtx.Unlock()
					return
				}
			}

			con.mtx.Unlock()

			time.Sleep(dur + dur/10)
		}
	}()

	con.pktTimeoutResenderIsRunning = true

	return nil
}

func (con *Conn) send(typ byte, others ...interface{}) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.sendUS(typ, others...)
}

type resendPktCtx struct {
	id            uint64
	p             []byte
	firstSendTime time.Time
	lastSendTime  time.Time
	sendCount     int
}

func (con *Conn) resendUS(rspc *resendPktCtx, count int) error {
	rspc.sendCount++
	_, err := con.writeUS(rspc.p, count)
	rspc.lastSendTime = con.lastWriteTime
	return err
}

func (con *Conn) resend(rspc *resendPktCtx, count int) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.resendUS(rspc, count)
}

func (con *Conn) flushUS() error {
	for {
		if con.closeState > 1 {
			if con.resendPktErr != nil {
				return con.resendPktErr
			}
			return errClosed
		}
		if len(con.resendPkts) == 0 {
			return nil
		}
		if con.onSentPktCond == nil {
			con.onSentPktCond = sync.NewCond(&con.mtx)
		}
		con.WaitSentPktCount++
		con.onSentPktCond.Wait()
		con.WaitSentPktCount--
	}
}

func (con *Conn) Flush() error {
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

var errIntervalTooBrief = errors.New("gatling: Interval too brief.")

func (con *Conn) Pace() error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if time.Now().Sub(con.lastWriteTime.Add(con.minRTT)) <= 0 {
		return errIntervalTooBrief
	}
	return con.sendUS(pktHeartbeat)
}

func (con *Conn) updateRTTAndSPSCUS(rtt time.Duration, sentPktSendCount int) {
	con.nowRTT = rtt
	con.nowSentPktSendCount = sentPktSendCount
	if rtt < con.minRTT {
		con.minRTT = rtt
	}
}

func (con *Conn) unresend(pktIDs ...uint64) {
	now := time.Now()

	con.mtx.Lock()

	isSent := false
	for _, pktID := range pktIDs {
		if !con.sentPktIDAppender.TryAdd(pktID, nil) {
			continue
		}
		isSent = true
		isCached := false
		for i, rspc := range con.resendPkts {
			if rspc.id == pktID {
				nowRTT := now.Sub(rspc.firstSendTime)
				con.updateRTTAndSPSCUS(nowRTT, rspc.sendCount)

				if i == len(con.resendPkts)-1 {
					con.resendPkts = con.resendPkts[:i]
					break
				}
				resendPktRights := make([]*resendPktCtx, len(con.resendPkts[i+1:]))
				copy(resendPktRights, con.resendPkts[i+1:])
				con.resendPkts = con.resendPkts[:len(con.resendPkts)-1]
				copy(con.resendPkts[i:], resendPktRights)

				isCached = true
				break
			}
		}
		if !isCached && con.someoneSentPktID == 0 {
			con.someoneSentPktID = pktID
			con.someonePktSentTS = now
		}
	}
	if isSent {
		if len(con.resendPkts) > 0 {
			if con.sentPktIDBaseCache == con.sentPktIDAppender.baseID {
				con.sentPktIDBaseRepeatCount++
				if con.sentPktIDBaseRepeatCount > 2 {
					con.sentPktIDBaseRepeatCount = 0

					err := con.resendUS(con.resendPkts[0], 1)
					if err != nil {
						return
					}

					/*if !con.pktNoDiffResenderIsRunning {
						con.pktNoDiffResenderIsRunning = true
						go func() {
							con.mtx.Lock()
							defer func() {
								con.pktNoDiffResenderIsRunning = false
								con.mtx.Unlock()
							}()

							c := len(con.resendPkts)
							if c == 0 {
								return
							}
							if c > 11 {
								c = 11
							}
							for i := 0; i < c; i++ {
								err := con.resendUS(con.resendPkts[i], 1)
								if err != nil {
									return
								}
							}
						}()
					}*/
				}
			} else {
				con.sentPktIDBaseCache = con.sentPktIDAppender.baseID
				con.sentPktIDBaseRepeatCount = 0
			}
		}
		if con.WaitSentPktCount > 0 {
			con.mtx.Unlock()
			con.onSentPktCond.Broadcast()
			return
		}
	}

	con.mtx.Unlock()
}

var errClosed = errors.New("gatling: Connection is closed.")

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
