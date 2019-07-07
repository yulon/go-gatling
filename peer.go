package gatling

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Peer struct {
	pktLnrs     []net.PacketConn
	lnPorts     []uint16
	lnPortsHash uint64
	lnIx        uint
	mtx         sync.Mutex
	conMap      sync.Map
	acptCh      chan *Conn
	useCount    int64
}

func (pr *Peer) use() {
	atomic.AddInt64(&pr.useCount, int64(1))
}

func (pr *Peer) unuse() {
	if atomic.AddInt64(&pr.useCount, -int64(1)) == 0 {
		pr.Close()
	}
}

func (pr *Peer) writeTo(p []byte, addr net.Addr, count int) (sz int, err error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if len(pr.pktLnrs) == 0 {
		err = errClosed
		return
	}

	for i := 0; i < count; i++ {
		pr.lnIx++
		ix := int(pr.lnIx % uint(len(pr.pktLnrs)))
		sz, err = pr.pktLnrs[ix].WriteTo(p, addr)
		if err != nil {
			return
		}
	}
	return
}

func (pr *Peer) bypassRecvPacket(from net.Addr, to net.PacketConn, h *header, p []byte) {
	r := bytes.NewBuffer(p)
	err := binary.Read(r, binary.LittleEndian, h)

	if err != nil || int(h.Type) >= len(isReliableType) {
		return
	}

	if h.DestPortsHash == 0 {
		if to.LocalAddr().(*net.UDPAddr).Port != int(pr.lnPorts[0]) {
			return
		}
	} else if h.DestPortsHash != pr.lnPortsHash {
		return
	}

	var con *Conn
	v, ok := pr.conMap.Load(h.ID)
	if ok {
		con = v.(*Conn)
		con.handleRecvInfo(from, to, h)
	} else {
		con = newConn(pr, h.ID)
		con.rmtPortsHash = h.SrcPortsHash
		con.isHandshaked = true
		con.handleRecvInfo(from, to, h)

		actual, loaded := pr.conMap.LoadOrStore(h.ID, con)
		if loaded {
			con = actual.(*Conn)
			con.handleRecvInfo(from, to, h)
		} else {
			if h.DestPortsHash == 0 && len(pr.lnPorts) > 1 {
				err := con.send(pktUpdatePorts, pr.lnPorts)
				if err != nil {
					return
				}
			}
			pr.mtx.Lock()
			if len(pr.pktLnrs) == 0 {
				pr.mtx.Unlock()
				return
			}
			pr.acptCh <- con
			pr.mtx.Unlock()
		}
	}
	con.handleRecvPacket(from, to, h, r)
	return
}

type PacketConnConverter func(net.PacketConn) net.PacketConn

func ListenUDP(udpAddr *net.UDPAddr, portCount int, pcc PacketConnConverter) (*Peer, error) {
	pr := &Peer{
		acptCh:      make(chan *Conn, 1),
		lnPortsHash: 1,
	}

	for i := 0; i < portCount; i++ {
		if i == 1 {
			udpAddr.Port = 0
		}

		udpLnr, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			for _, pktLnr := range pr.pktLnrs {
				pktLnr.Close()
			}
			return nil, err
		}

		var pktLnr net.PacketConn
		if pcc != nil {
			pktLnr = pcc(udpLnr)
		} else {
			pktLnr = udpLnr
		}

		go func() {
			var h header
			b := make([]byte, 1280)
			for {
				sz, addr, err := pktLnr.ReadFrom(b)
				if err != nil {
					return
				}
				pr.bypassRecvPacket(addr, pktLnr, &h, b[:sz])
			}
		}()

		pr.pktLnrs = append(pr.pktLnrs, pktLnr)
		port := pktLnr.LocalAddr().(*net.UDPAddr).Port
		pr.lnPortsHash = pr.lnPortsHash*hSeed + uint64(port)
		pr.lnPorts = append(pr.lnPorts, uint16(port))
	}

	go func() {
		for {
			dur := 90 * time.Second

			now := time.Now()
			pr.conMap.Range(func(_, v interface{}) bool {
				con := v.(*Conn)
				con.mtx.Lock()
				defer con.mtx.Unlock()

				endTime := con.lastReadTime.Add(con.readTimeout)
				if now.Before(endTime) {
					diff := endTime.Sub(now)
					if diff < dur {
						dur = diff
					}
					return true
				}
				con.closeUS(errTimeout)
				return true
			})

			time.Sleep(dur)
		}
	}()

	pr.use()
	return pr, nil
}

func Listen(addr string, portCount int, pcc PacketConnConverter) (*Peer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return ListenUDP(udpAddr, portCount, pcc)
}

func (pr *Peer) DialUDP(udpAddr *net.UDPAddr) (*Conn, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	con := newConn(pr, id)

	ports, loaded := portsMap.Load(udpAddr.String())
	if loaded {
		con.setRmtAddrUS(udpAddr, ports.([]uint16)...)
	} else {
		con.setRmtAddrUS(udpAddr)
	}

	pr.conMap.Store(id, con)
	return con, nil
}

func (pr *Peer) Dial(addr string) (*Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return pr.DialUDP(udpAddr)
}

var ipv4Localhost = net.ParseIP("127.0.0.1")

func DialUDP(remoteUDPAddr *net.UDPAddr, localPortCount int, pcc PacketConnConverter) (*Conn, error) {
	localUDPAddr := &net.UDPAddr{}
	if remoteUDPAddr.IP.Equal(ipv4Localhost) {
		localUDPAddr.IP = ipv4Localhost
	} else {
		localUDPAddr.IP = net.IPv4zero
	}
	pr, err := ListenUDP(localUDPAddr, localPortCount, pcc)
	if err != nil {
		return nil, err
	}
	con, err := pr.DialUDP(remoteUDPAddr)
	if err != nil {
		return nil, err
	}
	pr.unuse()
	return con, err
}

func Dial(remoteAddr string, localPortCount int, pcc PacketConnConverter) (*Conn, error) {
	remoteUDPAddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, err
	}
	return DialUDP(remoteUDPAddr, localPortCount, pcc)
}

func (pr *Peer) AcceptGatling() (*Conn, error) {
	con, ok := <-pr.acptCh
	if !ok || con.IsClose() {
		return nil, errClosed
	}
	return con, nil
}

func (pr *Peer) Accept() (net.Conn, error) {
	return pr.AcceptGatling()
}

func (pr *Peer) Addr() net.Addr {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if len(pr.pktLnrs) == 0 {
		return nil
	}
	return pr.pktLnrs[0].LocalAddr()
}

func (pr *Peer) Close() error {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if len(pr.pktLnrs) == 0 {
		return errClosed
	}
	close(pr.acptCh)
	for _, udpLnr := range pr.pktLnrs {
		udpLnr.Close()
	}
	pr.pktLnrs = nil
	return nil
}

func (pr *Peer) Range(f func(con *Conn) bool) {
	pr.conMap.Range(func(_, v interface{}) bool {
		return f(v.(*Conn))
	})
}
