package gatling

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Peer struct {
	lnPorts  []uint16
	pktLnrs  []net.PacketConn
	lnPtr    uint
	mtx      sync.Mutex
	conMap   sync.Map
	acptCh   chan *Conn
	useCount int64
	isClosed bool
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
		pr.lnPtr++
		ix := int(pr.lnPtr % uint(len(pr.pktLnrs)))
		sz, err = pr.pktLnrs[ix].WriteTo(p, addr)
		if err != nil {
			return
		}
	}
	return
}

func (pr *Peer) bypassRecvPacket(from net.Addr, to net.PacketConn, p []byte) {
	r := bytes.NewBuffer(p)
	var h header
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return
	}
	var con *Conn
	v, ok := pr.conMap.Load(h.ID)
	if !ok {
		con = newConn(h.ID, pr)
		con.handleRecvInfo(from, to)

		actual, loaded := pr.conMap.LoadOrStore(h.ID, con)
		if !loaded {
			if h.Type != pktRequestPorts || h.PktID != 1 {
				h.Type = pktInvalid
				h.PktID = 0
				to.WriteTo(makePacket(&h), from)
				con.closeUS(errClosed)
				return
			}
			pr.acptCh <- con
			return
		}
		con = actual.(*Conn)
	} else {
		con = v.(*Conn)
		con.handleRecvInfo(from, to)
	}
	con.handleRecvPacket(from, &h, r)
	return
}

type PacketConnConverter func(net.PacketConn) net.PacketConn

func listen(ip string, mainPort uint16, portCount int, pcc PacketConnConverter) (*Peer, error) {
	pr := &Peer{
		acptCh: make(chan *Conn, 1),
	}

	uniUDPAddr, err := net.ResolveUDPAddr("udp", ip+":0")
	if err != nil {
		return nil, err
	}

	for i := 0; i < portCount; {
		var udpAddr *net.UDPAddr
		if i == 0 {
			udpAddr = &net.UDPAddr{
				IP:   uniUDPAddr.IP,
				Port: int(mainPort),
				Zone: uniUDPAddr.Zone,
			}
		} else {
			udpAddr = uniUDPAddr
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
			b := make([]byte, 1280)
			for {
				sz, addr, err := pktLnr.ReadFrom(b)
				if err != nil {
					fmt.Println("pktLnr.ReadFrom(b)", err)
					return
				}
				pr.bypassRecvPacket(addr, pktLnr, b[:sz])
			}
		}()

		pr.pktLnrs = append(pr.pktLnrs, pktLnr)
		pr.lnPorts = append(pr.lnPorts, uint16(pktLnr.LocalAddr().(*net.UDPAddr).Port))

		i++
	}
	pr.use()
	return pr, nil
}

func Listen(addr string, pcc PacketConnConverter) (*Peer, error) {
	ipAndPort := strings.Split(addr, ":")
	if len(ipAndPort) != 2 {
		return nil, errIllegalAddr
	}

	portCount := 1

	if ipAndPort[1][len(ipAndPort[1])-1] == '+' {
		portCount = 512
		ipAndPort[1] = ipAndPort[1][:len(ipAndPort[1])-1]
	}

	if ipAndPort[1] == "*" {
		return listen(ipAndPort[0], 0, portCount, pcc)
	}
	port64, err := strconv.ParseUint(ipAndPort[1], 10, 16)
	if err != nil {
		return nil, err
	}
	return listen(ipAndPort[0], uint16(port64), portCount, pcc)
}

func (pr *Peer) Dial(addr string) (*Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	con := newConn(id, pr)
	con.setRmtAddr(udpAddr)
	con.SetSendTimeout(5 * time.Second)

	pr.conMap.Store(id, con)

	err = con.send(pktRequestPorts)
	if err != nil {
		return nil, err
	}

	err = con.flush()
	if err != nil {
		return nil, err
	}
	con.SetSendTimeout(30 * time.Second)
	return con, nil
}

func Dial(addr string, pcc PacketConnConverter) (*Conn, error) {
	lnAddr := ":*"
	ipAndPort := strings.Split(addr, ":")
	if len(ipAndPort) != 2 {
		return nil, errIllegalAddr
	}
	if ipAndPort[0] == "127.0.0.1" || ipAndPort[0] == "localhost" {
		lnAddr = "localhost" + lnAddr
	}
	pr, err := Listen(lnAddr, pcc)
	if err != nil {
		return nil, err
	}
	con, err := pr.Dial(addr)
	if err != nil {
		return nil, err
	}
	pr.unuse()
	return con, err
}

func (pr *Peer) AcceptGatling() (*Conn, error) {
	con, ok := <-pr.acptCh
	if !ok || con.IsClose() {
		return nil, errClosed
	}
	con.handleRecvPacket(
		nil,
		&header{
			con.id,
			1,
			pktRequestPorts,
		},
		nil,
	)
	return con, nil
}

func (pr *Peer) Accept() (net.Conn, error) {
	return pr.AcceptGatling()
}

func (pr *Peer) Addr() net.Addr {
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
