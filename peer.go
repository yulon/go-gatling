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
)

type Peer struct {
	lnPorts  []uint16
	lnC      int
	udpLnrs  []*net.UDPConn
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

func (pr *Peer) writeToUDP(p []byte, udpAddr *net.UDPAddr, count int) (sz int, err error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if len(pr.udpLnrs) == 0 {
		err = errClosed
		return
	}

	for i := 0; i < count; i++ {
		pr.lnPtr++
		ix := int(pr.lnPtr % uint(len(pr.udpLnrs)))
		sz, err = pr.udpLnrs[ix].WriteToUDP(p, udpAddr)
		if err != nil {
			return
		}
	}
	return
}

func (pr *Peer) bypassRecvPacket(from *net.UDPAddr, to *net.UDPConn, p []byte) {
	r := bytes.NewBuffer(p)
	var h header
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return
	}
	var con *Conn
	v, ok := pr.conMap.Load(h.ID)
	if !ok {
		con = newConn(h.ID, pr, from.IP.String())
		con.setRmtLastOutputInfo(from, to)

		actual, loaded := pr.conMap.LoadOrStore(h.ID, con)
		if !loaded {
			if h.Type != pktRequestPorts || h.PktID != 1 {
				h.Type = pktInvalid
				h.PktID = 0
				to.WriteToUDP(makePacket(&h), from)
				con.closeUS(errClosed)
				return
			}
			pr.acptCh <- con
			return
		}
		con = actual.(*Conn)
	} else {
		con = v.(*Conn)
		con.setRmtLastOutputInfo(from, to)
	}
	con.handleRecvPacket(&h, r)
	return
}

func listen(ip string, basePort uint16, basePortIsHard bool, portCount int) (*Peer, error) {
	pr := &Peer{
		lnC:    portCount,
		acptCh: make(chan *Conn, 1),
	}
	for i := 0; i < portCount; {
		udpAddr, err := net.ResolveUDPAddr("udp", ip+":"+strconv.FormatUint(uint64(basePort), 10))
		if err != nil {
			return nil, err
		}

		udpLnr, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			if basePortIsHard {
				return nil, err
			}
			basePort++
			continue
		}

		go func() {
			b := make([]byte, 1280)
			for {
				sz, addr, err := udpLnr.ReadFromUDP(b)
				if err != nil {
					fmt.Println("udpLnr.ReadFromUDP(b)", err)
					return
				}
				pr.bypassRecvPacket(addr, udpLnr, b[:sz])
			}
		}()

		pr.udpLnrs = append(pr.udpLnrs, udpLnr)
		pr.lnPorts = append(pr.lnPorts, basePort)

		i++
		basePort++
	}
	pr.use()
	return pr, nil
}

func Listen(addr string) (*Peer, error) {
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
		return listen(ipAndPort[0], uint16(10000), false, portCount)
	}
	port64, err := strconv.ParseUint(ipAndPort[1], 10, 16)
	if err != nil {
		return nil, err
	}
	return listen(ipAndPort[0], uint16(port64), true, portCount)
}

func (pr *Peer) Dial(addr string) (*Conn, error) {
	return dial(pr, addr)
}

func Dial(addr string) (*Conn, error) {
	lnAddr := ":*"
	ipAndPort := strings.Split(addr, ":")
	if len(ipAndPort) != 2 {
		return nil, errIllegalAddr
	}
	if ipAndPort[0] == "127.0.0.1" || ipAndPort[0] == "localhost" {
		lnAddr = "localhost" + lnAddr
	}
	pr, err := Listen(lnAddr)
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
	con.handleRecvPacket(&header{
		con.id,
		1,
		pktRequestPorts,
	}, nil)
	return con, nil
}

func (pr *Peer) Accept() (net.Conn, error) {
	return pr.AcceptGatling()
}

func (pr *Peer) Addr() net.Addr {
	return pr.udpLnrs[0].LocalAddr()
}

func (pr *Peer) Close() error {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if len(pr.udpLnrs) == 0 {
		return errClosed
	}
	close(pr.acptCh)
	for _, udpLnr := range pr.udpLnrs {
		udpLnr.Close()
	}
	pr.udpLnrs = nil
	return nil
}

func (pr *Peer) Range(f func(con *Conn) bool) {
	pr.conMap.Range(func(_, v interface{}) bool {
		return f(v.(*Conn))
	})
}
