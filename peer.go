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
)

type Peer struct {
	lnPorts []uint16
	lnC     int
	udpLnrs []*net.UDPConn
	lnPtr   uintptr
	conMap  sync.Map
	acptCh  chan *Conn
}

func (pr *Peer) lnIx() int {
	return int(atomic.AddUintptr(&pr.lnPtr, 1) % uintptr(len(pr.lnPorts)))
}

func (pr *Peer) bypassRecvPacket(from *net.UDPAddr, to *net.UDPConn, p []byte) {
	r := bytes.NewReader(p)
	var h header
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return
	}
	var con *Conn
	v, ok := pr.conMap.Load(h.ID)
	if !ok {
		con = &Conn{
			id:                     h.ID,
			lcPr:                   pr,
			rmtIP:                  from.IP.String(),
			rmtPortInfoMap:         map[uint16]*portInfo{},
			rtt:                    int64(500 * time.Millisecond),
			reliableMap:            map[uint64]*reliableCache{},
			recvReliableIDAppender: newIDAppender(nil),
		}
		con.setRmtPortInfos(portInfo{uint16(from.Port), to})
		con.recvStrmIDAppender = newIDAppender(func(iads []idAndData) {
			for _, iad := range iads {
				con.putRecvStrm(iad.data.([]byte), nil)
			}
		})

		actual, loaded := pr.conMap.LoadOrStore(h.ID, con)
		if !loaded {
			if h.Type == pktRequestPorts && h.ReliableCount == 1 {
				pr.acptCh <- con
				return
			}
			h.Type = pktClosed
			h.ReliableCount = 0
			to.WriteToUDP(makePacket(&h), from)
			return
		}
		con = actual.(*Conn)
	} else {
		con = v.(*Conn)
		con.addRmtPortInfos(portInfo{uint16(from.Port), to})
	}
	con.handleRecvPacket(&h, p[headerSz:])
	return
}

func listen(ip string, basePort uint16, portCount int) *Peer {
	pr := &Peer{
		lnC:    portCount,
		acptCh: make(chan *Conn, 1),
	}

	for i := 0; i < portCount; {
		udpAddr, err := net.ResolveUDPAddr("udp", ip+":"+strconv.FormatUint(uint64(basePort), 10))
		if err != nil {
			return nil
		}

		udpLnr, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
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

	return pr
}

func Listen(addr string) *Peer {
	ipAndPort := strings.Split(addr, ":")
	if len(ipAndPort) != 2 {
		return nil
	}

	portCount := 1

	if ipAndPort[1][len(ipAndPort[1])-1] == '+' {
		portCount = 512
		ipAndPort[1] = ipAndPort[1][:len(ipAndPort[1])-1]
	}

	var port uint16 = 10000
	if ipAndPort[1] != "*" {
		port64, err := strconv.ParseUint(ipAndPort[1], 10, 16)
		if err != nil {
			return nil
		}
		port = uint16(port64)
	}

	return listen(ipAndPort[0], port, portCount)
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
	pr := Listen(lnAddr)
	return pr.Dial(addr)
}

func (pr *Peer) Accept() (*Conn, error) {
	con := <-pr.acptCh
	con.handleRecvPacket(&header{
		con.id,
		1,
		pktRequestPorts,
	}, nil)
	return con, nil
}
