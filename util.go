package gatling

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/google/uuid"
)

const (
	pktUnreliable byte = iota
	pktReliable
	pktReceivedReliables
	pktRequestReliables
	pktRequestPorts
	pktUpdatePorts
	pktClosed
	pktHeartbeat
	pktStream
)

var isReliableType = []bool{
	false, // pktUnreliable
	true,  // pktReliable
	false, // pktReceivedReliables
	false, // pktRequestReliables
	true,  // pktRequestPorts
	true,  // pktUpdatePorts
	false, // pktClosed
	false, // pktHeartbeat
	true,  // pktStream
}

type header struct {
	ID            uuid.UUID
	ReliableCount uint64
	Type          byte
}

var headerSz = binary.Size(&header{})

func makePacket(h *header, others ...interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, h)
	for _, other := range others {
		switch other.(type) {
		case nil:
		case []byte:
			//binary.Write(buf, binary.LittleEndian, uint16(len(other.([]byte))))
			buf.Write(other.([]byte))
		default:
			binary.Write(buf, binary.LittleEndian, other)
		}
	}
	return buf.Bytes()
}

type idAndData struct {
	id   uint64
	data interface{}
}

type idAppender struct {
	baseID    uint64
	discretes []idAndData
	onAppend  func([]idAndData)
	mtx       sync.Mutex
}

func newIDAppender(onAppend func([]idAndData)) *idAppender {
	return &idAppender{onAppend: onAppend}
}

func (ida *idAppender) TryAdd(id uint64, data interface{}) bool {
	ida.mtx.Lock()
	defer ida.mtx.Unlock()

	if id <= ida.baseID {
		return false
	}

	isInst := false
	for i, iad := range ida.discretes {
		if id == iad.id {
			return false
		}
		if id < iad.id {
			a := ida.discretes[:i]
			b := []idAndData{idAndData{id, data}}
			c := ida.discretes[i:]
			ida.discretes = make([]idAndData, len(a)+len(b)+len(c))
			copy(ida.discretes, a)
			copy(ida.discretes[len(a):], b)
			copy(ida.discretes[len(a)+len(b):], c)
			isInst = true
			break
		}
	}
	if !isInst {
		if len(ida.discretes) == 0 {
			ida.discretes = []idAndData{idAndData{id, data}}
		} else if id > ida.discretes[len(ida.discretes)-1].id {
			ida.discretes = append(ida.discretes, idAndData{id, data})
		}
	}

	sz := len(ida.discretes)
	if sz == 0 || ida.discretes[0].id-ida.baseID > 1 {
		return true
	}
	i := 1
	for ; i < sz && ida.discretes[i].id-ida.discretes[i-1].id == 1; i++ {
	}
	if ida.onAppend != nil {
		ida.onAppend(ida.discretes[:i])
	}
	ida.baseID = ida.discretes[i-1].id
	ida.discretes = ida.discretes[i:]
	return true
}
