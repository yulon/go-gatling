package gatling

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	pktUnreliableData byte = iota
	pktData
	pktReceiveds
	pktRequests
	pktRequestPorts
	pktResponsePorts
	pktUpdatePorts
	pktClosed
	pktInvalid
	pktHeartbeat
	pktStreamData
)

var isReliableType = []bool{
	false, // pktUnreliableData
	true,  // pktData
	false, // pktReceiveds
	false, // pktRequests
	true,  // pktRequestPorts
	false, // pktResponsePorts
	true,  // pktUpdatePorts
	true,  // pktClosed
	false, // pktInvalid
	false, // pktHeartbeat
	true,  // pktStreamData
}

type header struct {
	ID    uuid.UUID
	PktID uint64
	Type  byte
}

var headerSz = binary.Size(&header{})

const sendPktCachesMax = 1024

const DefaultRTT = 266 * time.Millisecond

func makeData(others ...interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	for _, other := range others {
		switch other.(type) {
		case nil:
		case []byte:
			buf.Write(other.([]byte))
		default:
			binary.Write(buf, binary.LittleEndian, other)
		}
	}
	return buf.Bytes()
}

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

func newIDAndData(id uint64, data interface{}) idAndData {
	switch data.(type) {
	case []byte:
		dataSrc := data.([]byte)
		dataCpy := make([]byte, len(dataSrc))
		copy(dataCpy, dataSrc)
		return idAndData{id, dataCpy}
	}
	return idAndData{id, data}
}

type idAppender struct {
	baseID    uint64
	discretes []idAndData
	onAppend  func([]idAndData)
	mtx       *sync.Mutex
}

func newIDAppender(mtx *sync.Mutex, onAppend func([]idAndData)) *idAppender {
	return &idAppender{onAppend: onAppend, mtx: mtx}
}

func (ida *idAppender) TryAdd(id uint64, data interface{}) bool {
	if ida.mtx != nil {
		ida.mtx.Lock()
		defer ida.mtx.Unlock()
	}

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
			b := []idAndData{newIDAndData(id, data)}
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
			ida.discretes = []idAndData{newIDAndData(id, data)}
		} else if id > ida.discretes[len(ida.discretes)-1].id {
			ida.discretes = append(ida.discretes, newIDAndData(id, data))
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

func (ida *idAppender) Has(id uint64) bool {
	if ida.mtx != nil {
		ida.mtx.Lock()
		defer ida.mtx.Unlock()
	}

	if id <= ida.baseID {
		return true
	}
	for _, iad := range ida.discretes {
		if id == iad.id {
			return true
		}
	}
	return false
}
