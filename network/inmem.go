package network

import (
	"reliable/logger"
	"reliable/types"
	"sync"
)

type inMemNetwork struct {
	conns      map[types.ProcessID]map[types.ProcessID]*conn
	deliverers map[types.ProcessID]types.Deliverer
	mu         sync.RWMutex
}

type conn struct {
	from types.ProcessID
	to   types.ProcessID
	buf  chan types.Message
}

func NewInMemory() Network {
	return &inMemNetwork{
		conns:      make(map[types.ProcessID]map[types.ProcessID]*conn),
		deliverers: make(map[types.ProcessID]types.Deliverer),
	}
}

func (net *inMemNetwork) Send(from, to types.ProcessID, msg types.Message) {
	c := net.getOrCreateConn(from, to)
	select {
	case c.buf <- msg:
	default:
		c.buf <- msg
	}
}

func (net *inMemNetwork) getOrCreateConn(from, to types.ProcessID) *conn {
	net.mu.RLock()
	if fromConns, ok := net.conns[from]; ok {
		if c, ok := fromConns[to]; ok {
			net.mu.RUnlock()
			return c
		}
	}
	net.mu.RUnlock()

	net.mu.Lock()
	defer net.mu.Unlock()

	if net.conns[from] == nil {
		net.conns[from] = make(map[types.ProcessID]*conn)
	}
	if c, ok := net.conns[from][to]; ok {
		return c
	}

	c := &conn{
		from: from,
		to:   to,
		buf:  make(chan types.Message, bufSize),
	}
	net.conns[from][to] = c

	deliverer, ok := net.deliverers[to]
	if !ok {
		logger.Panicfmt("not connected")
		return c
	}

	for range bufReadersCount {
		go net.readBuf(c.buf, deliverer)
	}

	return c
}

func (net *inMemNetwork) readBuf(buf chan types.Message, deliverer types.Deliverer) {
	for msg := range buf {
		deliverer.Deliver(msg)
	}
}

func (net *inMemNetwork) Connect(deliverer types.Deliverer) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.deliverers[deliverer.ProcessID()] = deliverer
}
