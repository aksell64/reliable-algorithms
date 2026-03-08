package network

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"reliable/logger"
	"reliable/types"
	"sync"
)

const (
	bufReadersCount = 32
	bufSize         = 4048
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

func (net *inMemNetwork) Identify() types.Identify {
	return inMemAlwaysTrueIdentify{}
}

func (net *inMemNetwork) Boostrap() error { return nil }

func (net *inMemNetwork) Send(from, to types.ProcessID, msg types.Message) {
	c := net.getOrCreateConn(from, to)
	select {
	case c.buf <- msg:
	default:
		// cringe optimization
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

type keyRegistry struct {
	mu   sync.RWMutex
	keys map[types.ProcessID]ed25519.PrivateKey
}

func newKeyRegistry() *keyRegistry {
	return &keyRegistry{
		keys: make(map[types.ProcessID]ed25519.PrivateKey),
	}
}

func (r *keyRegistry) getOrCreate(pid types.ProcessID) ed25519.PrivateKey {
	r.mu.RLock()
	if key, ok := r.keys[pid]; ok {
		r.mu.RUnlock()
		return key
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// double-check после захвата write lock
	if key, ok := r.keys[pid]; ok {
		return key
	}

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("generate key for %s: %v", pid.String(), err))
	}
	r.keys[pid] = priv
	return priv
}

func (r *keyRegistry) publicKey(pid types.ProcessID) (ed25519.PublicKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key, ok := r.keys[pid]
	if !ok {
		return nil, fmt.Errorf("no key registered for process %s", pid.String())
	}
	return key.Public().(ed25519.PublicKey), nil
}

type inMemIdentify struct {
	self     types.ProcessID
	privKey  ed25519.PrivateKey
	registry *keyRegistry
}

func NewInMemIdentify(self types.ProcessID, priv ed25519.PrivateKey) types.Identify {
	return &inMemIdentify{
		self:     self,
		privKey:  priv,
		registry: newKeyRegistry(),
	}
}

func (id *inMemIdentify) Sign(raw []byte) ([]byte, error) {
	return ed25519.Sign(id.privKey, raw), nil
}

func (id *inMemIdentify) Verify(from types.ProcessID, raw []byte, signature []byte) error {
	pubKey, err := id.registry.publicKey(from)
	if err != nil {
		return err
	}

	if !ed25519.Verify(pubKey, raw, signature) {
		return fmt.Errorf("invalid signature from process %s", from.String())
	}
	return nil
}

type inMemAlwaysTrueIdentify struct{}

func (id inMemAlwaysTrueIdentify) Sign(raw []byte) ([]byte, error) {
	return raw, nil
}

func (id inMemAlwaysTrueIdentify) Verify(from types.ProcessID, raw []byte, signature []byte) error {
	return nil
}
