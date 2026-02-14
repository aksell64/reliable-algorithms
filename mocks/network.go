package mocks

import (
	"reliable/types"
	"sync"
	"sync/atomic"
)

// sendHook is a wrapper type for the message interception callback.
// Stored via atomic.Pointer to enable lock-free access in the hot path.
type sendHook func(from, to types.ProcessID, msg types.Message) bool

// InstantNetwork is a test-only Network implementation that delivers messages
// synchronously and instantly — no buffers, no background goroutines.
//
// It implements the network.Network interface and can replace the global
// network via network.SetNetwork() for deterministic, fast tests.
//
// ╔══════════════════════════════════════════════════════════════════════╗
// ║  WARNING: SYNCHRONOUS DELIVERY — NO REENTRANT Send() ALLOWED         ║
// ║                                                                      ║
// ║  Send() calls deliverer.Deliver(msg) directly in the caller's        ║
// ║  goroutine. If your Deliver handler calls Send() on the SAME         ║
// ║  InstantNetwork instance, you WILL get a deadlock or undefined       ║
// ║  behavior.                                                           ║
// ║                                                                      ║
// ║  This is fine for most layered stacks (BaseLink → PerfectLink →      ║
// ║  BEB → EpochConsensus) because BaseLink.Send() handles self-sends    ║
// ║  locally and only calls network.Send() for remote peers.             ║
// ║  However, if any Deliver path triggers a NEW network.Send() call     ║
// ║  (e.g. a relay/rebroadcast inside Deliver), the synchronous call     ║
// ║  chain will deadlock on connMu.                                      ║
// ║                                                                      ║
// ║  If you need reentrant delivery, use AsyncInstantNetwork (TODO)      ║
// ║  or the real buffered network.New() with goroutine readers.          ║
// ╚══════════════════════════════════════════════════════════════════════╝
type InstantNetwork struct {
	// deliverers stores connected nodes. Protected by connMu.
	// Written only during setup (Connect), read during Send.
	deliverers map[types.ProcessID]types.Deliverer
	connMu     sync.RWMutex

	// Atomic counters — lock-free, safe for concurrent Send() calls.
	sendCount atomic.Int64
	dropCount atomic.Int64

	// Optional hook to intercept/drop messages. Lock-free via atomic.Pointer.
	// If set, called before every delivery. Return false to drop the message.
	onSend atomic.Pointer[sendHook]
}

// NewInstantNetwork creates a new instant-delivery test network.
func NewInstantNetwork() *InstantNetwork {
	return &InstantNetwork{
		deliverers: make(map[types.ProcessID]types.Deliverer),
	}
}

// Connect registers a deliverer (node) in the network.
// Must be called before Send — typically during test setup.
func (n *InstantNetwork) Connect(deliverer types.Deliverer) {
	n.connMu.Lock()
	defer n.connMu.Unlock()
	n.deliverers[deliverer.ProcessID()] = deliverer
}

// Send delivers a message to the target node synchronously.
// See the WARNING above about reentrant calls.
func (n *InstantNetwork) Send(from, to types.ProcessID, msg types.Message) {
	n.connMu.RLock()
	deliverer, ok := n.deliverers[to]
	n.connMu.RUnlock()

	if !ok {
		// Receiver not connected — message is lost (mimics real network behavior).
		return
	}

	// Check the hook (lock-free read).
	if hookPtr := n.onSend.Load(); hookPtr != nil {
		hook := *hookPtr
		if !hook(from, to, msg) {
			n.dropCount.Add(1)
			return
		}
	}

	n.sendCount.Add(1)

	// Synchronous delivery — see WARNING in the type comment.
	deliverer.Deliver(msg)
}

// --- Test helpers ---

// SetOnSend sets an interception hook. Return false from the hook to drop a message.
// Pass nil to remove the hook.
func (n *InstantNetwork) SetOnSend(hook func(from, to types.ProcessID, msg types.Message) bool) {
	if hook == nil {
		n.onSend.Store(nil)
		return
	}
	h := sendHook(hook)
	n.onSend.Store(&h)
}

// Stats returns the number of successfully sent and dropped messages.
func (n *InstantNetwork) Stats() (sent int64, dropped int64) {
	return n.sendCount.Load(), n.dropCount.Load()
}

// Reset clears all counters and removes the send hook.
func (n *InstantNetwork) Reset() {
	n.sendCount.Store(0)
	n.dropCount.Store(0)
	n.onSend.Store(nil)
}
