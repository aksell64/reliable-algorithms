package p2p

import (
	"context"
	"reliable/types"
	"sync"
	"time"
)

type sent struct {
	to  types.ProcessID
	msg types.Message
}

type stubbornP2PLinks struct {
	types.Deliverer
	ctx            context.Context
	stop           func()
	self           types.ProcessID
	sent           []sent
	mu             sync.Mutex
	resendInterval time.Duration
	base           Link
	once           *types.WorkerOnce
}

func NewStubbornP2PLinks(ctx context.Context, base Link) Link {
	id := base.ProcessID()
	cctx, cancel := context.WithCancel(ctx)
	l := &stubbornP2PLinks{
		ctx:            cctx,
		stop:           cancel,
		self:           id,
		sent:           make([]sent, 0),
		resendInterval: 5 * time.Second,
		Deliverer:      types.NewUnaryDeliverer(id),
		once:           types.NewWorkerOnce(),
	}

	base.AddDeliverer(l)
	l.base = base

	return l
}

func (l *stubbornP2PLinks) Init() {
	l.once.Init(func() {
		l.base.Init()
	})
}

func (l *stubbornP2PLinks) Start() {
	l.once.Start(func() {
		l.base.Start()
		go l.background()
	})
}

func (l *stubbornP2PLinks) Stop() {
	l.once.Stop(func() {
		l.stop()
	})
}

func (l *stubbornP2PLinks) Send(to types.ProcessID, msg types.Message) {
	l.base.Send(to, msg)

	l.mu.Lock()
	defer l.mu.Unlock()
	l.sent = append(l.sent, sent{to: to, msg: msg})
}

func (l *stubbornP2PLinks) Deliver(msg types.Message) {
	l.Deliverer.Deliver(msg)
}

func (l *stubbornP2PLinks) background() {
	ticker := time.NewTicker(l.resendInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			msgs := l.nextSent()
			for _, msg := range msgs {
				l.base.Send(msg.to, msg.msg)
			}
		}
	}
}

func (l *stubbornP2PLinks) nextSent() []sent {
	l.mu.Lock()
	defer l.mu.Unlock()

	// in theory, it should be like this:
	// return l.sent[:]
	// but this is too much

	sentLen := len(l.sent) / 2
	msgs := l.sent[:sentLen]
	l.sent = l.sent[0:]
	return msgs
}

func (l *stubbornP2PLinks) Instance() string {
	return "sl"
}
