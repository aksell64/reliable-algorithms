package broadcaster

import (
	"context"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils/codec"
	"sync"

	"github.com/google/uuid"
)

// BCBFactory строит инстанс Byzantine Consistent Broadcast с заданным
// отправителем (originator) поверх переданного транспорта.
type BCBFactory func(sender types.ProcessID, link p2p.Link) Broadcaster

// NewAuthenticatedEchoBCBFactory возвращает BCBFactory, создающую инстансы
// authenticatedEchoBroadcaster (Byzantine Consistent Broadcast, Algorithm 3.10).
func NewAuthenticatedEchoBCBFactory(
	self types.ProcessID,
	processes []types.ProcessID,
	faults int,
	registry *codec.Registry,
) BCBFactory {
	return func(sender types.ProcessID, link p2p.Link) Broadcaster {
		return NewAuthenticatedEchoBroadcaster(self, sender, link, processes, faults, registry)
	}
}

// NewAuthenticatedDoubleEchoBCBFactory возвращает BCBFactory, создающую
// инстансы authenticatedDoubleEchoBroadcaster (Byzantine Reliable Broadcast,
// Algorithm 3.17). Используется, когда каналу нужны более сильные гарантии,
// чем BCB (totality поверх consistency).
func NewAuthenticatedDoubleEchoBCBFactory(
	self types.ProcessID,
	processes []types.ProcessID,
	faults int,
	registry *codec.Registry,
) BCBFactory {
	return func(sender types.ProcessID, link p2p.Link) Broadcaster {
		return NewAuthenticatedDoubleEchoBroadcaster(self, sender, processes, faults, link, registry)
	}
}

// byzantineChannel реализует Algorithm 3.19 (Byzantine Consistent Channel):
// для каждого процесса p поддерживается счётчик n[p] и текущий инстанс
// bcb.p.n[p]. Сообщения, отправляемые BCB-инстансом, теггируются парой
// (Sender, Number), что обеспечивает изоляцию инстансов на одном линке.
type byzantineChannel struct {
	types.Deliverer
	ctx       context.Context
	self      types.ProcessID
	processes []types.ProcessID
	al        p2p.Link
	registry  *codec.Registry
	factory   BCBFactory

	mu       sync.Mutex
	n        map[types.ProcessID]int
	bcasters map[types.ProcessID]Broadcaster
	links    map[types.ProcessID]*bcbLink
	queue    []types.Message
	ready    bool

	once *types.WorkerOnce
}

func NewByzantineConsistentChannel(
	ctx context.Context,
	self types.ProcessID,
	processes []types.ProcessID,
	al p2p.Link,
	registry *codec.Registry,
	factory BCBFactory,
) Broadcaster {
	return &byzantineChannel{
		ctx:       ctx,
		self:      self,
		processes: processes,
		al:        al,
		registry:  registry,
		factory:   factory,
		n:         make(map[types.ProcessID]int),
		bcasters:  make(map[types.ProcessID]Broadcaster),
		links:     make(map[types.ProcessID]*bcbLink),
		ready:     true,
		Deliverer: types.NewUnaryDeliverer(self),
		once:      types.NewWorkerOnce(),
	}
}

func (b *byzantineChannel) Init() {
	b.once.Init(func() {
		codec.Register[ByzChannelMessage](b.registry, ByzChannelMessageName)
		codec.Register[ByzChannelDomainMessage](b.registry, ByzChannelDomainMessageName)

		b.al.Init()
		b.al.AddDeliverer(b, types.DelivererWithMsgNames(ByzChannelMessageName))

		b.mu.Lock()
		defer b.mu.Unlock()
		for _, p := range b.processes {
			b.n[p] = 0
			b.installBCBLocked(p, 0)
		}
	})
}

func (b *byzantineChannel) Start() {
	b.once.Start(func() {
		b.al.Start()
	})
}

func (b *byzantineChannel) Stop() {
	b.once.Stop(func() {
		b.mu.Lock()
		bcs := make([]Broadcaster, 0, len(b.bcasters))
		for _, bc := range b.bcasters {
			bcs = append(bcs, bc)
		}
		b.mu.Unlock()
		for _, bc := range bcs {
			bc.Stop()
		}
		b.al.Stop()
	})
}

func (b *byzantineChannel) AddCorrect(pid types.ProcessID) {
	b.mu.Lock()
	addToProcessesSlice(&b.processes, pid)
	_, exists := b.n[pid]
	if !exists {
		b.n[pid] = 0
		b.installBCBLocked(pid, 0)
	}
	b.mu.Unlock()
}

func (b *byzantineChannel) RemoveCorrect(pid types.ProcessID) {
	b.mu.Lock()
	removeFromProcessesSlice(&b.processes, pid)
	b.mu.Unlock()
}

// Broadcast соответствует событию ⟨ bcch, Broadcast | m ⟩. Если ready=FALSE,
// сообщение помещается в очередь и будет отправлено, как только инстанс
// bcb.self.n[self] завершит текущую трансляцию.
func (b *byzantineChannel) Broadcast(ctx context.Context, msg types.Message) {
	b.mu.Lock()
	b.queue = append(b.queue, msg)
	bc, payload, ok := b.dequeueLocked()
	b.mu.Unlock()
	if ok {
		bc.Broadcast(ctx, payload)
	}
}

func (b *byzantineChannel) dequeueLocked() (Broadcaster, types.Message, bool) {
	if !b.ready || len(b.queue) == 0 {
		return nil, nil, false
	}
	msg := b.queue[0]
	b.queue = b.queue[1:]
	b.ready = false
	return b.bcasters[b.self], msg, true
}

// installBCBLocked создаёт новый инстанс bcb.sender.number и связывает его
// с каналом через per-instance link adapter и per-instance deliverer adapter.
// Должен вызываться с захваченным b.mu.
func (b *byzantineChannel) installBCBLocked(sender types.ProcessID, number int) {
	link := newBCBLink(b, sender, number)
	bc := b.factory(sender, link)

	adapter := &bcbDelivererAdapter{
		UnimplementedDeliverer: types.NewUnimplementedDeliverer(b.self),
		parent:                 b,
		sender:                 sender,
		number:                 number,
	}
	bc.AddDeliverer(adapter)

	b.links[sender] = link
	b.bcasters[sender] = bc

	bc.Init()
	bc.Start()
}

// Deliver принимает обёрнутые ByzChannelMessage из нижележащего линка и
// маршрутизирует внутренние сообщения BCB-протокола в соответствующий
// инстанс bcb.Sender.Number. Сообщения для несуществующего/устаревшего
// инстанса отбрасываются.
func (b *byzantineChannel) Deliver(message types.Message) {
	wrapped, ok := message.(ByzChannelMessage)
	if !ok {
		return
	}

	b.mu.Lock()
	expected, hasN := b.n[wrapped.Sender]
	link, hasLink := b.links[wrapped.Sender]
	b.mu.Unlock()

	if !hasN || !hasLink || wrapped.Number != expected {
		return
	}

	inner, err := messages.UnmarshalRawWithRegistry(wrapped.Inner, b.registry)
	if err != nil {
		return
	}
	link.deliverIn(inner)
}

// onBCBDelivered обрабатывает событие ⟨ bcb.p.n[p], Deliver | p, m ⟩:
// выдаёт сообщение наружу, инкрементирует n[p] и создаёт следующий
// инстанс bcb.p.n[p]. Для p = self снимается флаг ready и при наличии
// очереди запускается следующая трансляция.
func (b *byzantineChannel) onBCBDelivered(sender types.ProcessID, number int, msg types.Message) {
	b.mu.Lock()
	if b.n[sender] != number {
		b.mu.Unlock()
		return
	}
	b.n[sender] = number + 1
	delete(b.bcasters, sender)
	delete(b.links, sender)
	b.installBCBLocked(sender, number+1)

	var pendingBC Broadcaster
	var pendingMsg types.Message
	if sender == b.self {
		b.ready = true
		pendingBC, pendingMsg, _ = b.dequeueLocked()
	}
	b.mu.Unlock()

	raw, err := b.registry.Marshal(msg)
	if err == nil {
		domain := ByzChannelDomainMessage{
			BaseMsg: messages.NewBase(uuid.New(), sender, ByzChannelDomainMessageName),
			Inner:   messages.NewRaw(raw, msg.Type()),
			N:       number,
		}
		b.Deliverer.Deliver(domain)
	}

	if pendingBC != nil {
		pendingBC.Broadcast(b.ctx, pendingMsg)
	}
}

// bcbLink — адаптер линка, выдаваемый каждому BCB-инстансу. На исходящих
// сообщениях упаковывает payload в ByzChannelMessage с (Sender, Number)
// инстанса; на входящих — пробрасывает уже распакованное сообщение
// зарегистрированным деливерерам (т.е. самому BCB).
type bcbLink struct {
	types.Deliverer
	parent *byzantineChannel
	sender types.ProcessID
	number int
}

func newBCBLink(parent *byzantineChannel, sender types.ProcessID, number int) *bcbLink {
	return &bcbLink{
		Deliverer: types.NewUnaryDeliverer(parent.self),
		parent:    parent,
		sender:    sender,
		number:    number,
	}
}

func (l *bcbLink) Init()  {}
func (l *bcbLink) Start() {}
func (l *bcbLink) Stop()  {}

func (l *bcbLink) Send(to types.ProcessID, msg types.Message) {
	raw, err := l.parent.registry.Marshal(msg)
	if err != nil {
		return
	}
	wrapped := ByzChannelMessage{
		BaseMsg: messages.NewBase(uuid.New(), l.parent.self, ByzChannelMessageName),
		Inner:   messages.NewRaw(raw, msg.Type()),
		Sender:  l.sender,
		Number:  l.number,
	}
	l.parent.al.Send(to, wrapped)
}

func (l *bcbLink) deliverIn(msg types.Message) {
	l.Deliverer.Deliver(msg)
}

// bcbDelivererAdapter регистрируется как деливерер на BCB-инстансе; его
// единственная задача — донести до канала, какой именно (sender, number)
// инстанс выдал сообщение.
type bcbDelivererAdapter struct {
	types.UnimplementedDeliverer
	parent *byzantineChannel
	sender types.ProcessID
	number int
}

func (a *bcbDelivererAdapter) Deliver(msg types.Message) {
	a.parent.onBCBDelivered(a.sender, a.number, msg)
}
