package dkg

import (
	"context"
	"fmt"
	"math/big"
	"reliable/broadcaster"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/fsm"
	"reliable/utils/crypto"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	phase1Duration = 10 * time.Second
	phase4Duration = 10 * time.Second
)

type Keys struct {
	PkShare  *big.Int
	PubShare *big.Int
	PubKey   *big.Int
}

type Result struct {
	Keys Keys
	Err  error
}

type Share struct {
	S       *crypto.ZpElement
	SMasked *crypto.ZpElement
}

type DKG struct {
	types.Deliverer
	ctx    context.Context
	cancel context.CancelFunc

	self      types.ProcessID
	processes []types.ProcessID
	t         int
	beb       broadcaster.Broadcaster
	al        p2p.Link

	params *crypto.Params

	// Фаза 1
	polyPair        *polynomialPair
	polyCommitments map[types.ProcessID][]*crypto.ZpElement
	shares          map[types.ProcessID]Share
	pendingShares   map[types.ProcessID]Share
	complaints1     map[types.ProcessID]map[types.ProcessID]struct{} // dealer -> complainants
	revealed        map[types.ProcessID]map[types.ProcessID]struct{} // dealer -> answered complainants
	noqual          map[types.ProcessID]struct{}

	// Фаза 2/3
	qual    []types.ProcessID
	inQual  map[types.ProcessID]struct{}
	pkShare Share

	// Фаза 4
	pubCommitments map[types.ProcessID][]*crypto.ZpElement
	noqual2        map[types.ProcessID]struct{} // получили валидный complaint2
	pubKey         *crypto.ZpElement

	evtCh  chan fsm.Event
	msgsCh chan types.Message
	resCh  chan Result

	stopOnce sync.Once
	stopCh   chan struct{} // closed → начинается shutdown
	doneCh   chan struct{} // closed → shutdown полностью завершён
	wg       sync.WaitGroup
}

func New(
	ctx context.Context,
	self types.ProcessID,
	processes []types.ProcessID,
	t int,
	beb broadcaster.Broadcaster,
	al p2p.Link,
) *DKG {
	d := new(DKG)
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.self = self
	d.processes = processes
	d.t = t
	d.beb = beb
	d.al = al

	d.Deliverer = types.NewUnaryDeliverer(self)
	beb.AddDeliverer(d, types.DelivererWithMsgNames(
		CommitmentsMsgName, Complaint1MsgName, RevealShareMsgName,
		PubCommitmentsMsgName, Complaint2MsgName,
	))
	al.AddDeliverer(d, types.DelivererWithMsgNames(ShareMsgName))

	d.polyCommitments = make(map[types.ProcessID][]*crypto.ZpElement)
	d.shares = make(map[types.ProcessID]Share)
	d.pendingShares = make(map[types.ProcessID]Share)
	d.complaints1 = make(map[types.ProcessID]map[types.ProcessID]struct{})
	d.revealed = make(map[types.ProcessID]map[types.ProcessID]struct{})
	d.noqual = make(map[types.ProcessID]struct{})
	d.inQual = make(map[types.ProcessID]struct{})
	d.pubCommitments = make(map[types.ProcessID][]*crypto.ZpElement)
	d.noqual2 = make(map[types.ProcessID]struct{})

	d.evtCh = make(chan fsm.Event, len(processes)*4)
	d.msgsCh = make(chan types.Message, len(processes)*4)
	d.resCh = make(chan Result, 1)
	d.stopCh = make(chan struct{})
	d.doneCh = make(chan struct{})

	// shutdown watcher живёт всё время жизни DKG, реагирует на ctx или Stop().
	go d.shutdownWatcher()

	return d
}

func (d *DKG) GenerateAsync(params *crypto.Params) <-chan Result {
	d.wg.Add(2)
	go d.deliverLoop()
	go d.eventLoop()
	d.scheduleEvent(startGenerateEvt{
		BaseEvent: fsm.NewBaseEvent(startGenerateEvtName),
		params:    params,
	})
	return d.resCh
}

// Stop инициирует graceful shutdown и блокируется до полной финализации.
// Идемпотентен: повторные вызовы безопасны.
func (d *DKG) Stop() {
	d.requestStop()
	<-d.doneCh
}

func (d *DKG) requestStop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		d.cancel()
	})
}

func (d *DKG) Deliver(msg types.Message) {
	select {
	case <-d.stopCh:
	case d.msgsCh <- msg:
	}
}

// shutdownWatcher: ждёт сигнала остановки (Stop() или ctx.Done()), затем выполняет
// последовательную финализацию. Это единственное место, где закрываются каналы и
// отписываются deliverer'ы — никаких race на double-close.
func (d *DKG) shutdownWatcher() {
	select {
	case <-d.ctx.Done():
	case <-d.stopCh:
	}
	d.requestStop()

	// Отписка от BEB/AL — после этого Deliver(...) больше вызываться не будет.
	d.beb.RemoveDeliverer(d)
	d.al.RemoveDeliverer(d)

	// Дожидаемся всех писателей в evtCh/msgsCh: eventLoop, deliverLoop,
	// scheduleAfter-горутины. К этому моменту:
	//  - наши внутренние writers видят stopCh и завершаются;
	//  - внешние writers (Deliver) уже не вызываются — мы отписались выше.
	d.wg.Wait()

	// evtCh/msgsCh намеренно НЕ закрываем: внешние writers (Deliver) не учтены
	// в wg, и close + параллельный send → panic. После wg.Wait() читатели
	// исчезли, на каналы никто не ссылается — GC уберёт их вместе с DKG.

	// resCh — единственный исходящий канал; close сигналит потребителю «всё».
	d.resCh <- d.finalResult()
	close(d.resCh)

	close(d.doneCh)
}

func (d *DKG) finalResult() Result {
	if d.pubKey != nil && d.pkShare.S != nil {
		keys, err := d.buildKeys()
		if err != nil {
			return Result{Err: err}
		}
		return Result{Keys: keys}
	}
	if err := d.ctx.Err(); err != nil {
		return Result{Err: err}
	}
	return Result{Err: fmt.Errorf("dkg stopped before completion")}
}

func (d *DKG) eventLoop() {
	defer d.wg.Done()
	for {
		select {
		case <-d.stopCh:
			return
		case evt := <-d.evtCh:
			// одиночный плохой event не валит DKG
			_ = d.applyEvent(evt)
		}
	}
}

func (d *DKG) deliverLoop() {
	defer d.wg.Done()
	for {
		select {
		case <-d.stopCh:
			return
		case msg := <-d.msgsCh:
			d.handleMsg(msg)
		}
	}
}

// scheduleAfter — отложенный event без утечек:
//   - wg.Add синхронен с вызовом, поэтому wg.Wait в shutdown'е её дождётся;
//   - горутина выходит по stopCh, попутно останавливая Timer (defer t.Stop()).
func (d *DKG) scheduleAfter(delay time.Duration, factory func() fsm.Event) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-d.stopCh:
			return
		case <-t.C:
			d.scheduleEvent(factory())
		}
	}()
}

func (d *DKG) buildKeys() (Keys, error) {
	ownPub, ok := d.pubCommitments[d.self]
	if !ok || len(ownPub) == 0 {
		return Keys{}, fmt.Errorf("own pub commitments not built")
	}
	return Keys{
		PkShare:  d.pkShare.S.BigInt(),
		PubShare: ownPub[0].BigInt(),
		PubKey:   d.pubKey.BigInt(),
	}, nil
}

func (d *DKG) handleMsg(msg types.Message) {
	switch m := msg.(type) {
	case CommitmentsMsg:
		d.scheduleEvent(receivedCommitmentsEvt{
			BaseEvent: fsm.NewBaseEvent(receivedCommitmentsEvtName),
			From:      m.From(),
			Comms:     m.Comms,
		})
	case ShareMsg:
		d.scheduleEvent(shareEvt{
			BaseEvent: fsm.NewBaseEvent(shareEvtName),
			From:      m.From(),
			Share:     m.Share,
		})
	case Complaint1Msg:
		d.scheduleEvent(complaint1Evt{
			BaseEvent: fsm.NewBaseEvent(complaint1EvtName),
			From:      m.From(),
			Dealer:    m.Dealer,
		})
	case RevealShareMsg:
		d.scheduleEvent(revealShareEvt{
			BaseEvent: fsm.NewBaseEvent(revealShareEvtName),
			From:      m.From(),
			Target:    m.Target,
			Share:     m.Share,
		})
	case PubCommitmentsMsg:
		d.scheduleEvent(checkPubCommitmentsEvt{
			BaseEvent: fsm.NewBaseEvent(checkPubCommitmentsEvtName),
			From:      m.From(),
			Comms:     m.Comms,
		})
	case Complaint2Msg:
		d.scheduleEvent(complaint2Evt{
			BaseEvent:    fsm.NewBaseEvent(complaint2EvtName),
			From:         m.From(),
			Dealer:       m.Dealer,
			InitShare:    m.InitShare,
			InvalidShare: m.InvalidShare,
		})
	}
}

func (d *DKG) scheduleEvent(evt fsm.Event) {
	select {
	case <-d.stopCh:
	case d.evtCh <- evt:
	}
}

func (d *DKG) applyEvent(event fsm.Event) error {
	switch evt := event.(type) {
	case startGenerateEvt:
		return d.handleStart(evt)
	case receivedCommitmentsEvt:
		return d.handleReceivedCommitments(evt)
	case shareEvt:
		return d.handleShare(evt)
	case complaint1Evt:
		return d.handleComplaint1(evt)
	case revealShareEvt:
		return d.handleRevealShare(evt)
	case generatingPubEvt:
		return d.handleGeneratingPub(evt)
	case checkPubCommitmentsEvt:
		return d.handleCheckPubCommitment(evt)
	case complaint2Evt:
		return d.handleComplaint2(evt)
	case buildPubKeyEvt:
		return d.handleBuildPubKey(evt)
	}
	return nil
}

// ===== Phase 1a — генерация полиномов, бродкаст commitments, p2p рассылка шар. =====

func (d *DKG) handleStart(evt startGenerateEvt) error {
	d.params = evt.params

	comms, err := d.generateCommitments()
	if err != nil {
		return fmt.Errorf("generate commitments: %w", err)
	}

	shares := d.evaluateShares()
	for i, pid := range d.processes {
		msg := ShareMsg{
			BaseMsg: messages.NewBase(uuid.New(), d.self, ShareMsgName),
			Share:   shares[i],
		}
		d.al.Send(pid, msg)
	}

	commsMsg := CommitmentsMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, CommitmentsMsgName),
		Comms:   comms,
	}
	d.beb.Broadcast(d.ctx, commsMsg)

	// TODO: переход по локальному таймеру некорректен в асинхронной (или
	// частично синхронной) модели — у разных честных к моменту срабатывания будет
	// разный view канала, и локальные кандидаты QUAL_j разойдутся.
	//
	// Согласование общего QUAL — это, по сути, задача византийского консенсуса
	// (validated BA / multi-valued BA / common subset): на входе у каждого Pj свой
	// QUAL_j, на выходе у всех честных должен получиться один и тот же QUAL,
	// причём честные дилеры обязаны в него попасть, а явно дисквалифицированные —
	// нет. Без явного BA-раунда таймер этой гарантии не даёт.
	d.scheduleAfter(phase1Duration, func() fsm.Event {
		return generatingPubEvt{BaseEvent: fsm.NewBaseEvent(generatingPubEvtName)}
	})

	return nil
}

func (d *DKG) handleReceivedCommitments(evt receivedCommitmentsEvt) error {
	if _, exists := d.polyCommitments[evt.From]; exists {
		return nil
	}
	if len(evt.Comms) != d.t+1 {
		return fmt.Errorf("commitments from %s: wrong length %d (want %d)",
			evt.From, len(evt.Comms), d.t+1)
	}
	d.polyCommitments[evt.From] = evt.Comms

	if s, ok := d.pendingShares[evt.From]; ok {
		delete(d.pendingShares, evt.From)
		return d.processShare(evt.From, s)
	}
	return nil
}

// ===== Phase 1b — верификация шары, при невалидной — публичная жалоба. =====

func (d *DKG) handleShare(evt shareEvt) error {
	if _, exists := d.shares[evt.From]; exists {
		return nil
	}
	if _, ok := d.polyCommitments[evt.From]; !ok {
		// commitments ещё не пришли — откладываем проверку
		d.pendingShares[evt.From] = evt.Share
		return nil
	}
	return d.processShare(evt.From, evt.Share)
}

func (d *DKG) processShare(from types.ProcessID, s Share) error {
	valid, err := d.checkShare(from, d.self, s)
	if err != nil {
		return fmt.Errorf("check share from %s: %w", from, err)
	}
	if valid {
		d.shares[from] = s
		return nil
	}

	msg := Complaint1Msg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, Complaint1MsgName),
		Dealer:  from,
	}
	d.beb.Broadcast(d.ctx, msg)
	return nil
}

// ===== Phase 1b/1c — учёт жалобы; если жалуются на меня — бродкаст reveal. =====

func (d *DKG) handleComplaint1(evt complaint1Evt) error {
	if _, ok := d.complaints1[evt.Dealer]; !ok {
		d.complaints1[evt.Dealer] = make(map[types.ProcessID]struct{})
	}
	if _, dup := d.complaints1[evt.Dealer][evt.From]; dup {
		return nil
	}
	d.complaints1[evt.Dealer][evt.From] = struct{}{}

	if len(d.complaints1[evt.Dealer]) > d.t {
		d.markNoQUAL(evt.Dealer)
		return nil
	}

	if evt.Dealer != d.self {
		return nil
	}

	revealed := Share{
		S:       d.polyPair.f.Evaluate(evt.From.Int()),
		SMasked: d.polyPair.fMask.Evaluate(evt.From.Int()),
	}
	msg := RevealShareMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, RevealShareMsgName),
		Target:  evt.From,
		Share:   revealed,
	}
	d.beb.Broadcast(d.ctx, msg)
	return nil
}

// ===== Phase 1c/1d — приём публично раскрытой шары; невалидный reveal → дисквал. =====

func (d *DKG) handleRevealShare(evt revealShareEvt) error {
	// reveal валиден только в ответ на ранее опубликованную жалобу
	complainants, ok := d.complaints1[evt.From]
	if !ok {
		return nil
	}
	if _, ok := complainants[evt.Target]; !ok {
		return nil
	}
	if _, ok := d.polyCommitments[evt.From]; !ok {
		// reveal без опубликованных commitments проверить нельзя — дилер невалиден
		d.markNoQUAL(evt.From)
		return nil
	}

	valid, err := d.checkShare(evt.From, evt.Target, evt.Share)
	if err != nil {
		return fmt.Errorf("check revealed share: %w", err)
	}
	if !valid {
		d.markNoQUAL(evt.From)
		return nil
	}

	if _, ok := d.revealed[evt.From]; !ok {
		d.revealed[evt.From] = make(map[types.ProcessID]struct{})
	}
	d.revealed[evt.From][evt.Target] = struct{}{}

	// reveal адресован мне — это итоговая шара от данного дилера
	if evt.Target == d.self {
		d.shares[evt.From] = evt.Share
	}
	return nil
}

// ===== Phase 2/3/4a — финализация QUAL, сборка pkShare, бродкаст pub commitments. =====

func (d *DKG) handleGeneratingPub(_ generatingPubEvt) error {
	// 1d: если на дилера есть жалоба без валидного reveal — он дисквалифицируется.
	for dealer, complainants := range d.complaints1 {
		if _, alreadyBad := d.noqual[dealer]; alreadyBad {
			continue
		}
		for c := range complainants {
			if _, answered := d.revealed[dealer][c]; !answered {
				d.markNoQUAL(dealer)
				break
			}
		}
	}
	// дилер без опубликованных commitments — тоже noqual
	for _, pid := range d.processes {
		if _, ok := d.polyCommitments[pid]; !ok {
			d.markNoQUAL(pid)
		}
	}

	d.buildQUAL()
	d.buildPKShare()

	comms := d.buildPubCommitments()
	d.pubCommitments[d.self] = comms

	msg := PubCommitmentsMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, PubCommitmentsMsgName),
		Comms:   comms,
	}
	d.beb.Broadcast(d.ctx, msg)

	d.scheduleAfter(phase4Duration, func() fsm.Event {
		return buildPubKeyEvt{BaseEvent: fsm.NewBaseEvent(buildPubKeyEvtName)}
	})

	return nil
}

// ===== Phase 4b — приём Feldman commitments, проверка, при несоответствии — жалоба. =====

func (d *DKG) handleCheckPubCommitment(evt checkPubCommitmentsEvt) error {
	if _, mine := d.inQual[evt.From]; !mine {
		return nil
	}
	if _, exists := d.pubCommitments[evt.From]; exists {
		return nil
	}
	if len(evt.Comms) != d.t+1 {
		return fmt.Errorf("pub commitments from %s: wrong length %d (want %d)",
			evt.From, len(evt.Comms), d.t+1)
	}
	d.pubCommitments[evt.From] = evt.Comms

	if evt.From == d.self {
		return nil
	}
	myShare, ok := d.shares[evt.From]
	if !ok {
		return nil
	}
	if d.checkPubShare(d.self, myShare.S, evt.Comms) {
		return nil
	}

	msg := Complaint2Msg{
		BaseMsg:      messages.NewBase(uuid.New(), d.self, Complaint2MsgName),
		Dealer:       evt.From,
		InitShare:    myShare,
		InvalidShare: g(d.params).Exp(myShare.S.BigInt()),
	}
	d.beb.Broadcast(d.ctx, msg)
	return nil
}

// ===== Phase 4c — приём complaint2; при валидной жалобе дилер требует реконструкции. =====

func (d *DKG) handleComplaint2(evt complaint2Evt) error {
	if _, mine := d.inQual[evt.Dealer]; !mine {
		return nil
	}
	comms, ok := d.pubCommitments[evt.Dealer]
	if !ok {
		return nil
	}

	pedersenOK, err := d.checkShare(evt.Dealer, evt.From, evt.InitShare)
	if err != nil {
		return fmt.Errorf("check pedersen in complaint2: %w", err)
	}
	if !pedersenOK {
		return nil
	}
	if d.checkPubShare(evt.From, evt.InitShare.S, comms) {
		return nil
	}

	// TODO: doc.md шаг 4c требует запустить реконструкцию Pedersen-VSS.
	// До её реализации помечаем дилера как «непригодный для extraction».
	d.noqual2[evt.Dealer] = struct{}{}
	return nil
}

// ===== Phase 4d — сборка публичного ключа y = ∏ A_{i0} для i ∈ QUAL. =====

func (d *DKG) handleBuildPubKey(_ buildPubKeyEvt) error {
	// финальное событие фазы 4: в любом исходе инициируем shutdown,
	// чтобы Result доставился наружу через shutdownWatcher.
	defer d.requestStop()

	if len(d.noqual2) > 0 {
		return fmt.Errorf("reconstruction required for %d dealers but not implemented", len(d.noqual2))
	}

	pubShares := make([]*crypto.ZpElement, 0, len(d.qual))
	for _, pid := range d.qual {
		comms, ok := d.pubCommitments[pid]
		if !ok {
			return fmt.Errorf("pub commitments for %s not received", pid)
		}
		pubShares = append(pubShares, comms[0])
	}
	if len(pubShares) == 0 {
		return fmt.Errorf("empty QUAL — cannot build public key")
	}

	y := pubShares[0]
	for i := 1; i < len(pubShares); i++ {
		y = y.Mul(pubShares[i])
	}

	d.pubKey = y
	return nil
}

// ===== Чистые хелперы. =====

func (d *DKG) buildQUAL() {
	qual := make([]types.ProcessID, 0, len(d.processes))
	for _, pid := range d.processes {
		if _, bad := d.noqual[pid]; bad {
			d.beb.RemoveCorrect(pid)
			continue
		}
		qual = append(qual, pid)
		d.inQual[pid] = struct{}{}
	}
	d.qual = qual
}

func (d *DKG) markNoQUAL(pid types.ProcessID) {
	d.noqual[pid] = struct{}{}
}

func (d *DKG) generateCommitments() ([]*crypto.ZpElement, error) {
	pair, err := generatePolynomials(d.params, d.t)
	if err != nil {
		return nil, fmt.Errorf("generate polynomials: %w", err)
	}
	d.polyPair = pair

	gen := g(d.params)
	hen := h(d.params)

	commitments := make([]*crypto.ZpElement, 0, d.t+1)
	for k := 0; k <= d.t; k++ {
		a := pair.f.coefficients[k]
		b := pair.fMask.coefficients[k]
		commitments = append(commitments, gen.Exp(a.BigInt()).Mul(hen.Exp(b.BigInt())))
	}
	return commitments, nil
}

// buildPubCommitments — Feldman commitments: A_k = g^{a_k}, k = 0..t.
func (d *DKG) buildPubCommitments() []*crypto.ZpElement {
	gen := g(d.params)
	out := make([]*crypto.ZpElement, 0, d.t+1)
	for k := 0; k <= d.t; k++ {
		out = append(out, gen.Exp(d.polyPair.f.coefficients[k].BigInt()))
	}
	return out
}

// buildPKShare — x_self = Σ_{i ∈ QUAL} s_{i,self}, аналогично для маски.
func (d *DKG) buildPKShare() {
	sumS := crypto.ZpZero(d.params.Q)
	sumMasked := crypto.ZpZero(d.params.Q)
	for _, pid := range d.qual {
		s, ok := d.shares[pid]
		if !ok {
			// валидной шары от участника QUAL нет; после реализации шага 4c
			// она будет восстановлена. Пока пропускаем.
			continue
		}
		sumS = sumS.Add(s.S)
		sumMasked = sumMasked.Add(s.SMasked)
	}
	d.pkShare = Share{S: sumS, SMasked: sumMasked}
}

func (d *DKG) evaluateShares() []Share {
	result := make([]Share, 0, len(d.processes))
	for _, pid := range d.processes {
		s := d.polyPair.f.Evaluate(pid.Int())
		sMask := d.polyPair.fMask.Evaluate(pid.Int())
		result = append(result, Share{s, sMask})
	}
	return result
}

// checkShare: g^s · h^s' == ∏_{k=0..t} (C_{dealer,k})^{target^k}
func (d *DKG) checkShare(dealer types.ProcessID, target types.ProcessID, s Share) (bool, error) {
	comms, exists := d.polyCommitments[dealer]
	if !exists {
		return false, fmt.Errorf("commitments from %s not present", dealer)
	}
	if len(comms) != d.t+1 {
		return false, fmt.Errorf("commitments from %s wrong length %d (want %d)",
			dealer, len(comms), d.t+1)
	}

	right := evalCommitmentLine(comms, target, d.t)
	left := g(d.params).Exp(s.S.BigInt()).Mul(h(d.params).Exp(s.SMasked.BigInt()))
	return left.Equal(right), nil
}

// checkPubShare: g^s == ∏_{k=0..t} (A_k)^{target^k}
func (d *DKG) checkPubShare(target types.ProcessID, s *crypto.ZpElement, comms []*crypto.ZpElement) bool {
	if len(comms) != d.t+1 {
		return false
	}
	right := evalCommitmentLine(comms, target, d.t)
	left := g(d.params).Exp(s.BigInt())
	return left.Equal(right)
}

// evalCommitmentLine — общая формула ∏_{k=0..t} (C_k)^{target^k}.
func evalCommitmentLine(comms []*crypto.ZpElement, target types.ProcessID, t int) *crypto.ZpElement {
	j := big.NewInt(target.Int64())
	res := comms[0]
	for k := 1; k <= t; k++ {
		deg := new(big.Int).Exp(j, big.NewInt(int64(k)), nil)
		res = res.Mul(comms[k].Exp(deg))
	}
	return res
}

func g(p *crypto.Params) *crypto.ZpElement { return crypto.NewZpElement(p.G, p.P) }
func h(p *crypto.Params) *crypto.ZpElement { return crypto.NewZpElement(p.H, p.P) }
