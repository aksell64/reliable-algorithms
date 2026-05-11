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
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	complaintReceiptTime = 10 * time.Second
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
	S       *ZpElement
	SMasked *ZpElement
}

type complaintWaiter struct {
	startTime time.Time
}

type DKG struct {
	types.Deliverer
	ctx                context.Context
	self               types.ProcessID
	processes          []types.ProcessID
	t                  int
	beb                broadcaster.Broadcaster
	al                 p2p.Link
	params             *Params
	polyPair           *polynomialPair
	polyCommitments    map[types.ProcessID][]*ZpElement
	shares             map[types.ProcessID]Share
	noqual             map[types.ProcessID]struct{}
	qual               []types.ProcessID
	pkShare            Share
	complaints1        map[types.ProcessID]map[types.ProcessID]struct{}
	complaints1Waiters map[types.ProcessID]map[types.ProcessID]complaintWaiter
	pubCommitments     map[types.ProcessID][]*ZpElement
	pubKey             *ZpElement

	evtCh   chan fsm.Event
	msgsCh  chan types.Message
	stopped bool
	resCh   chan Result
}

func (d *DKG) GenerateAsync() <-chan Result {

}

func (d *DKG) eventLoop() {
	var lerr error
	for {
		if d.stopped {
			d.cleanup()
			if lerr != nil {
				d.resCh <- Result{Err: lerr}
				return
			}
			keys, err := d.buildKeys()
			if err != nil {
				d.resCh <- Result{Err: err}
				return
			}
			d.resCh <- Result{Keys: keys}
			return
		}

		select {
		case <-d.ctx.Done():
			lerr = d.ctx.Err()
			d.stopped = true
			continue

		case evt := <-d.evtCh:
			err := d.handleEvent(evt)
			if err != nil {
				d.stopped = true
				lerr = err
				continue
			}
		}
	}
}

func (d *DKG) deliverLoop() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case msg := <-d.msgsCh:
			d.handleMsg(msg)
		}
	}
}

func (d *DKG) buildKeys() (Keys, error) {

}

func (d *DKG) cleanup() {}

func (d *DKG) handleMsg(msg types.Message) {
	switch m := msg.(type) {
	case CommitmentsMsg:
		evt := receiptCommitmentsEvt{
			BaseEvent: fsm.NewBaseEvent(),
		}
	}
}

func (d *DKG) handleEvent(event fsm.Event) error {
	var err error
	switch evt := event.(type) {
	case startGenerateEvt:
		err = d.handleStart(evt)
	case receiptCommitmentsEvt:
		err = d.handleReceiptCommitments(evt)
	case shareEvt:
		err = d.handleShare(evt)
	case complaint1Evt:
		err = d.handleComplaint1(evt)
	case revealShareEvt:
		err = d.handleRevealShare(evt)
	case generatingPubEvt:
		err = d.handleGeneratingPub(evt)
	case checkPubCommitmentsEvt:
		err = d.handleReceiptPubCommitment(evt)
	case buildPubKeyEvt:
		err = d.handleBuildPubKey(evt)
	}
	return err
}

func (d *DKG) handleStart(evt startGenerateEvt) error {
	d.params = evt.params
	comms, err := d.generateCommitments()
	if err != nil {
		return fmt.Errorf("generate commitments: %w", err)
	}

	commsMsg := CommitmentsMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, CommitmentsMsgName),
		Comms:   comms,
	}

	d.beb.Broadcast(d.ctx, commsMsg)

	shares := d.evaluateShares()

	wg := sync.WaitGroup{}
	wg.Add(len(shares))

	for i, s := range shares {
		pid := d.processes[i]
		go func() {
			defer wg.Done()
			msg := ShareMsg{
				BaseMsg: messages.NewBase(uuid.New(), d.self, ShareMsgName),
				Share:   s,
			}
			d.al.Send(pid, msg)
		}()
	}

	wg.Wait()

	return nil
}

func (d *DKG) handleReceiptCommitments(evt receiptCommitmentsEvt) error {
	d.polyCommitments[evt.From] = evt.Comms
	return nil
}

func (d *DKG) handleShare(evt shareEvt) error {
	valid, err := d.checkShare(evt.From, d.self, evt.Share)
	if err != nil {
		return fmt.Errorf("check share: %w", err)
	}

	if valid {
		return nil
	}

	msg := Complaint1Msg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, Complaint1MsgName),
		Dealer:  d.self,
	}

	d.beb.Broadcast(d.ctx, msg)
	return nil
}

func (d *DKG) handleComplaint1(evt complaint1Evt) error {
	_, ok := d.complaints1[evt.Dealer]
	if !ok {
		d.complaints1[evt.Dealer] = make(map[types.ProcessID]struct{})
	}
	_, ok = d.complaints1[evt.Dealer][evt.From]
	if ok {
		return nil
	}
	d.complaints1[evt.Dealer][evt.From] = struct{}{}
	count := 0
	for range d.complaints1[evt.Dealer] {
		count++
	}
	if count > d.t {
		d.markNoQUAL(evt.Dealer)
		return nil
	}
	if _, ok := d.complaints1Waiters[evt.Dealer]; !ok {
		d.complaints1Waiters[evt.Dealer] = make(map[types.ProcessID]complaintWaiter)
	}
	_, ok = d.complaints1Waiters[evt.Dealer][evt.From]
	if !ok {
		d.complaints1Waiters[evt.Dealer][evt.From] = complaintWaiter{time.Now()}
	}
	return nil
}

func (d *DKG) handleRevealShare(evt revealShareEvt) error {
	_, ok := d.complaints1Waiters[evt.From]
	if !ok {
		return nil
	}
	_, ok = d.complaints1Waiters[evt.From][evt.Target]
	if !ok {
		return nil
	}

	valid, err := d.checkShare(evt.From, evt.Target, evt.Share)
	if err != nil {
		return fmt.Errorf("check share: %w", err)
	}

	if !valid {
		d.markNoQUAL(evt.From)
		return nil
	}

	return nil
}

func (d *DKG) handleGeneratingPub(_ generatingPubEvt) error {
	d.buildQUAL()

	comms := d.buildPubCommitments()
	msg := PubCommitmentsMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, PubCommitmentsMsgName),
		Comms:   comms,
	}
	d.beb.Broadcast(d.ctx, msg)
	return nil
}

func (d *DKG) handleReceiptPubCommitment(evt checkPubCommitmentsEvt) error {
	comms, ok := d.pubCommitments[evt.From]
	if !ok {
		return fmt.Errorf("commitments not exists")
	}

	valid := d.checkPubShare(d.self, evt.Share, comms)
	if !valid {
		initShare, ok := d.shares[evt.From]
		if !ok {
			return fmt.Errorf("shares not exists")
		}
		msg := Complaint2Msg{
			BaseMsg:      messages.NewBase(uuid.New(), d.self, Complaint2MsgName),
			Dealer:       evt.From,
			InitShare:    initShare,
			InvalidShare: evt.Share,
		}
		d.beb.Broadcast(d.ctx, msg)
	}

	return nil
}

func (d *DKG) handleBuildPubKey(_ buildPubKeyEvt) error {
	pubShares := make([]*ZpElement, 0, len(d.qual))
	for _, pid := range d.qual {
		comms, ok := d.pubCommitments[pid]
		if !ok {
			return fmt.Errorf("commitments not exists")
		}
		pubShare := comms[0]
		pubShares = append(pubShares, pubShare)
	}

	y := pubShares[0]
	for i := 1; i < len(pubShares); i++ {
		y = y.Mul(pubShares[i])
	}

	d.pubKey = y
	return nil
}

func (d *DKG) buildPubCommitments() []*ZpElement {
	g := NewZpElement(d.params.G, d.params.P)
	commitments := make([]*ZpElement, 0, d.t)
	for i := 0; i < d.t; i++ {
		fC := d.polyPair.f.coefficients[i]
		cg := g.Exp(fC.BigInt())
		commitments = append(commitments, cg)
	}
	return commitments
}

func (d *DKG) buildQUAL() {
	qual := make([]types.ProcessID, 0, len(d.processes))
	for _, pid := range d.processes {
		qual = append(qual, pid)
	}
	for noqualPid, _ := range d.noqual {
		for i, qualPid := range qual {
			if noqualPid == qualPid {
				qual = append(qual[:i], qual[:i]...)
			}
		}
		d.beb.RemoveCorrect(noqualPid)
	}
	d.qual = qual
}

func (d *DKG) markNoQUAL(pid types.ProcessID) {
	d.noqual[pid] = struct{}{}
}

func (d *DKG) generateCommitments() ([]*ZpElement, error) {
	pair, err := generatePolynomials(d.params, d.t)
	if err != nil {
		return nil, fmt.Errorf("generate polynomials: %w", err)
	}
	d.polyPair = pair

	g := NewZpElement(d.params.G, d.params.P)
	h := NewZpElement(d.params.H, d.params.P)

	commitments := make([]*ZpElement, 0, d.t)
	for i := 0; i < d.t; i++ {
		fC := pair.f.coefficients[i]
		fMaskC := pair.fMask.coefficients[i]
		cg := g.Exp(fC.BigInt())
		ch := h.Exp(fMaskC.BigInt())
		commitment := cg.Mul(ch)
		commitments = append(commitments, commitment)
	}

	return commitments, nil
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

func (d *DKG) checkShare(pidFrom types.ProcessID, pidTo types.ProcessID, s Share) (bool, error) {
	comms, exists := d.polyCommitments[pidFrom]
	if !exists {
		return false, fmt.Errorf("commitments not exists")
	}

	ipid := big.NewInt(pidTo.Int64())
	res := comms[0]
	for i := 1; i < d.t; i++ {
		deg := ipid.Exp(ipid, big.NewInt(int64(i)), nil)
		c := comms[i].Exp(deg)
		res = res.Mul(c)
	}
	g := NewZpElement(d.params.G, d.params.P)
	h := NewZpElement(d.params.H, d.params.P)
	gShare := g.Exp(s.S.BigInt())
	hShare := h.Exp(s.SMasked.BigInt())
	left := gShare.Mul(hShare)

	return left.Equal(res), nil
}

func (d *DKG) checkPubShare(pidTo types.ProcessID, s *ZpElement, comms []*ZpElement) bool {
	ipid := big.NewInt(pidTo.Int64())
	res := comms[0]
	for i := 1; i < d.t; i++ {
		deg := ipid.Exp(ipid, big.NewInt(int64(i)), nil)
		c := comms[i].Exp(deg)
		res = res.Mul(c)
	}
	g := NewZpElement(d.params.G, d.params.P)
	gShare := g.Exp(s.BigInt())

	return gShare.Equal(res)
}

func (d *DKG) buildPKShare() {
	shares := make([]Share, 0, len(d.shares))
	for _, s := range d.shares {
		shares = append(shares, s)
	}

	shareS := shares[0].S
	shareSMasked := shares[0].SMasked
	for i := 1; i < len(d.shares); i++ {
		share := shares[i]
		shareS = shareS.Add(share.S)
		shareSMasked = shareSMasked.Add(share.SMasked)
	}

	d.pkShare = Share{shareS, shareSMasked}
}

func (d *DKG) buildPubKeyCommitments() []*ZpElement {
	comms := make([]*ZpElement, 0, len(d.polyPair.f.coefficients))
	g := NewZpElement(d.params.G, d.params.P)
	for _, c := range d.polyPair.f.coefficients {
		comm := g.Exp(c.BigInt())
		comms = append(comms, comm)
	}

	return comms
}
