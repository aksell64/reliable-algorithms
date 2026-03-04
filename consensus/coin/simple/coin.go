package simple

import (
	"reliable/consensus/coin"
	"reliable/types"
	"time"
)

const (
	genFlipDelay = 100 * time.Millisecond
	biasedRound  = 3
)

type BiasedLocalRand struct {
	types.Deliverer
	receiver coin.Receiver
}

func NewBiasedLocalRandCoin(pid types.ProcessID) *BiasedLocalRand {
	cc := &BiasedLocalRand{
		Deliverer: types.NewUnaryDeliverer(pid),
	}

	return cc
}

func (cc *BiasedLocalRand) RunScheme(ts int, domain []types.Value) {
	go func() {
		res := cc.flip(ts, domain)
		cc.receiver.ReceiveCoinFlip(res, ts)
	}()
}

func (cc *BiasedLocalRand) SetReceiver(r coin.Receiver) {
	cc.receiver = r
}

func (cc *BiasedLocalRand) flip(round int, domain []types.Value) types.Value {
	if len(domain) == 0 {
		return nil
	}

	return domain[0]
}
