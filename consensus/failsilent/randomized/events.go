package randomized

import "reliable/types"

const (
	proposalEvtName = "proposalEvt"
	proposeEvtName  = "proposeEvt"
	phaseEvtName    = "phaseEvt"
	coinEvtName     = "coinEvt"
	decidedEvtName  = "decidedEvt"
)

type event interface {
	Name() string
	mustBeEmbeddedRandNodeEvent()
}

type proposalEvt struct {
	val types.Value
}

func (e proposalEvt) Name() string                 { return proposalEvtName }
func (e proposalEvt) mustBeEmbeddedRandNodeEvent() {}

type proposeEvt struct {
	val types.Value
}

func (e proposeEvt) Name() string                 { return proposeEvtName }
func (e proposeEvt) mustBeEmbeddedRandNodeEvent() {}

type phaseEvt struct {
	from     types.ProcessID
	phase    string
	round    int
	proposal *types.Value
}

func (e phaseEvt) Name() string                 { return phaseEvtName }
func (e phaseEvt) mustBeEmbeddedRandNodeEvent() {}

type coinEvt struct {
	output types.Value
	round  int
}

func (e coinEvt) Name() string                 { return coinEvtName }
func (e coinEvt) mustBeEmbeddedRandNodeEvent() {}

type decidedEvt struct {
	val types.Value
}

func (e decidedEvt) Name() string                 { return decidedEvtName }
func (e decidedEvt) mustBeEmbeddedRandNodeEvent() {}
