package randomized

import "reliable/types"

const (
	ProposalMsgName = "proposal"
	PhaseMsgName    = "phase"
	DecidedMsgName  = "decided"
)

type ProposalMsg struct {
	types.Message
	Value types.Value
}

type PhaseMsg struct {
	types.Message
	Phase    string
	Round    int
	Proposal *types.Value
}

type DecidedMsg struct {
	types.Message
	Decided types.Value
}
