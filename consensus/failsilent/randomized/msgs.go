package randomized

import (
	"reliable/messages"
)

const (
	ProposalMsgName = "proposal"
	PhaseMsgName    = "phase"
	DecidedMsgName  = "decided"
)

type ProposalMsg struct {
	messages.BaseMsg
	ValueRaw messages.RawMsg
}

func (m ProposalMsg) Type() string {
	return ProposalMsgName
}

type PhaseMsg struct {
	messages.BaseMsg
	Phase       string
	Round       int
	ProposalRaw *messages.RawMsg
}

func (m PhaseMsg) Type() string {
	return PhaseMsgName
}

type DecidedMsg struct {
	messages.BaseMsg
	DecidedRaw messages.RawMsg
}

func (m DecidedMsg) Type() string {
	return DecidedMsgName
}
