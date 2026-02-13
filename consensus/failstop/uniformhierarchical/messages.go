package uniformhierarchical

import "reliable/types"

const (
	AckMsgName      = "ack"
	DecidedMsgName  = "decide"
	ProposalMsgName = "proposal"
)

type AckMsg struct {
	types.Message
}

type DecidedMsg struct {
	types.Message
	Decision types.Value
}

type ProposalMsg struct {
	types.Message
	Proposal types.Value
}
