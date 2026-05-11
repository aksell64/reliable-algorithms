package dkg

import (
	"reliable/messages"
	"reliable/types"
)

const (
	CommitmentsMsgName    = "commitments"
	ShareMsgName          = "share"
	Complaint1MsgName     = "complaint1"
	RevealShareMsgName    = "revealShare"
	PubCommitmentsMsgName = "pubCommitments"
	Complaint2MsgName     = "complaint2"
)

type CommitmentsMsg struct {
	messages.BaseMsg
	Comms []*ZpElement
}

type ShareMsg struct {
	messages.BaseMsg
	Share Share
}

// Complaint1Msg — публичная жалоба на дилера Dealer.
type Complaint1Msg struct {
	messages.BaseMsg
	Dealer types.ProcessID
}

// RevealShareMsg — публичный ответ дилера на жалобу: раскрывает шару,
// выданную ранее адресату Target.
type RevealShareMsg struct {
	messages.BaseMsg
	Target types.ProcessID
	Share  Share
}

type PubCommitmentsMsg struct {
	messages.BaseMsg
	Comms []*ZpElement
}

type Complaint2Msg struct {
	messages.BaseMsg
	Dealer       types.ProcessID
	InitShare    Share
	InvalidShare *ZpElement
}
