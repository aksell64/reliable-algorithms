package dkg

import (
	"reliable/types"
	"reliable/types/fsm"
)

const (
	startGenerateEvtName       = "start"
	receiptCommitmentsEvtName  = "receiptCommitments"
	shareEvtName               = "share"
	complaint1EvtName          = "complaint1"
	revealShareEvtName         = "revealShare"
	generatingPubEvtName       = "generatingPub"
	checkPubCommitmentsEvtName = "checkPubCommitments"
)

type startGenerateEvt struct {
	fsm.BaseEvent
	params *Params
}

type receiptCommitmentsEvt struct {
	fsm.BaseEvent
	From  types.ProcessID
	Comms []*ZpElement
}

type shareEvt struct {
	fsm.BaseEvent
	From  types.ProcessID
	Share Share
}

// complaint1Evt — публичная жалоба participant `From` на дилера `Dealer`
// (получатель не смог провалидировать выданную шару).
type complaint1Evt struct {
	fsm.BaseEvent
	From   types.ProcessID
	Dealer types.ProcessID
}

// revealShareEvt — публичный ответ дилера `From` на жалобу от `Target`:
// раскрывает шару, которая должна удовлетворять Pedersen-проверке.
type revealShareEvt struct {
	fsm.BaseEvent
	From   types.ProcessID
	Target types.ProcessID
	Share  Share
}

type generatingPubEvt struct {
	fsm.BaseEvent
}

type checkPubCommitmentsEvt struct {
	fsm.BaseEvent
	From  types.ProcessID
	Comms []*ZpElement
	Share *ZpElement
}

type complaint2Evt struct {
	fsm.BaseEvent
	From   types.ProcessID
	Dealer types.ProcessID
}

type buildPubKeyEvt struct {
	fsm.BaseEvent
}
