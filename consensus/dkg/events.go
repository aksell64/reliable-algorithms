package dkg

import (
	"reliable/types"
	"reliable/types/fsm"
	"reliable/utils/crypto"
)

const (
	startGenerateEvtName       = "start"
	receivedCommitmentsEvtName = "receivedCommitments"
	shareEvtName               = "share"
	complaint1EvtName          = "complaint1"
	revealShareEvtName         = "revealShare"
	generatingPubEvtName       = "generatingPub"
	checkPubCommitmentsEvtName = "checkPubCommitments"
	complaint2EvtName          = "complaint2"
	buildPubKeyEvtName         = "buildPubKey"
)

type startGenerateEvt struct {
	fsm.BaseEvent
	params *crypto.Params
}

type receivedCommitmentsEvt struct {
	fsm.BaseEvent
	From  types.ProcessID
	Comms []*crypto.ZpElement
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
	Comms []*crypto.ZpElement
	Share *crypto.ZpElement
}

type complaint2Evt struct {
	fsm.BaseEvent
	From         types.ProcessID
	Dealer       types.ProcessID
	InitShare    Share
	InvalidShare *crypto.ZpElement
}

type buildPubKeyEvt struct {
	fsm.BaseEvent
}
