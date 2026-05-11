package byzantine

import (
	"fmt"
	"reliable/types"
	"reliable/types/inbox"
)

const (
	collectorInboxPrefix   = "byz_cc"
	rwConsensusInboxPrefix = "byz_rw"
)

type tsMsgsInbox struct {
	inner *inbox.Inbox
	ts    int
}

func (i *tsMsgsInbox) storeCCMsg(msg types.Message) error {
	return i.inner.Store(inbox.NewStringKey(i.genTsKey(collectorInboxPrefix)), msg)
}

func (i *tsMsgsInbox) storeRWMsg(msg types.Message) error {
	return i.inner.Store(inbox.NewStringKey(i.genTsKey(rwConsensusInboxPrefix)), msg)
}

func (i *tsMsgsInbox) drainCCMsgs() ([]types.Message, error) {
	return drainInboxedMsgs(i.inner, i.genTsKey(collectorInboxPrefix))
}

func (i *tsMsgsInbox) drainRwMsgs() ([]types.Message, error) {
	return drainInboxedMsgs(i.inner, i.genTsKey(rwConsensusInboxPrefix))
}

func (i *tsMsgsInbox) clear() {
	_, _ = drainInboxedMsgs(i.inner, i.genTsKey(rwConsensusInboxPrefix))
	_, _ = drainInboxedMsgs(i.inner, i.genTsKey(collectorInboxPrefix))
}

func drainInboxedMsgs(i *inbox.Inbox, key string) ([]types.Message, error) {
	vals, err := i.GetAndClear(inbox.NewStringKey(key))
	if err != nil {
		return nil, err
	}

	return castInboxValsToMsgs(vals)
}

func castInboxValsToMsgs(vals []inbox.Value) ([]types.Message, error) {
	msgs := make([]types.Message, 0, len(vals))
	for _, v := range vals {
		m, ok := v.(types.Message)
		if !ok {
			return nil, fmt.Errorf("invalid msg type: %T", v)
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (i *tsMsgsInbox) genTsKey(k string) string {
	return fmt.Sprintf("%d_%s", i.ts, k)
}
