package coin

import "reliable/types"

type Receiver interface {
	ReceiveCoinFlip(val types.Value, ts int)
}

type TsCoinScheme interface {
	RunScheme(ts int, domain []types.Value)
	SetReceiver(r Receiver)
}
