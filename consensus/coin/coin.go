package coin

import "reliable/types"

type CommonCoin interface {
	Output() <-chan types.Value
}
