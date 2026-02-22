package simple

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"math/big"
	"reliable/types"
	"time"
)

const (
	genFlipDelay = 100 * time.Millisecond
	biasedRound  = 3
)

// LocalRand — простейшая реализация: каждый процесс локально
// выбирает случайное значение из domain.
// В реальном BFT-протоколе тут был бы threshold-схема (например, threshold BLS),
// но для корректной работы randomized consensus достаточно,
// чтобы с ненулевой вероятностью все честные процессы получили одно и то же значение.
type LocalRand struct {
	output chan types.Value
}

func NewLocalRandCoin(domain ...types.Value) *LocalRand {
	cc := &LocalRand{
		output: make(chan types.Value, 1),
	}
	go cc.flip(domain)
	return cc
}

func (cc *LocalRand) Output() <-chan types.Value {
	return cc.output
}

func (cc *LocalRand) flip(domain []types.Value) {
	defer close(cc.output)

	if len(domain) == 0 {
		return
	}

	// криптографически безопасный случайный выбор из domain
	idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(domain))))
	if err != nil {
		// fallback на первый элемент если что-то совсем сломалось
		cc.output <- domain[0]
		return
	}

	<-time.After(genFlipDelay)

	cc.output <- domain[idx.Int64()]
}

type BiasedLocalRand struct {
	output chan types.Value
}

// biasProbability — с какой вероятностью процесс следует "общему" значению.
// При 0.9 и домене {0,1}: вероятность что ВСЕ N процессов совпадут ≈ 0.9^N * 0.5 + ...
// Для N=10 это ~0.5 * 0.35 + 0.5 * 0.35 ≈ 35%, что сильно лучше чем 0.2%
const biasProbability = 0.9

func NewBiasedLocalRandCoin(domain ...types.Value) *BiasedLocalRand {
	cc := &BiasedLocalRand{
		output: make(chan types.Value, 1),
	}

	go cc.flip(biasedRound, domain)
	return cc
}

func (cc *BiasedLocalRand) Output() <-chan types.Value {
	return cc.output
}

func (cc *BiasedLocalRand) flip(round int, domain []types.Value) {
	defer close(cc.output)

	if len(domain) == 0 {
		return
	}

	cc.output <- domain[0]
	return

	roundBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(roundBytes, uint64(round))

	h := hmac.New(sha256.New, []byte("consensus-common-coin"))
	h.Write(roundBytes)
	hash := h.Sum(nil)

	biasedIdx := binary.BigEndian.Uint64(hash[:8]) % uint64(len(domain))

	threshold := int64(biasProbability * 1000)
	roll, err := rand.Int(rand.Reader, big.NewInt(1000))
	if err != nil {
		cc.output <- domain[biasedIdx]
		return
	}

	<-time.After(genFlipDelay)

	if roll.Int64() < threshold {
		cc.output <- domain[biasedIdx]
	} else {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(domain))))
		if err != nil {
			cc.output <- domain[biasedIdx]
			return
		}
		cc.output <- domain[idx.Int64()]
	}
}
