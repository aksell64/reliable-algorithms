package consensus

import (
	"context"
	"reliable/logger"
	"reliable/types"
	"sync"
)

type Consensus interface {
	types.Layer
	Propose(v types.Value)
	Decided() <-chan types.Value
	Crashed() chan struct{}
}

type DeterministicSelector interface {
	Select(value []types.Value) types.Value
}

type minSelector struct {
}

func NewMinSelector() DeterministicSelector {
	return minSelector{}
}

func (s minSelector) Select(values []types.Value) types.Value {
	if len(values) == 1 {
		return values[0]
	}

	var minVal types.Value
	minVal = values[0]
	for _, val := range values[1:] {
		if val.Less(minVal) {
			minVal = val
		}
	}
	return minVal
}

type EpochStarter interface {
	StartEpoch(ts int, leader types.ProcessID)
}

type Decided struct {
	ProcessID types.ProcessID
	Value     types.Value
}

func PrintDecided(vals []Decided) {
	logger.Logger().Info().Any("decided", vals).Msg("result")
}

func CollectDecided(ctx context.Context, nodes ...Consensus) []Decided {
	results := make(chan Decided, len(nodes))
	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
			case <-node.Crashed():
			case v, ok := <-node.Decided():
				if !ok {
					return
				}
				decided := Decided{
					ProcessID: node.ProcessID(),
					Value:     v,
				}
				results <- decided
			}
		}()
	}

	wg.Wait()
	close(results)

	result := make([]Decided, 0, len(nodes))
	for decided := range results {
		result = append(result, decided)
	}
	return result
}

func MustUniqAgreement(decided []Decided) []Decided {
	if len(decided) == 0 {
		return decided
	}

	val := decided[0].Value
	for _, d := range decided {
		if val != d.Value {
			panic("Unique Agreement violation")
		}
	}
	return decided
}
