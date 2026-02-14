package utils

import (
	"context"
	"math/rand"
	"reliable/types"
	"sync/atomic"
	"time"
)

type Pauser interface {
	Pause()
	Resume()
	Checkpoint()
}

type Sig struct {
	ctx    context.Context
	paused atomic.Bool
}

func NewSig(ctx context.Context) *Sig {
	s := &Sig{ctx: ctx}
	s.paused.Store(false)
	return s
}

func (s *Sig) Checkpoint() {
	for s.paused.Load() {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Sig) Pause() {
	s.paused.Store(true)
}

func (s *Sig) Resume() {
	s.paused.Store(false)
}

func ExecuteWithRandomDelay(minDelay, maxDelay time.Duration, fn func()) {
	executeWithRandomDelay(minDelay, maxDelay, fn)
}

func RandomSleep(minDelay, maxDelay time.Duration) {
	executeWithRandomDelay(minDelay, maxDelay, func() {})
}

func executeWithRandomDelay(minDelay, maxDelay time.Duration, fn func()) {
	if maxDelay <= 0 {
		maxDelay = minDelay
	}

	// Generate a random duration between minDelay and maxDelay
	randDelay := minDelay
	if maxDelay > minDelay {
		randDelay += time.Duration(rand.Int63n(int64(maxDelay - minDelay + 1)))
	}

	// Schedule the function to run after the random delay
	<-time.After(randDelay)
	fn()
}

func executeWithRandomDelay2(
	ctx context.Context,
	minDelay,
	maxDelay time.Duration,
	fn func(),
	miss func()) {
	if maxDelay <= 0 {
		maxDelay = minDelay
	}

	// Generate a random duration between minDelay and maxDelay
	var randDelay time.Duration
	if maxDelay > minDelay {
		randDelay = minDelay + time.Duration(rand.Int63n(int64(maxDelay-minDelay+1)))
	} else {
		randDelay = minDelay
	}

	// Get the target time when the function should be executed
	targetTime := time.Now().Add(randDelay)

	// Create a ticker that checks every 100ms (adjust the interval as needed)
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			if time.Now().After(targetTime) {
				fn()
				return
			}

		case <-ctx.Done():
			return

		default:
			miss()
		}
	}

}

func KeysSlice[K comparable, T any](m map[K]T) []K {
	slice := make([]K, 0, len(m))
	for k, _ := range m {
		slice = append(slice, k)
	}
	return slice
}

func ValuesSlice[K comparable, T any](m map[K]T) []T {
	slice := make([]T, 0, len(m))
	for _, v := range m {
		slice = append(slice, v)
	}
	return slice
}

func IsSubset[K comparable, T1, T2 any](subset map[K]T1, superset map[K]T2) bool {
	if len(subset) > len(superset) {
		return false
	}
	for key := range subset {
		if _, exists := superset[key]; !exists {
			return false
		}
	}
	return true
}

func IsEquals[K comparable, T any](set1, set2 map[K]T) bool {
	if IsSubset(set1, set2) && IsSubset(set2, set1) {
		return true
	}
	return false
}

func Join[K comparable, T any](set1, set2 map[K]T) map[K]T {
	var first, second map[K]T
	if len(set1) <= len(set2) {
		first = set1
		second = set2
	} else {
		first = set2
		second = set1
	}

	for k, v := range first {
		second[k] = v
	}

	return second
}

func Intersection[K comparable, T any](set1, set2 map[K]T) map[K]T {
	var first, second map[K]T
	if len(set1) <= len(set2) {
		first = set1
		second = set2
	} else {
		first = set2
		second = set1
	}

	result := make(map[K]T)
	for key := range first {
		if v, ok := second[key]; ok {
			result[key] = v
		}
	}
	return result
}

func Difference[K comparable, T any](set1, set2 map[K]T) map[K]T {
	result := make(map[K]T)
	for key, v := range set1 {
		if _, exists := set2[key]; !exists {
			result[key] = v
		}
	}
	return result
}

func ProcessesIDRange(from, to int) []types.ProcessID {
	result := make([]types.ProcessID, 0, to-from+1)
	for i := from; i <= to; i++ {
		result = append(result, types.ProcessID(i))
	}
	return result
}

func Trigger(ctx context.Context, ch chan struct{}) {
	select {
	case <-ctx.Done():
	case ch <- struct{}{}:
	default:
	}
}

func TriggerSync(ctx context.Context, ch chan struct{}) {
	select {
	case <-ctx.Done():
	case ch <- struct{}{}:
	}
}

func Ptr[T any](v T) *T {
	return &v
}
