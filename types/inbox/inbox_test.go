package inbox

import (
	"fmt"
	"reliable/database"
	"sync"
	"testing"

	"reliable/utils/codec"
)

// ---------------------------------------------------------------------------
// Test helpers: Key & Value implementations
// ---------------------------------------------------------------------------

// testKey implements Key via fmt.Stringer.
type testKey struct {
	id string
}

func (k testKey) String() string { return k.id }

// emptyKey returns "" from String() — used to verify the "unresolved" fallback.
type emptyKey struct{}

func (k emptyKey) String() string { return "" }

// testValue — a simple Value type we register in the codec registry.
type testValue struct {
	Data string `json:"data"`
}

func (v testValue) Type() string { return "test_value" }

// anotherValue — a second Value type for heterogeneous-list tests.
type anotherValue struct {
	Number int `json:"number"`
}

func (v anotherValue) Type() string { return "another_value" }

// ---------------------------------------------------------------------------
// Factory: fresh Inbox for every test (no shared state).
// ---------------------------------------------------------------------------

func newTestInbox() *Inbox {
	reg := codec.New()
	codec.Register[testValue](reg, "test_value")
	codec.Register[anotherValue](reg, "another_value")
	store := database.NewInMemory()
	return New(reg, store)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestStore_SingleValue — store one value, read it back.
func TestStore_SingleValue(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "player:1"}

	err := ib.Store(key, testValue{Data: "hello"})
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(vals))
	}

	tv, ok := vals[0].(testValue)
	if !ok {
		t.Fatalf("expected testValue, got %T", vals[0])
	}
	if tv.Data != "hello" {
		t.Fatalf("expected Data=hello, got %s", tv.Data)
	}
}

// TestStore_MultipleValues — store several values at once in a single call.
func TestStore_MultipleValues(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "player:2"}

	err := ib.Store(key,
		testValue{Data: "a"},
		testValue{Data: "b"},
		testValue{Data: "c"},
	)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
}

// TestStore_AppendsToExisting — two consecutive Store calls should accumulate values.
func TestStore_AppendsToExisting(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "player:3"}

	_ = ib.Store(key, testValue{Data: "first"})
	_ = ib.Store(key, testValue{Data: "second"})

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}

	if vals[0].(testValue).Data != "first" {
		t.Fatalf("wrong order: first element = %v", vals[0])
	}
	if vals[1].(testValue).Data != "second" {
		t.Fatalf("wrong order: second element = %v", vals[1])
	}
}

// TestStore_EmptyValues — calling Store with zero values is a no-op.
func TestStore_EmptyValues(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "player:4"}

	err := ib.Store(key)
	if err != nil {
		t.Fatalf("Store with no values should not fail: %v", err)
	}

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) != 0 {
		t.Fatalf("expected 0 values, got %d", len(vals))
	}
}

// TestStore_HeterogeneousValues — store different Value types under one key.
func TestStore_HeterogeneousValues(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "player:5"}

	err := ib.Store(key,
		testValue{Data: "txt"},
		anotherValue{Number: 42},
	)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}

	if _, ok := vals[0].(testValue); !ok {
		t.Fatalf("vals[0]: expected testValue, got %T", vals[0])
	}
	if v, ok := vals[1].(anotherValue); !ok || v.Number != 42 {
		t.Fatalf("vals[1]: expected anotherValue{42}, got %T %v", vals[1], vals[1])
	}
}

// TestGet_NonExistentKey — Get on a key that was never written returns empty slice, no error.
func TestGet_NonExistentKey(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "does_not_exist"}

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get should not error on missing key: %v", err)
	}
	if len(vals) != 0 {
		t.Fatalf("expected empty slice, got %d items", len(vals))
	}
}

// TestGetAndClear — GetAndClear returns values and removes them atomically.
func TestGetAndClear(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "player:6"}

	_ = ib.Store(key, testValue{Data: "ephemeral"})

	vals, err := ib.GetAndClear(key)
	if err != nil {
		t.Fatalf("GetAndClear failed: %v", err)
	}
	if len(vals) != 1 || vals[0].(testValue).Data != "ephemeral" {
		t.Fatalf("unexpected values: %v", vals)
	}

	// After clear the key must be empty.
	vals, err = ib.Get(key)
	if err != nil {
		t.Fatalf("Get after clear failed: %v", err)
	}
	if len(vals) != 0 {
		t.Fatalf("expected 0 values after clear, got %d", len(vals))
	}
}

// TestGetAndClear_NonExistentKey — clearing a key that doesn't exist is safe.
func TestGetAndClear_NonExistentKey(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "ghost"}

	vals, err := ib.GetAndClear(key)
	if err != nil {
		t.Fatalf("GetAndClear on missing key should not error: %v", err)
	}
	if len(vals) != 0 {
		t.Fatalf("expected empty slice, got %d", len(vals))
	}
}

// TestEmptyKey_FallbackToUnresolved — key with empty String() gets the "unresolved" prefix.
func TestEmptyKey_FallbackToUnresolved(t *testing.T) {
	ib := newTestInbox()

	err := ib.Store(emptyKey{}, testValue{Data: "lost"})
	if err != nil {
		t.Fatalf("Store with empty key failed: %v", err)
	}

	vals, err := ib.Get(emptyKey{})
	if err != nil {
		t.Fatalf("Get with empty key failed: %v", err)
	}
	if len(vals) != 1 || vals[0].(testValue).Data != "lost" {
		t.Fatalf("unexpected values for empty key: %v", vals)
	}
}

// TestIsolation_DifferentKeys — values under different keys don't interfere.
func TestIsolation_DifferentKeys(t *testing.T) {
	ib := newTestInbox()
	k1 := testKey{id: "alpha"}
	k2 := testKey{id: "beta"}

	_ = ib.Store(k1, testValue{Data: "one"})
	_ = ib.Store(k2, testValue{Data: "two"})

	v1, _ := ib.Get(k1)
	v2, _ := ib.Get(k2)

	if len(v1) != 1 || v1[0].(testValue).Data != "one" {
		t.Fatalf("key alpha polluted: %v", v1)
	}
	if len(v2) != 1 || v2[0].(testValue).Data != "two" {
		t.Fatalf("key beta polluted: %v", v2)
	}
}

// TestStoreAndClear_ThenStoreAgain — store → clear → store must work cleanly.
func TestStoreAndClear_ThenStoreAgain(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "cycle"}

	_ = ib.Store(key, testValue{Data: "round1"})

	_, _ = ib.GetAndClear(key)

	_ = ib.Store(key, testValue{Data: "round2"})

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) != 1 || vals[0].(testValue).Data != "round2" {
		t.Fatalf("expected round2, got %v", vals)
	}
}

// TestGetAndClear_DoesNotAffectOtherKeys — clearing one key leaves others intact.
func TestGetAndClear_DoesNotAffectOtherKeys(t *testing.T) {
	ib := newTestInbox()
	k1 := testKey{id: "keep"}
	k2 := testKey{id: "drop"}

	_ = ib.Store(k1, testValue{Data: "stay"})
	_ = ib.Store(k2, testValue{Data: "bye"})

	_, _ = ib.GetAndClear(k2)

	vals, _ := ib.Get(k1)
	if len(vals) != 1 || vals[0].(testValue).Data != "stay" {
		t.Fatalf("key 'keep' was affected by clearing 'drop': %v", vals)
	}
}

// TestConcurrent_StoreAndGet — hammer the same key from many goroutines.
// No panics, no lost writes, no data races (run with -race).
func TestConcurrent_StoreAndGet(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "contended"}
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			err := ib.Store(key, testValue{Data: fmt.Sprintf("g%d", n)})
			if err != nil {
				t.Errorf("Store from goroutine %d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	vals, err := ib.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Every goroutine appends exactly 1 value, so we must have exactly `goroutines` entries.
	if len(vals) != goroutines {
		t.Fatalf("expected %d values, got %d", goroutines, len(vals))
	}
}

// TestConcurrent_StoreAndGetAndClear — concurrent Store + GetAndClear must not panic or corrupt.
func TestConcurrent_StoreAndGetAndClear(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "chaos"}
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = ib.Store(key, testValue{Data: fmt.Sprintf("v%d", i)})
		}
	}()

	// Reader+clearer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = ib.GetAndClear(key)
		}
	}()

	wg.Wait()

	// After everything settles, we should be able to read without error.
	_, err := ib.Get(key)
	if err != nil {
		t.Fatalf("final Get failed: %v", err)
	}
}

// TestConcurrent_DifferentKeys — concurrent writes to different keys, no cross-contamination.
func TestConcurrent_DifferentKeys(t *testing.T) {
	ib := newTestInbox()
	const keys = 20
	const writesPerKey = 10

	var wg sync.WaitGroup
	wg.Add(keys)

	for k := 0; k < keys; k++ {
		go func(kid int) {
			defer wg.Done()
			key := testKey{id: fmt.Sprintf("key:%d", kid)}
			for w := 0; w < writesPerKey; w++ {
				_ = ib.Store(key, testValue{Data: fmt.Sprintf("%d-%d", kid, w)})
			}
		}(k)
	}
	wg.Wait()

	for k := 0; k < keys; k++ {
		key := testKey{id: fmt.Sprintf("key:%d", k)}
		vals, err := ib.Get(key)
		if err != nil {
			t.Fatalf("Get key:%d failed: %v", k, err)
		}
		if len(vals) != writesPerKey {
			t.Fatalf("key:%d expected %d values, got %d", k, writesPerKey, len(vals))
		}
	}
}

// TestMultipleGetAndClear_Idempotent — second GetAndClear on the same key returns nothing.
func TestMultipleGetAndClear_Idempotent(t *testing.T) {
	ib := newTestInbox()
	key := testKey{id: "once"}

	_ = ib.Store(key, testValue{Data: "payload"})

	first, _ := ib.GetAndClear(key)
	second, _ := ib.GetAndClear(key)

	if len(first) != 1 {
		t.Fatalf("first GetAndClear: expected 1, got %d", len(first))
	}
	if len(second) != 0 {
		t.Fatalf("second GetAndClear: expected 0, got %d", len(second))
	}
}
