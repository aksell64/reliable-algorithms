package inmemory

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =====================================================================
// Direct operations (no transactions)
// =====================================================================

func TestKVStore_SetAndGet(t *testing.T) {
	// Basic set followed by get should return the stored value.
	kv := NewKVStore()
	kv.Set("key1", []byte("value1"))

	v, ok := kv.Get("key1")
	require.True(t, ok)
	assert.Equal(t, []byte("value1"), v)
}

func TestKVStore_GetNonExistent(t *testing.T) {
	// Reading a key that was never written must return (nil, false).
	kv := NewKVStore()

	v, ok := kv.Get("ghost")
	assert.False(t, ok)
	assert.Nil(t, v)
}

func TestKVStore_Overwrite(t *testing.T) {
	// A second Set on the same key must overwrite the previous value.
	kv := NewKVStore()
	kv.Set("k", []byte("old"))
	kv.Set("k", []byte("new"))

	v, ok := kv.Get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("new"), v)
}

func TestKVStore_Remove(t *testing.T) {
	// Remove should delete the key; subsequent Get returns false.
	kv := NewKVStore()
	kv.Set("k", []byte("v"))
	kv.Remove("k")

	_, ok := kv.Get("k")
	assert.False(t, ok)
}

func TestKVStore_RemoveNonExistent(t *testing.T) {
	// Removing a key that doesn't exist should not panic.
	kv := NewKVStore()
	assert.NotPanics(t, func() { kv.Remove("nope") })
}

func TestKVStore_EmptyKey(t *testing.T) {
	// Empty string is a valid map key in Go; make sure it works.
	kv := NewKVStore()
	kv.Set("", []byte("empty-key-value"))

	v, ok := kv.Get("")
	require.True(t, ok)
	assert.Equal(t, []byte("empty-key-value"), v)
}

func TestKVStore_NilValue(t *testing.T) {
	// Storing nil as a value should be distinguishable from "key not found".
	kv := NewKVStore()
	kv.Set("k", nil)

	v, ok := kv.Get("k")
	assert.True(t, ok) // key exists
	assert.Nil(t, v)   // but value is nil
}

func TestKVStore_LargeValue(t *testing.T) {
	// Sanity check: store a large blob and read it back.
	kv := NewKVStore()
	big := make([]byte, 1<<20) // 1 MiB
	for i := range big {
		big[i] = byte(i % 256)
	}
	kv.Set("big", big)

	v, ok := kv.Get("big")
	require.True(t, ok)
	assert.Equal(t, big, v)
}

// =====================================================================
// Transaction — basic commit
// =====================================================================

func TestTx_CommitSetGet(t *testing.T) {
	// Values set inside a transaction should be visible after Commit.
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Set("a", []byte("1"))
	tx.Commit()

	v, ok := kv.Get("a")
	require.True(t, ok)
	assert.Equal(t, []byte("1"), v)
}

func TestTx_CommitOverwritesExisting(t *testing.T) {
	// Transaction commit should overwrite a pre-existing value.
	kv := NewKVStore()
	kv.Set("a", []byte("before"))

	tx := kv.Begin()
	tx.Set("a", []byte("after"))
	tx.Commit()

	v, _ := kv.Get("a")
	assert.Equal(t, []byte("after"), v)
}

func TestTx_CommitRemove(t *testing.T) {
	// Remove inside a committed transaction should delete from main store.
	kv := NewKVStore()
	kv.Set("a", []byte("val"))

	tx := kv.Begin()
	tx.Remove("a")
	tx.Commit()

	_, ok := kv.Get("a")
	assert.False(t, ok)
}

func TestTx_CommitMultipleKeys(t *testing.T) {
	// Several keys modified in one transaction should all land atomically.
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("x", []byte("10"))
	tx.Set("y", []byte("20"))
	tx.Set("z", []byte("30"))
	tx.Commit()

	for _, tc := range []struct{ k, v string }{{"x", "10"}, {"y", "20"}, {"z", "30"}} {
		v, ok := kv.Get(tc.k)
		require.True(t, ok, "key %s must exist", tc.k)
		assert.Equal(t, []byte(tc.v), v)
	}
}

// =====================================================================
// Transaction — rollback
// =====================================================================

func TestTx_RollbackDiscardsChanges(t *testing.T) {
	// Rollback must leave the main store untouched.
	kv := NewKVStore()
	kv.Set("a", []byte("original"))

	tx := kv.Begin()
	tx.Set("a", []byte("modified"))
	tx.Set("b", []byte("new"))
	tx.Rollback()

	v, ok := kv.Get("a")
	require.True(t, ok)
	assert.Equal(t, []byte("original"), v)

	_, ok = kv.Get("b")
	assert.False(t, ok, "rolled-back key must not appear in store")
}

func TestTx_RollbackRemoveNoEffect(t *testing.T) {
	// Remove inside a rolled-back transaction must not delete from store.
	kv := NewKVStore()
	kv.Set("keep", []byte("me"))

	tx := kv.Begin()
	tx.Remove("keep")
	tx.Rollback()

	v, ok := kv.Get("keep")
	require.True(t, ok)
	assert.Equal(t, []byte("me"), v)
}

// =====================================================================
// Transaction — sandbox isolation (reads inside tx)
// =====================================================================

func TestTx_ReadOwnWrites(t *testing.T) {
	// A transaction should see its own uncommitted writes.
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("k", []byte("txval"))

	v, ok := tx.Get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("txval"), v)
	tx.Rollback()
}

func TestTx_ReadAfterRemoveInsideTx(t *testing.T) {
	// After Remove inside tx, Get should return (nil, false) within the same tx.
	kv := NewKVStore()
	kv.Set("k", []byte("exists"))

	tx := kv.Begin()
	tx.Remove("k")

	v, ok := tx.Get("k")
	assert.False(t, ok)
	assert.Nil(t, v)
	tx.Rollback()
}

func TestTx_ReadFallsBackToStore(t *testing.T) {
	// If tx hasn't touched a key, Get should read from the main store.
	kv := NewKVStore()
	kv.Set("pre", []byte("existing"))

	tx := kv.Begin()
	v, ok := tx.Get("pre")
	require.True(t, ok)
	assert.Equal(t, []byte("existing"), v)
	tx.Commit()
}

func TestTx_SetThenOverwriteInsideTx(t *testing.T) {
	// Multiple writes to the same key inside one tx — last write wins.
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("k", []byte("first"))
	tx.Set("k", []byte("second"))

	v, ok := tx.Get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("second"), v)
	tx.Commit()

	v, _ = kv.Get("k")
	assert.Equal(t, []byte("second"), v)
}

func TestTx_RemoveThenSetInsideTx(t *testing.T) {
	// Remove followed by Set inside the same tx — key should reappear.
	kv := NewKVStore()
	kv.Set("k", []byte("v"))

	tx := kv.Begin()
	tx.Remove("k")
	tx.Set("k", []byte("revived"))

	v, ok := tx.Get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("revived"), v)
	tx.Commit()

	v, _ = kv.Get("k")
	assert.Equal(t, []byte("revived"), v)
}

// =====================================================================
// Transaction — double commit / rollback safety
// =====================================================================

func TestTx_DoubleCommitIsNoop(t *testing.T) {
	// Second Commit should silently do nothing (no panic, no double-apply).
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("k", []byte("v"))
	tx.Commit()

	// Mutate store directly after first commit
	kv.Set("k", []byte("overwritten"))

	// Second commit must NOT reapply pending
	tx.Commit()

	v, _ := kv.Get("k")
	assert.Equal(t, []byte("overwritten"), v)
}

func TestTx_DoubleRollbackIsNoop(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Set("k", []byte("v"))

	assert.NotPanics(t, func() {
		tx.Rollback()
		tx.Rollback()
	})
}

func TestTx_CommitThenRollbackIsNoop(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Set("k", []byte("v"))
	tx.Commit()

	assert.NotPanics(t, func() { tx.Rollback() })

	v, ok := kv.Get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v"), v)
}

// =====================================================================
// Transaction — panic on use-after-finish
// =====================================================================

func TestTx_SetAfterCommitPanics(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Commit()

	assert.Panics(t, func() { tx.Set("k", []byte("v")) })
}

func TestTx_GetAfterCommitPanics(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Commit()

	assert.Panics(t, func() { tx.Get("k") })
}

func TestTx_RemoveAfterCommitPanics(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Commit()

	assert.Panics(t, func() { tx.Remove("k") })
}

func TestTx_SetAfterRollbackPanics(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	tx.Rollback()

	assert.Panics(t, func() { tx.Set("k", []byte("v")) })
}

// =====================================================================
// Per-key lock management — cleanup
// =====================================================================

func TestKeyLocks_CleanedUpAfterCommit(t *testing.T) {
	// After commit, the per-key lock map should be empty (no garbage).
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("a", []byte("1"))
	tx.Set("b", []byte("2"))
	tx.Commit()

	kv.keyLocksMu.Lock()
	remaining := len(kv.keyLocks)
	kv.keyLocksMu.Unlock()

	assert.Equal(t, 0, remaining, "all per-key locks should be cleaned up")
}

func TestKeyLocks_CleanedUpAfterRollback(t *testing.T) {
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("a", []byte("1"))
	tx.Rollback()

	kv.keyLocksMu.Lock()
	remaining := len(kv.keyLocks)
	kv.keyLocksMu.Unlock()

	assert.Equal(t, 0, remaining)
}

// =====================================================================
// Concurrency — direct operations
// =====================================================================

func TestKVStore_ConcurrentDirectSetGet(t *testing.T) {
	// Hammer the store with concurrent direct writes and reads; must not race.
	kv := NewKVStore()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := "shared"
			kv.Set(key, []byte{byte(id)})
			kv.Get(key)
		}(i)
	}
	wg.Wait()

	// Just verify no panic / race; the final value is non-deterministic.
	_, ok := kv.Get("shared")
	assert.True(t, ok)
}

func TestKVStore_ConcurrentDirectRemove(t *testing.T) {
	// Concurrent removes should not panic or race.
	kv := NewKVStore()
	kv.Set("target", []byte("bye"))

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			kv.Remove("target")
		}()
	}
	wg.Wait()

	_, ok := kv.Get("target")
	assert.False(t, ok)
}

// =====================================================================
// Concurrency — transactions on the SAME key (serialization)
// =====================================================================

func TestTx_ConcurrentSameKey_Serialized(t *testing.T) {
	// Two transactions touching the same key must execute serially
	// thanks to per-key locking. The final value must be from one of them.
	kv := NewKVStore()
	kv.Set("counter", []byte("0"))

	var wg sync.WaitGroup
	wg.Add(2)

	write := func(val string) {
		defer wg.Done()
		tx := kv.Begin()
		tx.Set("counter", []byte(val))
		tx.Commit()
	}

	go write("A")
	go write("B")
	wg.Wait()

	v, ok := kv.Get("counter")
	require.True(t, ok)
	assert.Contains(t, []string{"A", "B"}, string(v))
}

func TestTx_ConcurrentSameKey_ManyGoroutines(t *testing.T) {
	// Stress test: many goroutines all set the same key inside transactions.
	kv := NewKVStore()
	const n = 50

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			tx := kv.Begin()
			tx.Set("hot", []byte{byte(id)})
			tx.Commit()
		}(i)
	}
	wg.Wait()

	_, ok := kv.Get("hot")
	assert.True(t, ok)
}

// =====================================================================
// Concurrency — transactions on DIFFERENT keys (no contention)
// =====================================================================

func TestTx_ConcurrentDifferentKeys(t *testing.T) {
	// Transactions on independent keys should not block each other.
	kv := NewKVStore()
	const n = 100

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			key := string(rune('A'+id%26)) + string(rune('0'+id))
			tx := kv.Begin()
			tx.Set(key, []byte("val"))
			v, ok := tx.Get(key)
			assert.True(t, ok)
			assert.Equal(t, []byte("val"), v)
			tx.Commit()
		}(i)
	}
	wg.Wait()
}

// =====================================================================
// Concurrency — mixed commit & rollback
// =====================================================================

func TestTx_ConcurrentCommitAndRollback(t *testing.T) {
	// Half of the goroutines commit, half rollback. No panics, no races.
	kv := NewKVStore()
	const n = 60

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			tx := kv.Begin()
			tx.Set("k", []byte{byte(id)})
			if id%2 == 0 {
				tx.Commit()
			} else {
				tx.Rollback()
			}
		}(i)
	}
	wg.Wait()
}

// =====================================================================
// Isolation: uncommitted data must NOT leak
// =====================================================================

func TestTx_UncommittedDataNotVisibleOutside(t *testing.T) {
	// Data written inside an ongoing (uncommitted) transaction must not
	// be visible via direct KVStore.Get.
	kv := NewKVStore()

	tx := kv.Begin()
	tx.Set("secret", []byte("hidden"))

	// Direct read should not see it
	_, ok := kv.Get("secret")
	assert.False(t, ok, "uncommitted tx data must not leak to direct reads")

	tx.Rollback()
}

// =====================================================================
// Sequential transactions on the same store
// =====================================================================

func TestTx_SequentialTransactions(t *testing.T) {
	// Commit tx1 → begin tx2 → tx2 should see tx1's writes.
	kv := NewKVStore()

	tx1 := kv.Begin()
	tx1.Set("step", []byte("one"))
	tx1.Commit()

	tx2 := kv.Begin()
	v, ok := tx2.Get("step")
	require.True(t, ok)
	assert.Equal(t, []byte("one"), v)
	tx2.Set("step", []byte("two"))
	tx2.Commit()

	v, _ = kv.Get("step")
	assert.Equal(t, []byte("two"), v)
}

// =====================================================================
// Transaction with multiple operations forming a lifecycle
// =====================================================================

func TestTx_FullLifecycle(t *testing.T) {
	// Store → begin tx → read inside tx → modify → remove another → commit → verify.
	kv := NewKVStore()
	kv.Set("alive", []byte("yes"))
	kv.Set("doomed", []byte("bye"))

	tx := kv.Begin()

	// Read existing
	v, ok := tx.Get("alive")
	require.True(t, ok)
	assert.Equal(t, []byte("yes"), v)

	// Modify
	tx.Set("alive", []byte("still yes"))

	// Remove
	tx.Remove("doomed")

	// Add new
	tx.Set("fresh", []byte("hello"))

	tx.Commit()

	v, _ = kv.Get("alive")
	assert.Equal(t, []byte("still yes"), v)

	_, ok = kv.Get("doomed")
	assert.False(t, ok)

	v, ok = kv.Get("fresh")
	require.True(t, ok)
	assert.Equal(t, []byte("hello"), v)
}

// =====================================================================
// Edge case: empty transaction commit/rollback
// =====================================================================

func TestTx_EmptyCommit(t *testing.T) {
	// Committing a transaction with no operations must be safe and a no-op.
	kv := NewKVStore()
	kv.Set("untouched", []byte("original"))

	tx := kv.Begin()
	tx.Commit()

	v, ok := kv.Get("untouched")
	require.True(t, ok)
	assert.Equal(t, []byte("original"), v)
}

func TestTx_EmptyRollback(t *testing.T) {
	kv := NewKVStore()
	tx := kv.Begin()
	assert.NotPanics(t, func() { tx.Rollback() })
}
