package inmemory

import (
	"sync"
)

// -------------------- KVStore --------------------

type KVStore struct {
	store map[string][]byte
	mu    sync.RWMutex

	// Per-key lock manager
	keyLocks   map[string]*keyLock
	keyLocksMu sync.Mutex
}

// keyLock — мьютекс для конкретного ключа + счётчик ожидающих,
// чтобы можно было безопасно чистить неиспользуемые записи.
type keyLock struct {
	mu      sync.Mutex
	waiters int // сколько транзакций держат или ждут этот лок
}

func NewKVStore() *KVStore {
	return &KVStore{
		store:    make(map[string][]byte),
		keyLocks: make(map[string]*keyLock),
	}
}

// ---------- Прямые операции (вне транзакций) ----------

func (kv *KVStore) Set(k string, v []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store[k] = v
}

func (kv *KVStore) Get(k string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, exists := kv.store[k]
	return v, exists
}

func (kv *KVStore) Remove(k string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.store, k)
}

// ---------- Управление per-key локами ----------

// acquireKeyLock — захватывает мьютекс для ключа.
// Если другая транзакция уже держит этот ключ — горутина блокируется тут.
func (kv *KVStore) acquireKeyLock(key string) {
	kv.keyLocksMu.Lock()
	kl, ok := kv.keyLocks[key]
	if !ok {
		kl = &keyLock{}
		kv.keyLocks[key] = kl
	}
	kl.waiters++
	kv.keyLocksMu.Unlock()

	// Блокируемся здесь, если ключ занят другой транзакцией
	kl.mu.Lock()
}

// releaseKeyLock — отпускает мьютекс для ключа.
// Если никто больше не ждёт — удаляет запись из карты (не копим мусор).
func (kv *KVStore) releaseKeyLock(key string) {
	kv.keyLocksMu.Lock()
	defer kv.keyLocksMu.Unlock()

	kl, ok := kv.keyLocks[key]
	if !ok {
		return
	}
	kl.waiters--
	kl.mu.Unlock()

	if kl.waiters == 0 {
		delete(kv.keyLocks, key)
	}
}

// ---------- Транзакции ----------

func (kv *KVStore) Begin() *Transaction {
	return &Transaction{
		kv:       kv,
		pending:  make(map[string]entry),
		acquired: make(map[string]struct{}),
	}
}

type entry struct {
	value   []byte
	deleted bool // true = ключ помечен на удаление
}

type Transaction struct {
	kv       *KVStore
	pending  map[string]entry
	acquired map[string]struct{} // ключи, на которые мы взяли лок
	done     bool
	mu       sync.Mutex
}

// ensureLocked — гарантирует, что транзакция держит лок на данный ключ.
// Вызывается перед любой операцией с ключом. Идемпотентна.
func (tx *Transaction) ensureLocked(key string) {
	if _, held := tx.acquired[key]; held {
		return // уже наш
	}

	// ВАЖНО: отпускаем tx.mu перед захватом key-lock,
	// иначе можно получить deadlock (tx.mu → keyLock vs keyLock → tx.mu).
	tx.mu.Unlock()
	tx.kv.acquireKeyLock(key)
	tx.mu.Lock()

	// Проверяем ещё раз после повторного захвата tx.mu
	if tx.done {
		// Транзакцию успели завершить, пока мы ждали — отпускаем лок
		tx.kv.releaseKeyLock(key)
		return
	}
	tx.acquired[key] = struct{}{}
}

// releaseAllLocks — отпускает все захваченные ключевые локи.
func (tx *Transaction) releaseAllLocks() {
	for key := range tx.acquired {
		tx.kv.releaseKeyLock(key)
	}
	tx.acquired = nil
}

// ---------- Runner ----------

func (tx *Transaction) Set(k string, v []byte) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		panic("transaction already committed or rolled back")
	}

	tx.ensureLocked(k)
	tx.pending[k] = entry{value: v, deleted: false}
}

func (tx *Transaction) Get(k string) ([]byte, bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		panic("transaction already committed or rolled back")
	}

	tx.ensureLocked(k)

	// Сначала песочница
	if e, ok := tx.pending[k]; ok {
		if e.deleted {
			return nil, false
		}
		return e.value, true
	}

	// Иначе — основной store (лок на ключ уже наш, никто не изменит)
	tx.kv.mu.RLock()
	v, exists := tx.kv.store[k]
	tx.kv.mu.RUnlock()
	return v, exists
}

func (tx *Transaction) Remove(k string) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		panic("transaction already committed or rolled back")
	}

	tx.ensureLocked(k)
	tx.pending[k] = entry{value: nil, deleted: true}
}

// ---------- Commit / Rollback ----------

func (tx *Transaction) Commit() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return
	}
	tx.done = true

	// Атомарно сливаем pending → store
	tx.kv.mu.Lock()
	for k, e := range tx.pending {
		if e.deleted {
			delete(tx.kv.store, k)
		} else {
			tx.kv.store[k] = e.value
		}
	}
	tx.kv.mu.Unlock()

	tx.pending = nil
	tx.releaseAllLocks() // Отпускаем все per-key локи
}

func (tx *Transaction) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return
	}
	tx.done = true

	tx.pending = nil
	tx.releaseAllLocks() // Отпускаем все per-key локи
}
