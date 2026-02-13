package inmemory

import "sync"

type KVStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string][]byte),
	}
}

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
