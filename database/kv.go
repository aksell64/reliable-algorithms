package database

import "reliable/database/inmemory"

type KVStore interface {
	Runner
	Begin() Transaction
}

type Runner interface {
	Get(key string) (value []byte, exists bool)
	Set(key string, value []byte)
	Remove(key string)
}

type Transaction interface {
	Runner

	Commit()
	Rollback()
}

type inmem struct {
	*inmemory.KVStore
}

func NewInMemory() KVStore {
	return &inmem{
		KVStore: inmemory.NewKVStore(),
	}
}

func (kv *inmem) Begin() Transaction {
	return kv.KVStore.Begin()
}
