package database

type KVStore interface {
	Get(key string) (value []byte, exists bool)
	Set(key string, value []byte)
}
