package inbox

import (
	"fmt"
	"reliable/database"
	"reliable/utils/codec"
)

const (
	inboxKVKey = "inboxed"
	rawName    = "raw"
)

type Key interface {
	fmt.Stringer
}

type Value interface {
	Type() string
}

type StringKey struct {
	str string
}

func String(str string) StringKey {
	return StringKey{str: str}
}

func (k StringKey) String() string {
	return k.str
}

type Inbox struct {
	storage  database.KVStore
	registry *codec.Registry
}

func New(registry *codec.Registry, storage database.KVStore) *Inbox {
	codec.Register[Raw](registry, rawName)
	return &Inbox{storage: storage, registry: registry}
}

func (i *Inbox) Store(key Key, values ...Value) error {
	if len(values) == 0 {
		return nil
	}

	rawKey := i.genKey(key)

	tx := i.storage.Begin()
	defer tx.Rollback()

	currentValues, err := i.getByKey(tx, rawKey)
	if err != nil {
		return fmt.Errorf("get current key: %w", err)
	}

	currentValues = append(currentValues, values...)
	raw, err := i.encodeValues(currentValues)
	if err != nil {
		return fmt.Errorf("encode values: %w", err)
	}

	tx.Set(rawKey, raw)
	tx.Commit()

	return nil
}

func (i *Inbox) Get(key Key) ([]Value, error) {
	rawKey := i.genKey(key)
	return i.getByKey(i.storage, rawKey)
}

func (i *Inbox) GetAndClear(key Key) ([]Value, error) {
	tx := i.storage.Begin()
	defer tx.Rollback()
	rawKey := i.genKey(key)
	values, err := i.getByKey(tx, rawKey)
	if err != nil {
		return nil, err
	}
	tx.Remove(rawKey)
	tx.Commit()
	return values, nil
}

func (i *Inbox) getByKey(runner database.Runner, key string) ([]Value, error) {
	raw, exists := runner.Get(key)
	if !exists {
		return []Value{}, nil
	}

	vals, err := i.decodeValues(raw)
	if err != nil {
		return []Value{}, err
	}

	return vals, nil
}

func (i *Inbox) genKey(key Key) string {
	keyStr := key.String()
	if keyStr == "" {
		keyStr = "unresolved"
	}

	return fmt.Sprintf("%s:%s", inboxKVKey, keyStr)
}

type Envelope struct {
	TypeName string
	Payload  []byte
}

type Raw struct {
	Envelopes []Envelope
}

func (i *Inbox) encodeValues(values []Value) ([]byte, error) {
	raw := Raw{Envelopes: make([]Envelope, 0, len(values))}
	for _, v := range values {
		data, err := i.registry.Marshal(v)
		if err != nil {
			return nil, err
		}
		raw.Envelopes = append(raw.Envelopes, Envelope{Payload: data, TypeName: v.Type()})
	}

	return i.registry.Marshal(raw)
}

func (i *Inbox) decodeValues(data []byte) ([]Value, error) {
	obj, err := i.registry.Unmarshal(data, rawName)
	if err != nil {
		return nil, err
	}
	raw, ok := obj.(Raw)
	if !ok {
		return nil, fmt.Errorf("expected *Raw, got %T", obj)
	}

	values := make([]Value, 0, len(raw.Envelopes))
	for _, env := range raw.Envelopes {
		obj, err := i.registry.Unmarshal(env.Payload, env.TypeName)
		if err != nil {
			return nil, fmt.Errorf("unmarshal payload: %w", err)
		}
		val, ok := obj.(Value)
		if !ok {
			return nil, fmt.Errorf("expected Value, got %T", obj)
		}
		values = append(values, val)
	}

	return values, nil
}
