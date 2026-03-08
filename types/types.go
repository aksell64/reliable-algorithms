package types

import (
	"encoding/binary"
	"reliable/utils/codec"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

type ProcessID int

func (id ProcessID) String() string {
	return strconv.Itoa(int(id))
}

func (id ProcessID) Bytes() []byte {
	return []byte(id.String())
}

func IDFromBytes(byt []byte) (ProcessID, error) {
	pid, err := strconv.Atoi(string(byt))
	if err != nil {
		return ProcessID(0), err
	}
	return ProcessID(pid), nil
}

type ProcessRank int

func (r ProcessRank) String() string {
	return strconv.Itoa(int(r))
}

func (r ProcessRank) Int() int {
	return int(r)
}

type Value interface {
	Type() string
	Compare(other Value) bool
	Less(other Value) bool
	String() string
	Bytes() ([]byte, error)
	Copy() Value
}

func IntValue(i int) Value {
	v := intValue(i)
	return v
}

type intValue int64

func (v intValue) Type() string {
	return "intValue"
}

func (v intValue) String() string {
	return strconv.Itoa(int(v))
}

func (v intValue) Copy() Value {
	vv := v
	return &vv
}

func (v intValue) Compare(other Value) bool {
	ov, ok := other.(intValue)
	if !ok {
		return false
	}
	return v == ov
}

func (v intValue) Less(other Value) bool {
	ov, ok := other.(intValue)
	if !ok {
		return false
	}

	return v < ov
}

func (v intValue) Bytes() ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b, nil
}

func RegisterIntValue(r *codec.Registry) {
	codec.RegisterTyped[intValue](r)
}

type Message interface {
	ID() uuid.UUID
	Name() string
	From() ProcessID
	Type() string
}

type Crasher interface {
	OnCrash(id ProcessID)
	ProcessID() ProcessID
}

type EventuallyCrasher interface {
	OnSuspectCrashed(id ProcessID)
	OnRestoreCrashed(id ProcessID)
	ID() ProcessID
}

type Layer interface {
	Deliverer
	Worker
}

type Worker interface {
	Init()
	Start()
	Stop()
}

func InitWorkers(workers ...Worker) {
	for _, w := range workers {
		w.Init()
	}
}

func StartWorkers(workers ...Worker) {
	for _, w := range workers {
		w.Start()
	}
}

func StopWorkers(workers ...Worker) {
	for _, w := range workers {
		w.Stop()
	}
}

type WorkerOnce struct {
	initOnce  sync.Once
	startOnce sync.Once
	stopOnce  sync.Once
}

func NewWorkerOnce() *WorkerOnce {
	return &WorkerOnce{}
}

func (w *WorkerOnce) Init(f func()) {
	w.initOnce.Do(f)
}

func (w *WorkerOnce) Start(f func()) {
	w.startOnce.Do(f)
}

func (w *WorkerOnce) Stop(f func()) {
	w.stopOnce.Do(f)
}

type Identify interface {
	Sign(raw []byte) ([]byte, error)
	Verify(from ProcessID, raw []byte, signature []byte) error
}
