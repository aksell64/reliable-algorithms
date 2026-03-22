package types

import (
	"sync"

	"github.com/google/uuid"
)

type DelivererOption func(*delivererConfig)

type delivererConfig struct {
	msgNames []string
}

func DelivererWithMsgNames(names ...string) DelivererOption {
	return func(cfg *delivererConfig) {
		cfg.msgNames = append(cfg.msgNames, names...)
	}
}

func buildDelivererConfig(opts []DelivererOption) delivererConfig {
	var cfg delivererConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

type Deliverer interface {
	ID() uuid.UUID
	ProcessID() ProcessID
	Deliver(msg Message)
	AddDeliverer(d Deliverer, opts ...DelivererOption)
	RemoveDeliverer(d Deliverer)
	Instance() string
}

type UnimplementedDeliverer struct {
	id  uuid.UUID
	pid ProcessID
}

func NewUnimplementedDeliverer(pid ProcessID) UnimplementedDeliverer {
	return UnimplementedDeliverer{
		id:  uuid.New(),
		pid: pid,
	}
}

func (ud UnimplementedDeliverer) Deliver(msg Message)                               {}
func (ud UnimplementedDeliverer) AddDeliverer(d Deliverer, opts ...DelivererOption) {}
func (ud UnimplementedDeliverer) RemoveDeliverer(d Deliverer)                       {}
func (ud UnimplementedDeliverer) ID() uuid.UUID                                     { return ud.id }
func (ud UnimplementedDeliverer) ProcessID() ProcessID                              { return ud.pid }
func (ud UnimplementedDeliverer) Instance() string                                  { return "unimplemented" }

type multyDeliverer struct {
	UnimplementedDeliverer
	fallbackDeliverers []Deliverer
	routes             map[string][]Deliverer
	mu                 sync.RWMutex
}

func NewUnaryDeliverer(pid ProcessID) Deliverer {
	return newMultyDeliverer(pid)
}

func newMultyDeliverer(pid ProcessID, d ...Deliverer) *multyDeliverer {
	if len(d) == 0 {
		d = make([]Deliverer, 0)
	}
	md := &multyDeliverer{
		fallbackDeliverers:     d,
		UnimplementedDeliverer: NewUnimplementedDeliverer(pid),
		routes:                 make(map[string][]Deliverer),
	}

	return md
}

func (md *multyDeliverer) Instance() string {
	if md.fallbackDeliverers == nil {
		return "empty"
	}

	instance := ""
	for _, d := range md.fallbackDeliverers {
		instance += d.Instance() + " "
	}
	return instance
}

func (md *multyDeliverer) Deliver(msg Message) {
	deliverers := md.getDeliverers(msg)
	for _, d := range deliverers {
		d.Deliver(msg)
	}
}

func (md *multyDeliverer) getDeliverers(msg Message) []Deliverer {
	deliverers := make([]Deliverer, 0)

	md.mu.RLock()
	defer md.mu.RUnlock()

	if routed, ok := md.routes[msg.Name()]; ok {
		for _, d := range routed {
			deliverers = append(deliverers, d)
		}
		return deliverers
	}

	for _, d := range md.fallbackDeliverers {
		deliverers = append(deliverers, d)
	}
	return deliverers
}

func (md *multyDeliverer) AddDeliverer(d Deliverer, opts ...DelivererOption) {
	cfg := buildDelivererConfig(opts)

	md.mu.Lock()
	defer md.mu.Unlock()

	if len(cfg.msgNames) > 0 {
		for _, name := range cfg.msgNames {
			md.routes[name] = append(md.routes[name], d)
		}
		return
	}

	md.fallbackDeliverers = append(md.fallbackDeliverers, d)
}

func (md *multyDeliverer) RemoveDeliverer(delivererToRemove Deliverer) {
	md.mu.Lock()
	defer md.mu.Unlock()
	md.removeDeliverer(delivererToRemove)
}

func (md *multyDeliverer) removeDeliverer(delivererToRemove Deliverer) bool {
	removed := false

	for i := len(md.fallbackDeliverers) - 1; i >= 0; i-- {
		current := md.fallbackDeliverers[i]
		if nestedMD, ok := current.(*multyDeliverer); ok {
			if nestedMD.removeDeliverer(delivererToRemove) {
				removed = true
				if len(nestedMD.fallbackDeliverers) == 0 && len(nestedMD.routes) == 0 {
					md.fallbackDeliverers = append(md.fallbackDeliverers[:i], md.fallbackDeliverers[i+1:]...)
				} else if len(nestedMD.fallbackDeliverers) == 1 && len(nestedMD.routes) == 0 {
					md.fallbackDeliverers[i] = nestedMD.fallbackDeliverers[0]
				}
			}
			continue
		}
		if current.ID() == delivererToRemove.ID() {
			md.fallbackDeliverers = append(md.fallbackDeliverers[:i], md.fallbackDeliverers[i+1:]...)
			removed = true
		}
	}

	for name, ds := range md.routes {
		for i := len(ds) - 1; i >= 0; i-- {
			if ds[i].ID() == delivererToRemove.ID() {
				md.routes[name] = append(ds[:i], ds[i+1:]...)
				removed = true
			}
		}
		if len(md.routes[name]) == 0 {
			delete(md.routes, name)
		}
	}

	return removed
}
