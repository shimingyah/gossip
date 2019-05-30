package main

import (
	"encoding/json"
	"sync"

	"github.com/shimingyah/gossip"
)

// Broadcast immpl boradcast interface.
type Broadcast struct {
	msg    []byte
	notify chan struct{}
}

// Invalidates see boradcast interface.
func (b *Broadcast) Invalidates(other gossip.Broadcast) bool {
	return false
}

// Message see boradcast interface.
func (b *Broadcast) Message() []byte {
	return b.msg
}

// Finished see boradcast interface.
func (b *Broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

// EventType the type of event
type EventType byte

// The list of available event types.
const (
	SetEvent EventType = iota
	GetEvent
	DelEvent
)

// Event event
type Event struct {
	EventType EventType
	EventData map[string]string
}

// Serialize event
func (e *Event) Serialize() ([]byte, error) {
	return json.Marshal(*e)
}

// Unserialize event
func (e *Event) Unserialize(b []byte) error {
	return json.Unmarshal(b, e)
}

// Store the interface for operation kv
type Store interface {
	// open api users can use directly
	Has(key string) bool
	Get(key string) string
	Set(key, value string) error
	Del(key string) error

	// internal api is used to Delegate
	Update(b []byte) error
	Items(overhead, limit int) [][]byte
	Serialize() ([]byte, error)
	Unserialize(b []byte) error
}

// KVStore the mem impl for Store interface
type KVStore struct {
	items      map[string]string
	broadcasts *gossip.TransmitLimitedQueue
	sync.RWMutex
}

// NewKVStore return kv store
func NewKVStore() *KVStore {
	return &KVStore{
		items: make(map[string]string),
		broadcasts: &gossip.TransmitLimitedQueue{
			NumNodes:       func() int { return 10 },
			RetransmitMult: 3,
		},
	}
}

// Has see Store interface
func (kvs *KVStore) Has(key string) bool {
	kvs.RLock()
	defer kvs.RUnlock()

	_, has := kvs.items[key]
	return has
}

// Get see Store interface
func (kvs *KVStore) Get(key string) string {
	kvs.RLock()
	defer kvs.RUnlock()

	return kvs.items[key]
}

// Set see Store interface
func (kvs *KVStore) Set(key, value string) error {
	kvs.Lock()
	kvs.items[key] = value
	kvs.Unlock()

	e := &Event{
		EventType: SetEvent,
		EventData: map[string]string{
			key: value,
		},
	}
	data, err := e.Serialize()
	if err != nil {
		return err
	}

	kvs.broadcasts.QueueBroadcast(&Broadcast{msg: data})
	return nil
}

// Del see Store interface
func (kvs *KVStore) Del(key string) error {
	kvs.Lock()
	delete(kvs.items, key)
	kvs.Unlock()

	e := &Event{
		EventType: DelEvent,
		EventData: map[string]string{
			key: "",
		},
	}
	data, err := e.Serialize()
	if err != nil {
		return err
	}

	kvs.broadcasts.QueueBroadcast(&Broadcast{msg: data})
	return nil
}

// Update see Store interface
func (kvs *KVStore) Update(b []byte) error {
	e := &Event{}
	if err := e.Unserialize(b); err != nil {
		return err
	}

	kvs.Lock()
	defer kvs.Unlock()

	for k, v := range e.EventData {
		switch e.EventType {
		case SetEvent:
			kvs.items[k] = v
		case DelEvent:
			delete(kvs.items, k)
		}
	}

	return nil
}

// Items see Store interface
func (kvs *KVStore) Items(overhead, limit int) [][]byte {
	return kvs.broadcasts.GetBroadcasts(overhead, limit)
}

// Serialize see Store interface
func (kvs *KVStore) Serialize() ([]byte, error) {
	kvs.RLock()
	m := kvs.items
	kvs.RUnlock()

	return json.Marshal(m)
}

// Unserialize see Store interface
func (kvs *KVStore) Unserialize(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	kvs.Lock()
	for k, v := range m {
		kvs.items[k] = v
	}
	kvs.Unlock()

	return nil
}

// Delegate impl gossip Delegate interface
type Delegate struct {
	meta  []byte
	store Store
}

// NewDelegate return delegate with meta
func NewDelegate(meta []byte, store Store) *Delegate {
	return &Delegate{meta: meta, store: store}
}

// NodeMeta see gossip Delegate interface
func (d *Delegate) NodeMeta(limit int) []byte {
	if d.meta == nil {
		return []byte{}
	}
	if limit < 0 || limit > len(d.meta) {
		return d.meta
	}
	return d.meta[:limit]
}

// NotifyMsg see gossip Delegate interface
func (d *Delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}
	d.store.Update(b)
}

// GetBroadcasts see Delegate interface
func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.store.Items(overhead, limit)
}

// LocalState see Delegate interface
func (d *Delegate) LocalState(join bool) []byte {
	data, err := d.store.Serialize()
	if err != nil {
		return []byte{}
	}
	return data
}

// MergeRemoteState see Delegate interface
func (d *Delegate) MergeRemoteState(buf []byte, join bool) {
	if !join {
		return
	}
	d.store.Unserialize(buf)
}
