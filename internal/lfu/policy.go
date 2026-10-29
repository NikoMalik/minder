package lfu

import (
	"github.com/NikoMalik/bloom"
	"math"
	"sync"
	"sync/atomic"
)

const (
	// lfuSample is the number of items to sample when looking at eviction candidates.
	lfuSample = 5
)

// Policy is an eviction policy using TinyLFU for admission control
// and sampled LFU for eviction decisions.
type Policy[K comparable, V any] struct {
	sync.Mutex
	admit    *tinyLFU
	evict    *sampledLFU[K, V]
	isClosed bool

	// Async batching for frequency updates
	getBuf   []uint64
	getBufMu sync.Mutex
	itemsCh  chan []uint64
	stop     chan struct{}
	done     chan struct{}
}

func NewPolicy[K comparable, V any](numCounters, maxCost int64) *Policy[K, V] {
	p := &Policy[K, V]{
		admit:   newTinyLFU(numCounters),
		evict:   newSampledLFU[K, V](maxCost),
		getBuf:  make([]uint64, 0, 64),
		itemsCh: make(chan []uint64, 3),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go p.processItems()
	return p
}

type policyPair[K comparable] struct {
	key         uint64
	originalKey K
	cost        int64
}

// Evicted represents an item that was evicted from the cache.
type Evicted[K comparable, V any] struct {
	Key         uint64
	OriginalKey K
	Cost        int64
}

// Add decides whether the item with the given key and cost should be accepted.
// It returns the list of victims that have been evicted, a boolean indicating
// whether the incoming item should be accepted, and a boolean indicating
// whether the item was updated (already existed).
func (p *Policy[K, V]) Add(key uint64, originalKey K, cost int64) ([]*Evicted[K, V], bool, bool) {
	p.Lock()
	defer p.Unlock()

	if p.isClosed {
		return nil, false, false
	}

	if cost > p.evict.getMaxCost() {
		return nil, false, false
	}

	if has := p.evict.updateIfHas(key, originalKey, cost); has {
		return nil, false, true // Item was updated
	}

	room := p.evict.roomLeft(cost)
	if room >= 0 {
		p.evict.add(key, originalKey, cost)
		return nil, true, false
	}

	incHits := p.admit.Estimate(key)
	sample := make([]*policyPair[K], 0, lfuSample)
	victims := make([]*Evicted[K, V], 0)

	for ; room < 0; room = p.evict.roomLeft(cost) {
		sample = p.evict.fillSample(sample)
		minKey, minHits, minId, minCost := uint64(0), int64(math.MaxInt64), 0, int64(0)
		var minOriginalKey K
		for i, pair := range sample {
			if hits := p.admit.Estimate(pair.key); hits < minHits {
				minKey, minHits, minId, minCost = pair.key, hits, i, pair.cost
				minOriginalKey = pair.originalKey
			}
		}

		if incHits < minHits {
			return victims, false, false
		}

		p.evict.del(minKey)
		sample[minId] = sample[len(sample)-1]
		sample = sample[:len(sample)-1]
		victims = append(victims, &Evicted[K, V]{
			Key:         minKey,
			OriginalKey: minOriginalKey,
			Cost:        minCost,
		})
	}

	p.evict.add(key, originalKey, cost)
	return victims, true, false
}

func (p *Policy[K, V]) Has(key uint64) bool {
	p.Lock()
	_, exists := p.evict.keyCosts[key]
	p.Unlock()
	return exists
}

// IncrementAccess increments the access frequency for a key.
// This operation is lock-free as tinyLFU uses atomic operations.
func (p *Policy[K, V]) IncrementAccess(key uint64) {
	p.admit.Increment(key)
}

func (p *Policy[K, V]) Del(key uint64) {
	p.Lock()
	p.evict.del(key)
	p.Unlock()
}

func (p *Policy[K, V]) Cap() int64 {
	p.Lock()
	capacity := p.evict.getMaxCost() - p.evict.used
	p.Unlock()
	return capacity
}

func (p *Policy[K, V]) Update(key uint64, originalKey K, cost int64) {
	p.Lock()
	p.evict.updateIfHas(key, originalKey, cost)
	p.Unlock()
}

func (p *Policy[K, V]) Cost(key uint64) int64 {
	p.Lock()
	if pair, found := p.evict.keyCosts[key]; found {
		p.Unlock()
		return pair.cost
	}
	p.Unlock()
	return -1
}

func (p *Policy[K, V]) Clear() {
	p.Lock()
	p.admit.clear()
	p.evict.clear()
	p.Unlock()
}

func (p *Policy[K, V]) Close() {
	p.Lock()
	if p.isClosed {
		p.Unlock()
		return
	}
	p.isClosed = true
	p.Unlock()

	// Stop the processItems goroutine
	close(p.stop)
	<-p.done
}

// processItems processes batched frequency updates in the background.
func (p *Policy[K, V]) processItems() {
	defer close(p.done)
	for {
		select {
		case <-p.stop:
			return
		case keys := <-p.itemsCh:
			for _, key := range keys {
				p.admit.Increment(key)
			}
		}
	}
}

// Push sends a batch of keys for frequency increment.
// This is the async entry point for recording accesses.
func (p *Policy[K, V]) Push(keys []uint64) {
	select {
	case p.itemsCh <- keys:
	default:
		// Channel full, process synchronously to avoid blocking
		for _, key := range keys {
			p.admit.Increment(key)
		}
	}
}

// RecordAccess buffers an access for async frequency update.
// When the buffer is full, it's sent for processing.
func (p *Policy[K, V]) RecordAccess(key uint64) {
	p.getBufMu.Lock()
	p.getBuf = append(p.getBuf, key)
	if len(p.getBuf) >= cap(p.getBuf) {
		// Buffer full, send for processing
		p.Push(p.getBuf)
		p.getBuf = make([]uint64, 0, 64)
	}
	p.getBufMu.Unlock()
}

// FlushAccessBuffer forces the current access buffer to be processed.
// Useful for testing or when you need immediate frequency updates.
func (p *Policy[K, V]) FlushAccessBuffer() {
	p.getBufMu.Lock()
	if len(p.getBuf) > 0 {
		p.Push(p.getBuf)
		p.getBuf = make([]uint64, 0, 64)
	}
	p.getBufMu.Unlock()
}

func (p *Policy[K, V]) MaxCost() int64 {
	if p == nil || p.evict == nil {
		return 0
	}
	return p.evict.getMaxCost()
}

func (p *Policy[K, V]) UpdateMaxCost(maxCost int64) {
	if p == nil || p.evict == nil {
		return
	}
	p.evict.updateMaxCost(maxCost)
}

// sampledLFU is an eviction helper storing key-cost pairs.
type sampledLFU[K comparable, V any] struct {
	maxCost  int64
	used     int64
	keyCosts map[uint64]policyPair[K]
}

func newSampledLFU[K comparable, V any](maxCost int64) *sampledLFU[K, V] {
	return &sampledLFU[K, V]{
		keyCosts: make(map[uint64]policyPair[K]),
		maxCost:  maxCost,
	}
}

func (p *sampledLFU[K, V]) getMaxCost() int64 {
	return atomic.LoadInt64(&p.maxCost)
}

func (p *sampledLFU[K, V]) updateMaxCost(maxCost int64) {
	atomic.StoreInt64(&p.maxCost, maxCost)
}

func (p *sampledLFU[K, V]) roomLeft(cost int64) int64 {
	return p.getMaxCost() - (p.used + cost)
}

func (p *sampledLFU[K, V]) fillSample(in []*policyPair[K]) []*policyPair[K] {
	if len(in) >= lfuSample {
		return in
	}
	for key, pair := range p.keyCosts {
		in = append(in, &policyPair[K]{key: key, originalKey: pair.originalKey, cost: pair.cost})
		if len(in) >= lfuSample {
			return in
		}
	}
	return in
}

func (p *sampledLFU[K, V]) del(key uint64) {
	pair, ok := p.keyCosts[key]
	if !ok {
		return
	}
	p.used -= pair.cost
	delete(p.keyCosts, key)
}

func (p *sampledLFU[K, V]) add(key uint64, originalKey K, cost int64) {
	p.keyCosts[key] = policyPair[K]{key: key, originalKey: originalKey, cost: cost}
	p.used += cost
}

func (p *sampledLFU[K, V]) updateIfHas(key uint64, originalKey K, cost int64) bool {
	if prev, found := p.keyCosts[key]; found {
		p.used += cost - prev.cost
		p.keyCosts[key] = policyPair[K]{key: key, originalKey: originalKey, cost: cost}
		return true
	}
	return false
}

func (p *sampledLFU[K, V]) clear() {
	p.used = 0
	p.keyCosts = make(map[uint64]policyPair[K])
}

// tinyLFU is an admission helper that keeps track of access frequency using
// tiny (4-bit) counters in the form of a count-min sketch.
// Uses atomic operations for lock-free concurrent access on hot path.
type tinyLFU struct {
	freq      *cmSketch
	door      *bloom.BloomFilter
	incrs     atomic.Int64
	resetAt   int64
	resetting atomic.Bool // prevents concurrent reset
}

func newTinyLFU(numCounters int64) *tinyLFU {
	return &tinyLFU{
		freq:    newCmSketch(numCounters),
		door:    bloom.NewFilter(uint32(min(numCounters, 1<<31))),
		resetAt: numCounters,
	}
}

// Estimate returns the frequency estimate for a key.
// This operation is lock-free.
func (p *tinyLFU) Estimate(key uint64) int64 {
	hits := p.freq.Estimate(key)
	if p.door.Contains(key) {
		hits++
	}
	return hits
}

// Increment increments the frequency counter for a key.
// This operation is mostly lock-free using atomic operations.
func (p *tinyLFU) Increment(key uint64) {
	// Flip doorkeeper bit if not already done.
	if p.door.Contains(key) {
		// Increment count-min counter if doorkeeper bit is already set.
		p.freq.Increment(key)
	} else {
		p.door.Add(key)
	}
	incrs := p.incrs.Add(1)
	if incrs >= p.resetAt {
		p.tryReset()
	}
}

func (p *tinyLFU) tryReset() {
	// Only one goroutine can reset at a time
	if !p.resetting.CompareAndSwap(false, true) {
		return
	}
	defer p.resetting.Store(false)

	// Double-check after acquiring the "lock"
	if p.incrs.Load() < p.resetAt {
		return
	}

	p.incrs.Store(0)
	p.door.Clear()
	p.freq.Reset()
}

func (p *tinyLFU) clear() {
	// Try to acquire reset lock, with limited retries
	for i := 0; i < 1000; i++ {
		if p.resetting.CompareAndSwap(false, true) {
			p.incrs.Store(0)
			p.door.Clear()
			p.freq.Clear()
			p.resetting.Store(false)
			return
		}
	}
	// If we couldn't acquire after retries, just do it anyway
	// This is safe because Clear operations are idempotent
	p.incrs.Store(0)
	p.door.Clear()
	p.freq.Clear()
}
