package minder

import (
	"hash/maphash"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/errgroup"
)

const (
	LFU_SHARD_COUNT = 512
	LFU_TOTAL       = 1024_000
	// Default cost per item when cost is not specified.
	defaultCost = 1
)

// lfuCacheItem stores the value along with its metadata.
type lfuCacheItem[V any] struct {
	value      V
	expiration int64
	cost       int64
}

// lfuShard is a single shard of the LFU cache.
type lfuShard[K comparable, V any] struct {
	sync.RWMutex
	store       *xsync.Map[K, *lfuCacheItem[V]]
	policy      *lfuPolicy[K, V]
	hashSeed    maphash.Seed
	validSize   atomic.Int64
	stopCleanup chan struct{}
}

// ShardedLFUCache is a sharded cache with TinyLFU-based eviction.
type ShardedLFUCache[K comparable, V any] struct {
	shards      [LFU_SHARD_COUNT]*lfuShard[K, V]
	hashSeed    maphash.Seed
	stopCleanup chan struct{}
}

// NewShardedLFUCache creates a new sharded cache with LFU eviction.
func NewShardedLFUCache[K comparable, V any]() *ShardedLFUCache[K, V] {
	return NewShardedLFUCacheWithSize[K, V](LFU_TOTAL)
}

// NewShardedLFUCacheWithSize creates a new sharded LFU cache with specified total capacity.
func NewShardedLFUCacheWithSize[K comparable, V any](totalSize int64) *ShardedLFUCache[K, V] {
	sc := &ShardedLFUCache[K, V]{
		hashSeed:    maphash.MakeSeed(),
		stopCleanup: make(chan struct{}),
	}
	perShard := totalSize / LFU_SHARD_COUNT
	numCounters := perShard * 10 // 10x counters for better accuracy

	for i := 0; i < LFU_SHARD_COUNT; i++ {
		sc.shards[i] = &lfuShard[K, V]{
			store:       xsync.NewMap[K, *lfuCacheItem[V]](xsync.WithPresize(int(perShard)), xsync.WithGrowOnly()),
			policy:      newLFUPolicy[K, V](numCounters, perShard),
			hashSeed:    sc.hashSeed,
			stopCleanup: make(chan struct{}),
		}
	}
	go sc.cleanupRoutine()
	return sc
}

func lfuFastRange(h uint64) uint64 {
	h64, _ := bits.Mul64(h, LFU_SHARD_COUNT)
	return h64
}

func (sc *ShardedLFUCache[K, V]) mask(h uint64) uint64 {
	return lfuFastRange(h)
}

func (sc *ShardedLFUCache[K, V]) keyHash(k K) uint64 {
	return maphash.Comparable(sc.hashSeed, k)
}

func (sc *ShardedLFUCache[K, V]) getShard(hash uint64) *lfuShard[K, V] {
	return sc.shards[sc.mask(hash)]
}

// Set adds an item to the cache with default cost of 1.
func (sc *ShardedLFUCache[K, V]) Set(k K, v V) bool {
	return sc.SetWithCost(k, v, defaultCost)
}

// SetWithCost adds an item with a specified cost.
func (sc *ShardedLFUCache[K, V]) SetWithCost(k K, v V, cost int64) bool {
	hash := sc.keyHash(k)
	shard := sc.getShard(hash)
	return shard.set(k, v, hash, cost, 0)
}

// SetWithTTL adds an item with a TTL and default cost of 1.
func (sc *ShardedLFUCache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) bool {
	return sc.SetWithTTLAndCost(k, v, ttl, defaultCost)
}

// SetWithTTLAndCost adds an item with both TTL and cost.
func (sc *ShardedLFUCache[K, V]) SetWithTTLAndCost(k K, v V, ttl time.Duration, cost int64) bool {
	hash := sc.keyHash(k)
	shard := sc.getShard(hash)
	expiration := time.Now().Add(ttl).UnixMilli()
	return shard.set(k, v, hash, cost, expiration)
}

func (s *lfuShard[K, V]) set(k K, v V, hash uint64, cost int64, expiration int64) bool {
	item := &lfuCacheItem[V]{
		value:      v,
		expiration: expiration,
		cost:       cost,
	}

	// Check policy for admission and eviction.
	victims, accepted, updated := s.policy.Add(hash, k, cost)

	// If item was updated in policy, update it in store too.
	if updated {
		s.store.Store(k, item)
		return true
	}

	if !accepted {
		return false
	}

	// Delete victims first.
	for _, victim := range victims {
		s.store.Compute(victim.OriginalKey, func(old *lfuCacheItem[V], loaded bool) (*lfuCacheItem[V], xsync.ComputeOp) {
			if loaded {
				s.validSize.Add(-1)
				return nil, xsync.DeleteOp
			}
			return nil, xsync.CancelOp
		})
	}

	// Add the new item.
	var added bool
	s.store.Compute(k, func(old *lfuCacheItem[V], loaded bool) (*lfuCacheItem[V], xsync.ComputeOp) {
		if !loaded {
			added = true
		}
		return item, xsync.UpdateOp
	})

	if added {
		s.validSize.Add(1)
	}

	return true
}

// Get retrieves an item from the cache.
func (sc *ShardedLFUCache[K, V]) Get(k K) (V, bool) {
	hash := sc.keyHash(k)
	shard := sc.getShard(hash)
	return shard.get(k, hash)
}

func (s *lfuShard[K, V]) get(k K, hash uint64) (V, bool) {
	item, ok := s.store.Load(k)
	if !ok {
		var zero V
		return zero, false
	}

	// Check expiration.
	if item.expiration != 0 && time.Now().UnixMilli() > item.expiration {
		s.store.Compute(k, func(oldItem *lfuCacheItem[V], loaded bool) (*lfuCacheItem[V], xsync.ComputeOp) {
			if !loaded || oldItem != item {
				return nil, xsync.CancelOp
			}
			if time.Now().UnixMilli() > oldItem.expiration {
				s.validSize.Add(-1)
				s.policy.Del(hash)
				return nil, xsync.DeleteOp
			}
			return oldItem, xsync.CancelOp
		})
		var zero V
		return zero, false
	}

	// Record access for frequency tracking.
	s.recordAccess(hash)

	return item.value, true
}

func (s *lfuShard[K, V]) recordAccess(hash uint64) {
	// Async batched frequency update for better performance
	s.policy.RecordAccess(hash)
}

// Del removes an item from the cache.
func (sc *ShardedLFUCache[K, V]) Del(k K) {
	hash := sc.keyHash(k)
	shard := sc.getShard(hash)
	shard.del(k, hash)
}

func (s *lfuShard[K, V]) del(k K, hash uint64) {
	_, loaded := s.store.LoadAndDelete(k)
	if loaded {
		s.validSize.Add(-1)
		s.policy.Del(hash)
	}
}

// GetTTL returns the remaining TTL for an item.
func (sc *ShardedLFUCache[K, V]) GetTTL(k K) (time.Duration, bool) {
	hash := sc.keyHash(k)
	shard := sc.getShard(hash)
	return shard.getTTL(k, hash)
}

func (s *lfuShard[K, V]) getTTL(k K, hash uint64) (time.Duration, bool) {
	item, ok := s.store.Load(k)
	if !ok {
		return 0, false
	}
	if item.expiration == 0 {
		return 0, false
	}
	now := time.Now().UnixMilli()
	if now > item.expiration {
		s.store.Compute(k, func(oldItem *lfuCacheItem[V], loaded bool) (*lfuCacheItem[V], xsync.ComputeOp) {
			if !loaded || oldItem != item {
				return nil, xsync.CancelOp
			}
			if now > oldItem.expiration {
				s.validSize.Add(-1)
				s.policy.Del(hash)
				return nil, xsync.DeleteOp
			}
			return oldItem, xsync.CancelOp
		})
		return 0, false
	}
	return time.Duration((item.expiration - now) * int64(time.Millisecond)), true
}

// Range iterates over all items sequentially.
// The callback is called from a single goroutine, so no external synchronization needed.
// Return false from callback to stop iteration.
func (sc *ShardedLFUCache[K, V]) Range(f func(key K, value V) bool) {
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		stop := false
		sc.shards[i].rangeItems(func(k K, v V) bool {
			if !f(k, v) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// RangeParallel iterates over all items in parallel across shards.
// WARNING: The callback may be called from multiple goroutines simultaneously.
// Ensure your callback is thread-safe or use Range() for sequential iteration.
func (sc *ShardedLFUCache[K, V]) RangeParallel(f func(key K, value V) bool) {
	var eg errgroup.Group
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		shard := sc.shards[i]
		eg.Go(func() error {
			shard.rangeItems(f)
			return nil
		})
	}
	_ = eg.Wait()
}

func (s *lfuShard[K, V]) rangeItems(f func(key K, value V) bool) {
	now := time.Now().UnixMilli()
	s.store.Range(func(k K, item *lfuCacheItem[V]) bool {
		if item.expiration != 0 && now > item.expiration {
			hash := maphash.Comparable(s.hashSeed, k)
			s.store.Compute(k, func(oldItem *lfuCacheItem[V], loaded bool) (*lfuCacheItem[V], xsync.ComputeOp) {
				if !loaded || oldItem != item {
					return nil, xsync.CancelOp
				}
				if now > oldItem.expiration {
					s.validSize.Add(-1)
					s.policy.Del(hash)
					return nil, xsync.DeleteOp
				}
				return oldItem, xsync.CancelOp
			})
			return true
		}
		return f(k, item.value)
	})
}

// Len returns the total number of valid items in the cache.
func (sc *ShardedLFUCache[K, V]) Len() int {
	var total int64
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		total += sc.shards[i].validSize.Load()
	}
	return int(total)
}

// Clear removes all items from the cache.
func (sc *ShardedLFUCache[K, V]) Clear() {
	var eg errgroup.Group
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		shard := sc.shards[i]
		eg.Go(func() error {
			shard.clear()
			return nil
		})
	}
	_ = eg.Wait()
}

func (s *lfuShard[K, V]) clear() {
	s.store.Clear()
	s.policy.Clear()
	s.validSize.Store(0)
}

func (sc *ShardedLFUCache[K, V]) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var eg errgroup.Group
			for i := 0; i < LFU_SHARD_COUNT; i++ {
				shard := sc.shards[i]
				eg.Go(func() error {
					shard.cleanup()
					return nil
				})
			}
			_ = eg.Wait()
		case <-sc.stopCleanup:
			return
		}
	}
}

func (s *lfuShard[K, V]) cleanup() {
	now := time.Now().UnixMilli()
	s.store.Range(func(k K, item *lfuCacheItem[V]) bool {
		if item.expiration != 0 && now > item.expiration {
			hash := maphash.Comparable(s.hashSeed, k)
			s.store.Compute(k, func(oldItem *lfuCacheItem[V], loaded bool) (*lfuCacheItem[V], xsync.ComputeOp) {
				if !loaded || oldItem != item {
					return nil, xsync.CancelOp
				}
				if now > oldItem.expiration {
					s.validSize.Add(-1)
					s.policy.Del(hash)
					return nil, xsync.DeleteOp
				}
				return oldItem, xsync.CancelOp
			})
		}
		return true
	})
}

// Close stops the cache cleanup routine and releases resources.
func (sc *ShardedLFUCache[K, V]) Close() {
	close(sc.stopCleanup)
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		sc.shards[i].policy.Close()
	}
}

// Cap returns the remaining capacity across all shards.
func (sc *ShardedLFUCache[K, V]) Cap() int64 {
	var total int64
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		total += sc.shards[i].policy.Cap()
	}
	return total
}

// MaxCost returns the maximum cost capacity across all shards.
func (sc *ShardedLFUCache[K, V]) MaxCost() int64 {
	var total int64
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		total += sc.shards[i].policy.MaxCost()
	}
	return total
}

// FlushAccessBuffers forces all shards to process pending access buffers.
// Useful for testing when you need immediate frequency updates.
func (sc *ShardedLFUCache[K, V]) FlushAccessBuffers() {
	for i := 0; i < LFU_SHARD_COUNT; i++ {
		sc.shards[i].policy.FlushAccessBuffer()
	}
}
