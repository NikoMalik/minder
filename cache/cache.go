package cache

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NikoMalik/minder/internal/lfu"
	"github.com/puzpuzpuz/xsync/v4"
)

type Mode uint8

const (
	ModeTTL Mode = 1 << iota
	ModeLFU
)

type Config struct {
	Mode            Mode
	ShardCount      int
	MaxCost         int64
	CleanupInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		Mode:            ModeTTL,
		ShardCount:      256,
		MaxCost:         1_024_000,
		CleanupInterval: 5 * time.Minute,
	}
}

type item[V any] struct {
	value      V
	expiration int64
	cost       int64
}

func (it *item[V]) expired(now int64) bool {
	return it.expiration != 0 && now > it.expiration
}

type shard[K comparable, V any] struct {
	mu     sync.Mutex
	store  *xsync.Map[K, *item[V]]
	policy *lfu.Policy[K, V]
	size   atomic.Int64
}

type Cache[K comparable, V any] struct {
	shards    []*shard[K, V]
	mask      uint64
	seed      maphash.Seed
	config    Config
	stop      chan struct{}
	closeOnce sync.Once
}

func New[K comparable, V any](config Config) *Cache[K, V] {
	def := DefaultConfig()
	if config.Mode == 0 {
		config.Mode = def.Mode
	}
	if config.ShardCount <= 0 {
		config.ShardCount = def.ShardCount
	}
	config.ShardCount = int(lfu.Next2Power(int64(config.ShardCount)))
	if config.MaxCost <= 0 {
		config.MaxCost = def.MaxCost
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = def.CleanupInterval
	}

	c := &Cache[K, V]{
		shards: make([]*shard[K, V], config.ShardCount),
		mask:   uint64(config.ShardCount - 1),
		seed:   maphash.MakeSeed(),
		config: config,
		stop:   make(chan struct{}),
	}
	perShard := max(config.MaxCost/int64(config.ShardCount), 1)
	for i := range c.shards {
		s := &shard[K, V]{store: xsync.NewMap[K, *item[V]]()}
		if config.Mode&ModeLFU != 0 {
			s.policy = lfu.NewPolicy[K, V](perShard*10, perShard)
		}
		c.shards[i] = s
	}
	if config.Mode&ModeTTL != 0 {
		go c.cleanupRoutine()
	}
	return c
}

func (c *Cache[K, V]) locate(k K) (*shard[K, V], uint64) {
	h := maphash.Comparable(c.seed, k)
	return c.shards[h&c.mask], h
}

func (c *Cache[K, V]) Config() Config {
	return c.config
}

func (c *Cache[K, V]) Set(k K, v V) bool {
	return c.SetWithTTLAndCost(k, v, 0, 1)
}

func (c *Cache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) bool {
	return c.SetWithTTLAndCost(k, v, ttl, 1)
}

func (c *Cache[K, V]) SetWithCost(k K, v V, cost int64) bool {
	return c.SetWithTTLAndCost(k, v, 0, cost)
}

func (c *Cache[K, V]) SetWithTTLAndCost(k K, v V, ttl time.Duration, cost int64) bool {
	it := &item[V]{value: v, cost: max(cost, 1)}
	if ttl > 0 && c.config.Mode&ModeTTL != 0 {
		it.expiration = time.Now().Add(ttl).UnixMilli()
	}
	s, h := c.locate(k)
	return s.set(k, h, it)
}

func (s *shard[K, V]) set(k K, h uint64, it *item[V]) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.policy != nil {
		victims, accepted, updated := s.policy.Add(h, k, it.cost)
		if !accepted && !updated {
			return false
		}
		for _, victim := range victims {
			if _, ok := s.store.LoadAndDelete(victim.OriginalKey); ok {
				s.size.Add(-1)
			}
		}
	}
	if _, loaded := s.store.LoadAndStore(k, it); !loaded {
		s.size.Add(1)
	}
	return true
}

func (c *Cache[K, V]) Get(k K) (V, bool) {
	var zero V
	s, h := c.locate(k)
	if s.policy != nil {
		s.policy.RecordAccess(h)
	}
	it, ok := s.store.Load(k)
	if !ok {
		return zero, false
	}
	if it.expired(time.Now().UnixMilli()) {
		s.delIf(k, h, it)
		return zero, false
	}
	return it.value, true
}

func (c *Cache[K, V]) GetTTL(k K) (time.Duration, bool) {
	s, h := c.locate(k)
	it, ok := s.store.Load(k)
	if !ok || it.expiration == 0 {
		return 0, false
	}
	now := time.Now().UnixMilli()
	if it.expired(now) {
		s.delIf(k, h, it)
		return 0, false
	}
	return time.Duration(it.expiration-now) * time.Millisecond, true
}

func (c *Cache[K, V]) Del(k K) {
	s, h := c.locate(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.store.LoadAndDelete(k); ok {
		s.size.Add(-1)
		if s.policy != nil {
			s.policy.Del(h)
		}
	}
}

func (s *shard[K, V]) delIf(k K, h uint64, want *item[V]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.store.Load(k); !ok || cur != want {
		return
	}
	s.store.Delete(k)
	s.size.Add(-1)
	if s.policy != nil {
		s.policy.Del(h)
	}
}

func (c *Cache[K, V]) Range(f func(key K, value V) bool) {
	now := time.Now().UnixMilli()
	for _, s := range c.shards {
		stop := false
		s.store.Range(func(k K, it *item[V]) bool {
			if it.expired(now) {
				return true
			}
			stop = !f(k, it.value)
			return !stop
		})
		if stop {
			return
		}
	}
}

func (c *Cache[K, V]) Len() int {
	var total int64
	for _, s := range c.shards {
		total += s.size.Load()
	}
	return int(total)
}

func (c *Cache[K, V]) Cap() int64 {
	var total int64
	for _, s := range c.shards {
		if s.policy != nil {
			total += s.policy.Cap()
		}
	}
	return total
}

func (c *Cache[K, V]) MaxCost() int64 {
	var total int64
	for _, s := range c.shards {
		if s.policy != nil {
			total += s.policy.MaxCost()
		}
	}
	return total
}

func (c *Cache[K, V]) FlushAccessBuffers() {
	for _, s := range c.shards {
		if s.policy != nil {
			s.policy.FlushAccessBuffer()
		}
	}
}

func (c *Cache[K, V]) Clear() {
	for _, s := range c.shards {
		s.mu.Lock()
		s.store.Clear()
		if s.policy != nil {
			s.policy.Clear()
		}
		s.size.Store(0)
		s.mu.Unlock()
	}
}

func (c *Cache[K, V]) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		for _, s := range c.shards {
			if s.policy != nil {
				s.policy.Close()
			}
		}
	})
}

func (c *Cache[K, V]) cleanupRoutine() {
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stop:
			return
		}
	}
}

func (c *Cache[K, V]) cleanup() {
	now := time.Now().UnixMilli()
	for _, s := range c.shards {
		s.store.Range(func(k K, it *item[V]) bool {
			if it.expired(now) {
				s.delIf(k, maphash.Comparable(c.seed, k), it)
			}
			return true
		})
	}
}
