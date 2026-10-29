package cache

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NikoMalik/minder/internal/lfu"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/tidwall/btree"
)

const (
	userTreeDegree = 128
	sweepBatch     = 1024
)

var itemSeq atomic.Uint64

type UserCacheConfig struct {
	DefaultCachedCount int
	SweepInterval      time.Duration
	CacheAgeDays       int
	ShardCount         int
}

func DefaultUserCacheConfig() UserCacheConfig {
	return UserCacheConfig{
		DefaultCachedCount: 100,
		SweepInterval:      5 * time.Minute,
		CacheAgeDays:       30,
		ShardCount:         256,
	}
}

type UserRecord[K comparable, V any] struct {
	Key       K
	Value     V
	CreatedAt time.Time
	CachedAt  time.Time
}

type UserItem[K comparable, V any] = struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}

type userItem[K comparable, V any] struct {
	key       K
	value     V
	cached    int64
	createdAt int64
	cost      int64
	seq       uint64
}

func (it *userItem[K, V]) record() UserRecord[K, V] {
	return UserRecord[K, V]{
		Key:       it.key,
		Value:     it.value,
		CreatedAt: time.UnixMilli(it.createdAt),
		CachedAt:  time.UnixMilli(it.cached),
	}
}

func userItemLess[K comparable, V any](a, b *userItem[K, V]) bool {
	if a.createdAt != b.createdAt {
		return a.createdAt > b.createdAt
	}
	return a.seq < b.seq
}

type userSet[K comparable, V any] struct {
	mu          sync.RWMutex
	items       map[K]*userItem[K, V]
	tree        *btree.BTreeG[*userItem[K, V]]
	hint        btree.PathHint
	loaded      int64
	fullyLoaded bool
	dead        bool
}

func newUserSet[K comparable, V any]() *userSet[K, V] {
	return &userSet[K, V]{
		items: make(map[K]*userItem[K, V]),
		tree:  btree.NewBTreeGOptions(userItemLess[K, V], btree.Options{Degree: userTreeDegree, NoLocks: true}),
	}
}

func (s *userSet[K, V]) from(offset int, fn func(it *userItem[K, V]) bool) {
	pivot, ok := s.tree.GetAt(offset)
	if !ok {
		return
	}
	s.tree.Ascend(pivot, fn)
}

func (s *userSet[K, V]) take(offset, limit int) []*userItem[K, V] {
	if limit <= 0 || offset < 0 {
		return nil
	}
	out := make([]*userItem[K, V], 0, min(limit, len(s.items)))
	s.from(offset, func(it *userItem[K, V]) bool {
		out = append(out, it)
		return len(out) < limit
	})
	return out
}

type UserCache[U comparable, K comparable, V any] struct {
	shards []*xsync.Map[U, *userSet[K, V]]
	mask   uint64
	seed   maphash.Seed
	owners *xsync.Map[K, U]
	size   atomic.Int64
	config UserCacheConfig
	stop   chan struct{}
	closed sync.Once
}

func NewUserCache[U comparable, K comparable, V any](config UserCacheConfig) *UserCache[U, K, V] {
	if config.ShardCount <= 0 {
		config.ShardCount = 256
	}
	config.ShardCount = int(lfu.Next2Power(int64(config.ShardCount)))

	c := &UserCache[U, K, V]{
		shards: make([]*xsync.Map[U, *userSet[K, V]], config.ShardCount),
		mask:   uint64(config.ShardCount - 1),
		seed:   maphash.MakeSeed(),
		owners: xsync.NewMap[K, U](),
		config: config,
		stop:   make(chan struct{}),
	}
	for i := range c.shards {
		c.shards[i] = xsync.NewMap[U, *userSet[K, V]]()
	}
	go c.sweepRoutine()
	return c
}

func (c *UserCache[U, K, V]) users(userID U) *xsync.Map[U, *userSet[K, V]] {
	return c.shards[maphash.Comparable(c.seed, userID)&c.mask]
}

func (c *UserCache[U, K, V]) write(userID U, fn func(s *userSet[K, V])) {
	users := c.users(userID)
	for {
		s, _ := users.LoadOrCompute(userID, func() (*userSet[K, V], bool) {
			return newUserSet[K, V](), false
		})
		s.mu.Lock()
		if s.dead {
			s.mu.Unlock()
			continue
		}
		fn(s)
		s.mu.Unlock()
		return
	}
}

func (c *UserCache[U, K, V]) read(userID U, fn func(s *userSet[K, V])) {
	s, ok := c.users(userID).Load(userID)
	if !ok {
		return
	}
	s.mu.RLock()
	fn(s)
	s.mu.RUnlock()
}

func (c *UserCache[U, K, V]) insertLocked(userID U, s *userSet[K, V], it *userItem[K, V]) {
	if old, ok := s.items[it.key]; ok {
		s.tree.Delete(old)
	} else {
		c.size.Add(1)
	}
	s.items[it.key] = it
	s.tree.SetHint(it, &s.hint)
	c.owners.Store(it.key, userID)
}

func (c *UserCache[U, K, V]) deleteLocked(userID U, s *userSet[K, V], key K) bool {
	it, ok := s.items[key]
	if !ok {
		return false
	}
	delete(s.items, key)
	s.tree.Delete(it)
	c.size.Add(-1)
	c.owners.Compute(key, func(owner U, loaded bool) (U, xsync.ComputeOp) {
		if loaded && owner == userID {
			return owner, xsync.DeleteOp
		}
		return owner, xsync.CancelOp
	})
	return true
}

func (c *UserCache[U, K, V]) dropLocked(userID U, s *userSet[K, V]) {
	for key := range s.items {
		c.deleteLocked(userID, s, key)
	}
	s.dead = true
}

func newUserItem[K comparable, V any](key K, value V, createdAt time.Time, cost int64, now int64) *userItem[K, V] {
	return &userItem[K, V]{
		key:       key,
		value:     value,
		cached:    now,
		createdAt: createdAt.UnixMilli(),
		cost:      max(cost, 1),
		seq:       itemSeq.Add(1),
	}
}

func (c *UserCache[U, K, V]) Set(userID U, key K, value V, createdAt time.Time) {
	c.SetWithCost(userID, key, value, createdAt, 1)
}

func (c *UserCache[U, K, V]) SetWithCost(userID U, key K, value V, createdAt time.Time, cost int64) {
	it := newUserItem(key, value, createdAt, cost, time.Now().UnixMilli())
	c.write(userID, func(s *userSet[K, V]) {
		c.insertLocked(userID, s, it)
	})
}

func (c *UserCache[U, K, V]) Get(userID U, key K) (V, bool) {
	var value V
	var found bool
	c.read(userID, func(s *userSet[K, V]) {
		if it, ok := s.items[key]; ok {
			value, found = it.value, true
		}
	})
	return value, found
}

func (c *UserCache[U, K, V]) GetByKey(key K) (V, bool) {
	userID, ok := c.owners.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return c.Get(userID, key)
}

func (c *UserCache[U, K, V]) GetNewest(userID U, count int) []UserRecord[K, V] {
	if count <= 0 {
		count = int(^uint(0) >> 1)
	}
	return c.GetPage(userID, 0, count)
}

func (c *UserCache[U, K, V]) GetPage(userID U, offset, limit int) []UserRecord[K, V] {
	var out []UserRecord[K, V]
	c.read(userID, func(s *userSet[K, V]) {
		items := s.take(offset, limit)
		out = make([]UserRecord[K, V], len(items))
		for i, it := range items {
			out[i] = it.record()
		}
	})
	return out
}

func (c *UserCache[U, K, V]) GetAllSorted(userID U) []UserRecord[K, V] {
	return c.GetNewest(userID, 0)
}

func (c *UserCache[U, K, V]) GetUserRecordCount(userID U) int {
	var n int
	c.read(userID, func(s *userSet[K, V]) {
		n = len(s.items)
	})
	return n
}

func (c *UserCache[U, K, V]) LoadMore(userID U, items []UserItem[K, V]) int {
	n := c.LoadUserRecords(userID, items)
	c.write(userID, func(s *userSet[K, V]) {
		s.loaded += int64(n)
	})
	return n
}

func (c *UserCache[U, K, V]) LoadUserRecords(userID U, items []UserItem[K, V]) int {
	now := time.Now().UnixMilli()
	c.write(userID, func(s *userSet[K, V]) {
		for _, rec := range items {
			c.insertLocked(userID, s, newUserItem(rec.Key, rec.Value, rec.CreatedAt, rec.Cost, now))
		}
	})
	return len(items)
}

func (c *UserCache[U, K, V]) ReplaceUserRecords(userID U, items []UserItem[K, V]) {
	now := time.Now().UnixMilli()
	c.write(userID, func(s *userSet[K, V]) {
		for key := range s.items {
			c.deleteLocked(userID, s, key)
		}
		for _, rec := range items {
			c.insertLocked(userID, s, newUserItem(rec.Key, rec.Value, rec.CreatedAt, rec.Cost, now))
		}
		s.fullyLoaded = true
	})
}

func (c *UserCache[U, K, V]) GetLoadedCount(userID U) int64 {
	var n int64
	c.read(userID, func(s *userSet[K, V]) {
		n = s.loaded
	})
	return n
}

func (c *UserCache[U, K, V]) IsFullyLoaded(userID U) bool {
	var full bool
	c.read(userID, func(s *userSet[K, V]) {
		full = s.fullyLoaded
	})
	return full
}

func (c *UserCache[U, K, V]) SetFullyLoaded(userID U, fully bool) {
	s, ok := c.users(userID).Load(userID)
	if !ok {
		return
	}
	s.mu.Lock()
	s.fullyLoaded = fully
	s.mu.Unlock()
}

func (c *UserCache[U, K, V]) Del(userID U, key K) {
	s, ok := c.users(userID).Load(userID)
	if !ok {
		return
	}
	s.mu.Lock()
	c.deleteLocked(userID, s, key)
	s.mu.Unlock()
}

func (c *UserCache[U, K, V]) DelUser(userID U) {
	s, ok := c.users(userID).LoadAndDelete(userID)
	if !ok {
		return
	}
	s.mu.Lock()
	c.dropLocked(userID, s)
	s.mu.Unlock()
}

func (c *UserCache[U, K, V]) Len() int {
	return int(c.size.Load())
}

func (c *UserCache[U, K, V]) UserCount() int {
	var n int
	for _, users := range c.shards {
		n += users.Size()
	}
	return n
}

func (c *UserCache[U, K, V]) Clear() {
	for _, users := range c.shards {
		users.Range(func(userID U, s *userSet[K, V]) bool {
			users.Delete(userID)
			s.mu.Lock()
			c.dropLocked(userID, s)
			s.mu.Unlock()
			return true
		})
	}
}

func (c *UserCache[U, K, V]) Close() {
	c.closed.Do(func() {
		close(c.stop)
	})
}

func (c *UserCache[U, K, V]) ForceSweep() {
	c.sweep()
}

func (c *UserCache[U, K, V]) Config() UserCacheConfig {
	return c.config
}

func (c *UserCache[U, K, V]) sweepRoutine() {
	ticker := time.NewTicker(c.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.sweep()
		case <-c.stop:
			return
		}
	}
}

func (c *UserCache[U, K, V]) sweep() {
	maxAge := time.Duration(c.config.CacheAgeDays) * 24 * time.Hour
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	keep := c.config.DefaultCachedCount

	for _, users := range c.shards {
		users.Range(func(userID U, s *userSet[K, V]) bool {
			c.sweepUser(userID, s, cutoff, keep)
			return true
		})
	}
}

func (c *UserCache[U, K, V]) sweepUser(userID U, s *userSet[K, V], cutoff int64, keep int) {
	pos := keep
	for {
		s.mu.Lock()
		if s.dead || len(s.items) <= keep {
			s.mu.Unlock()
			return
		}
		scanned := 0
		var old []K
		s.from(pos, func(it *userItem[K, V]) bool {
			scanned++
			if it.cached < cutoff {
				old = append(old, it.key)
			}
			return scanned < sweepBatch
		})
		for _, key := range old {
			c.deleteLocked(userID, s, key)
		}
		if len(old) > 0 && len(s.items) <= keep {
			s.fullyLoaded = false
		}
		s.mu.Unlock()

		if scanned < sweepBatch {
			return
		}
		pos += scanned - len(old)
	}
}

func (c *UserCache[U, K, V]) snapshot(s *userSet[K, V]) []*userItem[K, V] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.take(0, len(s.items))
}

func (c *UserCache[U, K, V]) RangeRecords(f func(userID U, record UserRecord[K, V]) bool) {
	for _, users := range c.shards {
		stop := false
		users.Range(func(userID U, s *userSet[K, V]) bool {
			for _, it := range c.snapshot(s) {
				if !f(userID, it.record()) {
					stop = true
					return false
				}
			}
			return true
		})
		if stop {
			return
		}
	}
}

func (c *UserCache[U, K, V]) Range(f func(userID U, key K, value V) bool) {
	c.RangeRecords(func(userID U, r UserRecord[K, V]) bool {
		return f(userID, r.Key, r.Value)
	})
}

func (c *UserCache[U, K, V]) RangeUsers(f func(userID U, recordCount int) bool) {
	for _, users := range c.shards {
		stop := false
		users.Range(func(userID U, s *userSet[K, V]) bool {
			s.mu.RLock()
			n := len(s.items)
			s.mu.RUnlock()
			stop = !f(userID, n)
			return !stop
		})
		if stop {
			return
		}
	}
}

func (c *UserCache[U, K, V]) RangeByUser(userID U, f func(key K, value V) bool) {
	s, ok := c.users(userID).Load(userID)
	if !ok {
		return
	}
	for _, it := range c.snapshot(s) {
		if !f(it.key, it.value) {
			return
		}
	}
}
