package minder

import (
	"cmp"
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// Config for UserLFUCache
type UserLFUCacheConfig struct {
	// DefaultCachedCount - count cache items for user
	DefaultCachedCount int

	// SweepInterval - interval for sweep clean
	SweepInterval time.Duration

	// CacheAgeDays - max time for cache item(ttl)
	CacheAgeDays int

	// TotalCapacity - capacity for lfu evictions
	TotalCapacity int64

	// ShardCount - shard count,must be power of t2o
	ShardCount int
}

// DefaultUserLFUConfig returns default configuration
func DefaultUserLFUConfig() UserLFUCacheConfig {
	return UserLFUCacheConfig{
		DefaultCachedCount: 100,
		SweepInterval:      5 * time.Minute,
		CacheAgeDays:       30,
		TotalCapacity:      1_000_000,
		ShardCount:         256,
	}
}

// UserRecord represents a cached record with metadata
type UserRecord[K comparable, V any] struct {
	Key       K
	Value     V
	CreatedAt time.Time
	CachedAt  time.Time
}

// userCacheItem stores value with metadata
type userCacheItem[K comparable, V any] struct {
	key       K
	value     V
	cached    int64
	createdAt int64
	cost      int64
}

type userRecordSet[K comparable, V any] struct {
	sync.RWMutex
	records map[K]*userCacheItem[K, V]
	//  createdAt (newest first)
	sorted []*userCacheItem[K, V]
	// expanded - if full history
	expanded atomic.Bool
}

func newUserRecordSet[K comparable, V any]() *userRecordSet[K, V] {
	return &userRecordSet[K, V]{
		records: make(map[K]*userCacheItem[K, V]),
		sorted:  make([]*userCacheItem[K, V], 0),
	}
}

// insert with sort
func (rs *userRecordSet[K, V]) insert(item *userCacheItem[K, V]) {
	rs.Lock()
	defer rs.Unlock()

	// delete old
	if old, exists := rs.records[item.key]; exists {
		rs.removeFromSortedLocked(old)
	}

	rs.records[item.key] = item

	// binary search for insertion point
	idx, _ := slices.BinarySearchFunc(rs.sorted, item, func(a, b *userCacheItem[K, V]) int {
		return cmp.Compare(b.createdAt, a.createdAt)
	})
	rs.sorted = slices.Insert(rs.sorted, idx, item)
}

func (rs *userRecordSet[K, V]) removeFromSortedLocked(item *userCacheItem[K, V]) {
	for i, it := range rs.sorted {
		if it == item {
			rs.sorted = slices.Delete(rs.sorted, i, i+1)
			return
		}
	}
}

func (rs *userRecordSet[K, V]) delete(key K) (*userCacheItem[K, V], bool) {
	rs.Lock()
	defer rs.Unlock()

	item, exists := rs.records[key]
	if !exists {
		return nil, false
	}

	delete(rs.records, key)
	rs.removeFromSortedLocked(item)
	return item, true
}

func (rs *userRecordSet[K, V]) get(key K) (*userCacheItem[K, V], bool) {
	rs.RLock()
	defer rs.RUnlock()
	item, ok := rs.records[key]
	return item, ok
}

func (rs *userRecordSet[K, V]) count() int {
	rs.RLock()
	defer rs.RUnlock()
	return len(rs.records)
}

// getNewest return N newest  (sorted)
func (rs *userRecordSet[K, V]) getNewest(n int) []*userCacheItem[K, V] {
	rs.RLock()
	defer rs.RUnlock()

	if n <= 0 || n > len(rs.sorted) {
		n = len(rs.sorted)
	}

	result := make([]*userCacheItem[K, V], n)
	copy(result, rs.sorted[:n])
	return result
}

// getAll return all sorted values
func (rs *userRecordSet[K, V]) getAll() []*userCacheItem[K, V] {
	rs.RLock()
	defer rs.RUnlock()

	result := make([]*userCacheItem[K, V], len(rs.sorted))
	copy(result, rs.sorted)
	return result
}

func (rs *userRecordSet[K, V]) clear() {
	rs.Lock()
	defer rs.Unlock()
	rs.records = make(map[K]*userCacheItem[K, V])
	rs.sorted = rs.sorted[:0]
}

// userLFUShard is a single shard
type userLFUShard[U comparable, K comparable, V any] struct {
	sync.RWMutex
	users        *xsync.Map[U, *userRecordSet[K, V]]
	keyIndex     *xsync.Map[K, U] // key -> userID mapping for fast lookup and eviction
	globalKeyIdx *xsync.Map[K, globalKeyEntry]
	policy       *lfuPolicy[K, V]
	hashSeed     maphash.Seed
	config       *UserLFUCacheConfig
	shardIdx     int
	validSize    atomic.Int64
}

// globalKeyEntry stores shard index for O(1) key lookup
type globalKeyEntry struct {
	shardIdx int
}

// UserLFUCache is a cache with per-user record limits and LFU eviction
type UserLFUCache[U comparable, K comparable, V any] struct {
	shards       []*userLFUShard[U, K, V]
	globalKeyIdx *xsync.Map[K, globalKeyEntry] // key -> shardIdx for O(1) GetByKey
	shardCount   int
	shardMask    uint64
	hashSeed     maphash.Seed
	config       UserLFUCacheConfig
	stopCleanup  chan struct{}
}

// GetByKey retrieves a record by key only (without knowing userID).
// Uses globalKeyIdx for O(1) shard lookup.
func (c *UserLFUCache[U, K, V]) GetByKey(k K) (V, bool) {
	entry, ok := c.globalKeyIdx.Load(k)
	if !ok {
		var zero V
		return zero, false
	}

	shard := c.shards[entry.shardIdx]
	userID, ok := shard.keyIndex.Load(k)
	if !ok {
		// Stale global index entry, clean up
		c.globalKeyIdx.Delete(k)
		var zero V
		return zero, false
	}

	keyHash := c.keyHash(k)
	value, found := shard.get(userID, k, keyHash)
	if !found {
		// Record was evicted, clean up indices
		shard.keyIndex.Delete(k)
		c.globalKeyIdx.Delete(k)
		var zero V
		return zero, false
	}

	return value, true
}

// NewUserLFUCache creates a new cache with per-user limits and LFU eviction
func NewUserLFUCache[U comparable, K comparable, V any](config UserLFUCacheConfig) *UserLFUCache[U, K, V] {
	if config.ShardCount <= 0 {
		config.ShardCount = 256
	}
	config.ShardCount = int(next2Power(int64(config.ShardCount)))

	perShard := config.TotalCapacity / int64(config.ShardCount)
	numCounters := perShard * 10

	cache := &UserLFUCache[U, K, V]{
		shards:       make([]*userLFUShard[U, K, V], config.ShardCount),
		globalKeyIdx: xsync.NewMap[K, globalKeyEntry](xsync.WithPresize(int(config.TotalCapacity))),
		shardCount:   config.ShardCount,
		shardMask:    uint64(config.ShardCount - 1),
		hashSeed:     maphash.MakeSeed(),
		config:       config,
		stopCleanup:  make(chan struct{}),
	}

	for i := 0; i < config.ShardCount; i++ {
		cache.shards[i] = &userLFUShard[U, K, V]{
			users:        xsync.NewMap[U, *userRecordSet[K, V]](),
			keyIndex:     xsync.NewMap[K, U](),
			globalKeyIdx: cache.globalKeyIdx,
			policy:       newLFUPolicy[K, V](numCounters, perShard),
			hashSeed:     cache.hashSeed,
			config:       &cache.config,
			shardIdx:     i,
		}
	}

	go cache.sweepRoutine()
	return cache
}

func (c *UserLFUCache[U, K, V]) getShard(userID U) *userLFUShard[U, K, V] {
	hash := maphash.Comparable(c.hashSeed, userID)
	return c.shards[hash&c.shardMask]
}

func (c *UserLFUCache[U, K, V]) keyHash(key K) uint64 {
	return maphash.Comparable(c.hashSeed, key)
}

// Set adds or updates a record for a user
// createdAt - time created item (for sort)
func (c *UserLFUCache[U, K, V]) Set(userID U, key K, value V, createdAt time.Time) bool {
	return c.SetWithCost(userID, key, value, createdAt, 1)
}

// SetWithCost adds or updates a record with specified cost
func (c *UserLFUCache[U, K, V]) SetWithCost(userID U, key K, value V, createdAt time.Time, cost int64) bool {
	shard := c.getShard(userID)
	keyHash := c.keyHash(key)
	return shard.set(userID, key, keyHash, value, createdAt, cost)
}

func (s *userLFUShard[U, K, V]) set(userID U, key K, keyHash uint64, value V, createdAt time.Time, cost int64) bool {
	now := time.Now().UnixMilli()
	item := &userCacheItem[K, V]{
		key:       key,
		value:     value,
		cached:    now,
		createdAt: createdAt.UnixMilli(),
		cost:      cost,
	}

	// Get or create user record set
	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	// Check policy for admission
	victims, accepted, updated := s.policy.Add(keyHash, key, cost)

	if updated {
		records.insert(item)
		s.keyIndex.Store(key, userID)
		s.globalKeyIdx.Store(key, globalKeyEntry{shardIdx: s.shardIdx})
		return true
	}

	if !accepted {
		return false
	}

	// Delete victims using keyIndex for O(1) lookup
	for _, victim := range victims {
		if victimUserID, ok := s.keyIndex.Load(victim.OriginalKey); ok {
			if victimRecords, ok := s.users.Load(victimUserID); ok {
				victimRecords.delete(victim.OriginalKey)
			}
			s.keyIndex.Delete(victim.OriginalKey)
			s.globalKeyIdx.Delete(victim.OriginalKey)
			s.validSize.Add(-1)
		}
	}

	records.insert(item)
	s.keyIndex.Store(key, userID)
	s.globalKeyIdx.Store(key, globalKeyEntry{shardIdx: s.shardIdx})
	s.validSize.Add(1)
	return true
}

// Get retrieves a record and updates access frequency
func (c *UserLFUCache[U, K, V]) Get(userID U, key K) (V, bool) {
	shard := c.getShard(userID)
	keyHash := c.keyHash(key)
	return shard.get(userID, key, keyHash)
}

func (s *userLFUShard[U, K, V]) get(userID U, key K, keyHash uint64) (V, bool) {
	var zero V

	records, ok := s.users.Load(userID)
	if !ok {
		return zero, false
	}

	item, ok := records.get(key)
	if !ok {
		return zero, false
	}

	// Record access for LFU
	s.policy.IncrementAccess(keyHash)

	return item.value, true
}

// GetNewest returns the N newest records for a user (sorted by createdAt, newest first)
// This is the main method for getting DEFAULT_CACHED_COUNT newest records
func (c *UserLFUCache[U, K, V]) GetNewest(userID U, count int) []UserRecord[K, V] {
	shard := c.getShard(userID)
	return shard.getNewest(userID, count, c.keyHash)
}

func (s *userLFUShard[U, K, V]) getNewest(userID U, count int, keyHashFn func(K) uint64) []UserRecord[K, V] {
	records, ok := s.users.Load(userID)
	if !ok {
		return nil
	}

	items := records.getNewest(count)
	result := make([]UserRecord[K, V], len(items))

	for i, item := range items {
		// Update access frequency for each retrieved item
		keyHash := keyHashFn(item.key)
		s.policy.IncrementAccess(keyHash)

		result[i] = UserRecord[K, V]{
			Key:       item.key,
			Value:     item.value,
			CreatedAt: time.UnixMilli(item.createdAt),
			CachedAt:  time.UnixMilli(item.cached),
		}
	}

	return result
}

// GetAllSorted returns all records for a user sorted by createdAt (newest first)
func (c *UserLFUCache[U, K, V]) GetAllSorted(userID U) []UserRecord[K, V] {
	shard := c.getShard(userID)
	return shard.getAllSorted(userID, c.keyHash)
}

func (s *userLFUShard[U, K, V]) getAllSorted(userID U, keyHashFn func(K) uint64) []UserRecord[K, V] {
	records, ok := s.users.Load(userID)
	if !ok {
		return nil
	}

	items := records.getAll()
	result := make([]UserRecord[K, V], len(items))

	for i, item := range items {
		keyHash := keyHashFn(item.key)
		s.policy.IncrementAccess(keyHash)

		result[i] = UserRecord[K, V]{
			Key:       item.key,
			Value:     item.value,
			CreatedAt: time.UnixMilli(item.createdAt),
			CachedAt:  time.UnixMilli(item.cached),
		}
	}

	return result
}

// GetUserRecordCount returns the number of cached records for a user
func (c *UserLFUCache[U, K, V]) GetUserRecordCount(userID U) int {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if !ok {
		return 0
	}
	return records.count()
}

// IsUserExpanded returns whether the user has requested full history
func (c *UserLFUCache[U, K, V]) IsUserExpanded(userID U) bool {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if !ok {
		return false
	}
	return records.expanded.Load()
}

// SetUserExpanded marks a user as having full history loaded
func (c *UserLFUCache[U, K, V]) SetUserExpanded(userID U, expanded bool) {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if ok {
		records.expanded.Store(expanded)
	}
}

// LoadUserRecords loads multiple records for a user at once (for initial load from DB)
// All records get the same cached timestamp
func (c *UserLFUCache[U, K, V]) LoadUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) int {
	shard := c.getShard(userID)
	return shard.loadUserRecords(userID, items, c.keyHash)
}

func (s *userLFUShard[U, K, V]) loadUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}, keyHashFn func(K) uint64) int {
	now := time.Now().UnixMilli()

	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	loaded := 0
	for _, rec := range items {
		keyHash := keyHashFn(rec.Key)
		cost := rec.Cost
		if cost <= 0 {
			cost = 1
		}

		item := &userCacheItem[K, V]{
			key:       rec.Key,
			value:     rec.Value,
			cached:    now,
			createdAt: rec.CreatedAt.UnixMilli(),
			cost:      cost,
		}

		_, accepted, updated := s.policy.Add(keyHash, rec.Key, cost)
		if accepted {
			records.insert(item)
			s.keyIndex.Store(rec.Key, userID)
			s.globalKeyIdx.Store(rec.Key, globalKeyEntry{shardIdx: s.shardIdx})
			s.validSize.Add(1)
			loaded++
		} else if updated {
			records.insert(item)
			s.keyIndex.Store(rec.Key, userID)
			s.globalKeyIdx.Store(rec.Key, globalKeyEntry{shardIdx: s.shardIdx})
			loaded++
		}
	}

	return loaded
}

// ReplaceUserRecords replaces all records for a user (for full history load)
// All records get the same cached timestamp to preserve relative age
func (c *UserLFUCache[U, K, V]) ReplaceUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) {
	shard := c.getShard(userID)
	shard.replaceUserRecords(userID, items, c.keyHash)
}

func (s *userLFUShard[U, K, V]) replaceUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}, keyHashFn func(K) uint64) {
	now := time.Now().UnixMilli()

	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	// Delete old records from policy and keyIndex
	oldItems := records.getAll()
	for _, item := range oldItems {
		keyHash := keyHashFn(item.key)
		s.policy.Del(keyHash)
		s.keyIndex.Delete(item.key)
		s.globalKeyIdx.Delete(item.key)
	}
	s.validSize.Add(-int64(len(oldItems)))

	// Clear old records
	records.clear()

	// Add new records with same cached timestamp
	added := 0
	for _, rec := range items {
		keyHash := keyHashFn(rec.Key)
		cost := rec.Cost
		if cost <= 0 {
			cost = 1
		}

		item := &userCacheItem[K, V]{
			key:       rec.Key,
			value:     rec.Value,
			cached:    now,
			createdAt: rec.CreatedAt.UnixMilli(),
			cost:      cost,
		}

		_, accepted, _ := s.policy.Add(keyHash, rec.Key, cost)
		if accepted {
			records.insert(item)
			s.keyIndex.Store(rec.Key, userID)
			s.globalKeyIdx.Store(rec.Key, globalKeyEntry{shardIdx: s.shardIdx})
			added++
		}
	}
	s.validSize.Add(int64(added))

	records.expanded.Store(true)
}

// Del removes a record
func (c *UserLFUCache[U, K, V]) Del(userID U, key K) {
	shard := c.getShard(userID)
	keyHash := c.keyHash(key)
	shard.del(userID, key, keyHash)
}

func (s *userLFUShard[U, K, V]) del(userID U, key K, keyHash uint64) {
	records, ok := s.users.Load(userID)
	if !ok {
		return
	}

	if _, deleted := records.delete(key); deleted {
		s.policy.Del(keyHash)
		s.keyIndex.Delete(key)
		s.globalKeyIdx.Delete(key)
		s.validSize.Add(-1)
	}
}

// DelUser removes all records for a user
func (c *UserLFUCache[U, K, V]) DelUser(userID U) {
	shard := c.getShard(userID)
	shard.delUser(userID, c.keyHash)
}

func (s *userLFUShard[U, K, V]) delUser(userID U, keyHashFn func(K) uint64) {
	records, ok := s.users.LoadAndDelete(userID)
	if !ok {
		return
	}

	items := records.getAll()
	for _, item := range items {
		keyHash := keyHashFn(item.key)
		s.policy.Del(keyHash)
		s.keyIndex.Delete(item.key)
		s.globalKeyIdx.Delete(item.key)
	}
	s.validSize.Add(-int64(len(items)))
}

// sweepRoutine runs periodic cleanup
func (c *UserLFUCache[U, K, V]) sweepRoutine() {
	ticker := time.NewTicker(c.config.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sweep()
		case <-c.stopCleanup:
			return
		}
	}
}

// sweep removes old records beyond DefaultCachedCount
func (c *UserLFUCache[U, K, V]) sweep() {
	maxAge := time.Duration(c.config.CacheAgeDays) * 24 * time.Hour
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	defaultCount := c.config.DefaultCachedCount

	for _, shard := range c.shards {
		shard.sweep(cutoff, defaultCount, c.keyHash)
	}
}

func (s *userLFUShard[U, K, V]) sweep(cutoffTime int64, defaultCount int, keyHashFn func(K) uint64) {
	s.users.Range(func(userID U, records *userRecordSet[K, V]) bool {
		records.Lock()
		count := len(records.sorted)

		if count <= defaultCount {
			records.Unlock()
			return true
		}

		toDelete := make([]K, 0)
		for i := defaultCount; i < len(records.sorted); i++ {
			if records.sorted[i].cached < cutoffTime {
				toDelete = append(toDelete, records.sorted[i].key)
			}
		}
		records.Unlock()

		// Delete outside of lock
		deleted := 0
		for _, key := range toDelete {
			if item, ok := records.delete(key); ok {
				keyHash := keyHashFn(item.key)
				s.policy.Del(keyHash)
				s.keyIndex.Delete(item.key)
				s.globalKeyIdx.Delete(item.key)
				deleted++
			}
		}
		s.validSize.Add(-int64(deleted))

		// Reset expanded flag if trimmed to default count
		if records.count() <= defaultCount {
			records.expanded.Store(false)
		}

		return true
	})
}

// Len returns total number of cached records (O(shards)
func (c *UserLFUCache[U, K, V]) Len() int {
	var total int64
	for _, shard := range c.shards {
		total += shard.validSize.Load()
	}
	return int(total)
}

// UserCount returns number of users with cached records
func (c *UserLFUCache[U, K, V]) UserCount() int {
	var total int
	for _, shard := range c.shards {
		shard.users.Range(func(u U, records *userRecordSet[K, V]) bool {
			total++
			return true
		})
	}
	return total
}

// Clear removes all cached records
func (c *UserLFUCache[U, K, V]) Clear() {
	c.globalKeyIdx.Clear()
	for _, shard := range c.shards {
		shard.users.Range(func(u U, records *userRecordSet[K, V]) bool {
			records.clear()
			return true
		})
		shard.users.Clear()
		shard.keyIndex.Clear()
		shard.policy.Clear()
		shard.validSize.Store(0)
	}
}

// Close stops the cleanup routine
func (c *UserLFUCache[U, K, V]) Close() {
	close(c.stopCleanup)
	for _, shard := range c.shards {
		shard.policy.Close()
	}
}

// ForceSweep runs cleanup immediately (for testing)
func (c *UserLFUCache[U, K, V]) ForceSweep() {
	c.sweep()
}

// Config returns the cache configuration
func (c *UserLFUCache[U, K, V]) Config() UserLFUCacheConfig {
	return c.config
}

// Range iterates over all records in the cache.
// The callback receives userID, key, value for each record.
// Return false from the callback to stop iteration.
func (c *UserLFUCache[U, K, V]) Range(f func(userID U, key K, value V) bool) {
	for _, shard := range c.shards {
		stop := false
		shard.users.Range(func(userID U, records *userRecordSet[K, V]) bool {
			for _, item := range records.getAll() {
				if !f(userID, item.key, item.value) {
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

// RangeRecords iterates over all records with full metadata.
// The callback receives userID and UserRecord for each record.
// Return false from the callback to stop iteration.
func (c *UserLFUCache[U, K, V]) RangeRecords(f func(userID U, record UserRecord[K, V]) bool) {
	for _, shard := range c.shards {
		stop := false
		shard.users.Range(func(userID U, records *userRecordSet[K, V]) bool {
			for _, item := range records.getAll() {
				rec := UserRecord[K, V]{
					Key:       item.key,
					Value:     item.value,
					CreatedAt: time.UnixMilli(item.createdAt),
					CachedAt:  time.UnixMilli(item.cached),
				}
				if !f(userID, rec) {
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

// RangeUsers iterates over all users in the cache.
// The callback receives userID and record count for each user.
// Return false from the callback to stop iteration.
func (c *UserLFUCache[U, K, V]) RangeUsers(f func(userID U, recordCount int) bool) {
	for _, shard := range c.shards {
		stop := false
		shard.users.Range(func(userID U, records *userRecordSet[K, V]) bool {
			if !f(userID, records.count()) {
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

// RangeByUser iterates over all records for a specific user.
// Return false from the callback to stop iteration.
func (c *UserLFUCache[U, K, V]) RangeByUser(userID U, f func(key K, value V) bool) {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if !ok {
		return
	}
	for _, item := range records.getAll() {
		if !f(item.key, item.value) {
			return
		}
	}
}
