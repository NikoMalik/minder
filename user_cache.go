package minder

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/tidwall/btree"
)

// itemSeq is a global monotonic counter for btree tie-breaking.
// Avoids fmt.Sprint allocations in the comparator.
var itemSeq atomic.Uint64

// UserCacheConfig is the configuration for UserCache.
type UserCacheConfig struct {
	// DefaultCachedCount - per-user cap for cached items.
	DefaultCachedCount int

	// SweepInterval - interval for periodic cleanup.
	SweepInterval time.Duration

	// CacheAgeDays - max cache item age (TTL in days).
	CacheAgeDays int

	// ShardCount - number of shards, must be power of two.
	ShardCount int
}

// DefaultUserCacheConfig returns default configuration.
func DefaultUserCacheConfig() UserCacheConfig {
	return UserCacheConfig{
		DefaultCachedCount: 100,
		SweepInterval:      5 * time.Minute,
		CacheAgeDays:       30,
		ShardCount:         256,
	}
}

// UserRecord represents a cached record with metadata.
type UserRecord[K comparable, V any] struct {
	Key       K
	Value     V
	CreatedAt time.Time
	CachedAt  time.Time
}

// userCacheItem stores value with metadata.
type userCacheItem[K comparable, V any] struct {
	key       K
	value     V
	cached    int64
	createdAt int64
	cost      int64
	seq       uint64 // monotonic ID for btree uniqueness
}

// globalKeyEntry stores shard index for O(1) key lookup.
type globalKeyEntry struct {
	shardIdx int
}

// loadCursor tracks incremental loading state per user.
type loadCursor struct {
	loaded      atomic.Int64
	fullyLoaded atomic.Bool
}

type userRecordSet[K comparable, V any] struct {
	records *xsync.Map[K, *userCacheItem[K, V]]
	tree    *btree.BTreeG[*userCacheItem[K, V]]
	cursor  loadCursor
}

func userCacheItemLess[K comparable, V any](a, b *userCacheItem[K, V]) bool {
	if a.createdAt != b.createdAt {
		return a.createdAt > b.createdAt // desc
	}
	return a.seq < b.seq
}

func newUserRecordSet[K comparable, V any]() *userRecordSet[K, V] {
	return &userRecordSet[K, V]{
		records: xsync.NewMap[K, *userCacheItem[K, V]](),
		tree:    btree.NewBTreeGOptions(userCacheItemLess[K, V], btree.Options{Degree: 128}),
	}
}

// insert adds or replaces an item.
func (rs *userRecordSet[K, V]) insert(item *userCacheItem[K, V]) {
	// delete old if exists
	if old, exists := rs.records.Load(item.key); exists {
		rs.tree.Delete(old)
	}

	rs.records.Store(item.key, item)
	rs.tree.Set(item)
}

func (rs *userRecordSet[K, V]) delete(key K) (*userCacheItem[K, V], bool) {
	item, exists := rs.records.Load(key)
	if !exists {
		return nil, false
	}

	rs.records.Delete(key)
	rs.tree.Delete(item)
	return item, true
}

func (rs *userRecordSet[K, V]) get(key K) (*userCacheItem[K, V], bool) {
	item, ok := rs.records.Load(key)
	return item, ok
}

func (rs *userRecordSet[K, V]) count() int {
	return rs.records.Size()
}

// getNewest returns N newest items (tree is already sorted createdAt desc).
func (rs *userRecordSet[K, V]) getNewest(n int) []*userCacheItem[K, V] {

	total := rs.records.Size()
	if n <= 0 || n > total {
		n = total
	}

	result := make([]*userCacheItem[K, V], 0, n)
	rs.tree.Scan(func(item *userCacheItem[K, V]) bool {
		result = append(result, item)
		return len(result) < n
	})
	return result
}

// getPage returns items at [offset, offset+limit) in createdAt desc order.
func (rs *userRecordSet[K, V]) getPage(offset, limit int) []*userCacheItem[K, V] {

	if limit <= 0 {
		return nil
	}

	result := make([]*userCacheItem[K, V], 0, limit)
	skipped := 0
	rs.tree.Scan(func(item *userCacheItem[K, V]) bool {
		if skipped < offset {
			skipped++
			return true
		}
		result = append(result, item)
		return len(result) < limit
	})
	return result
}

// getAll returns all items sorted by createdAt desc.
func (rs *userRecordSet[K, V]) getAll() []*userCacheItem[K, V] {

	result := make([]*userCacheItem[K, V], 0, rs.records.Size())
	rs.tree.Scan(func(item *userCacheItem[K, V]) bool {
		result = append(result, item)
		return true
	})
	return result
}

func (rs *userRecordSet[K, V]) clear() {
	rs.records = xsync.NewMap[K, *userCacheItem[K, V]]()
	rs.tree = btree.NewBTreeGOptions(userCacheItemLess[K, V], btree.Options{Degree: 64})
}

// scanFromIndex scans tree items starting at index `start`, calling fn for each.
// Returns under RLock. Used by sweep.
func (rs *userRecordSet[K, V]) scanFromIndex(start int, fn func(item *userCacheItem[K, V]) bool) {
	idx := 0
	rs.tree.Scan(func(item *userCacheItem[K, V]) bool {
		if idx < start {
			idx++
			return true
		}
		idx++
		return fn(item)
	})
}

// userCacheShard is a single shard.
type userCacheShard[U comparable, K comparable, V any] struct {
	sync.RWMutex
	users        *xsync.Map[U, *userRecordSet[K, V]]
	keyIndex     *xsync.Map[K, U] // key -> userID mapping
	globalKeyIdx *xsync.Map[K, globalKeyEntry]
	hashSeed     maphash.Seed
	config       *UserCacheConfig
	shardIdx     int
	validSize    atomic.Int64
}

// UserCache is a cache with per-user record limits (no LFU eviction).
type UserCache[U comparable, K comparable, V any] struct {
	shards       []*userCacheShard[U, K, V]
	globalKeyIdx *xsync.Map[K, globalKeyEntry]
	shardCount   int
	shardMask    uint64
	hashSeed     maphash.Seed
	config       UserCacheConfig
	stopCleanup  chan struct{}
}

// NewUserCache creates a new cache with per-user limits.
func NewUserCache[U comparable, K comparable, V any](config UserCacheConfig) *UserCache[U, K, V] {
	if config.ShardCount <= 0 {
		config.ShardCount = 256
	}
	config.ShardCount = int(next2Power(int64(config.ShardCount)))

	cache := &UserCache[U, K, V]{
		shards:       make([]*userCacheShard[U, K, V], config.ShardCount),
		globalKeyIdx: xsync.NewMap[K, globalKeyEntry](),
		shardCount:   config.ShardCount,
		shardMask:    uint64(config.ShardCount - 1),
		hashSeed:     maphash.MakeSeed(),
		config:       config,
		stopCleanup:  make(chan struct{}),
	}

	for i := 0; i < config.ShardCount; i++ {
		cache.shards[i] = &userCacheShard[U, K, V]{
			users:        xsync.NewMap[U, *userRecordSet[K, V]](),
			keyIndex:     xsync.NewMap[K, U](),
			globalKeyIdx: cache.globalKeyIdx,
			hashSeed:     cache.hashSeed,
			config:       &cache.config,
			shardIdx:     i,
		}
	}

	go cache.sweepRoutine()
	return cache
}

func (c *UserCache[U, K, V]) getShard(userID U) *userCacheShard[U, K, V] {
	hash := maphash.Comparable(c.hashSeed, userID)
	return c.shards[hash&c.shardMask]
}

func (c *UserCache[U, K, V]) keyHash(key K) uint64 {
	return maphash.Comparable(c.hashSeed, key)
}

// GetByKey retrieves a record by key only (without knowing userID).
// Uses globalKeyIdx for O(1) shard lookup.
func (c *UserCache[U, K, V]) GetByKey(k K) (V, bool) {
	entry, ok := c.globalKeyIdx.Load(k)
	if !ok {
		var zero V
		return zero, false
	}

	shard := c.shards[entry.shardIdx]
	userID, ok := shard.keyIndex.Load(k)
	if !ok {
		c.globalKeyIdx.Delete(k)
		var zero V
		return zero, false
	}

	value, found := shard.get(userID, k)
	if !found {
		shard.keyIndex.Delete(k)
		c.globalKeyIdx.Delete(k)
		var zero V
		return zero, false
	}

	return value, true
}

// Set adds or updates a record for a user.
// Always accepts the record (no admission control).
func (c *UserCache[U, K, V]) Set(userID U, key K, value V, createdAt time.Time) {
	c.SetWithCost(userID, key, value, createdAt, 1)
}

// SetWithCost adds or updates a record with specified cost.
func (c *UserCache[U, K, V]) SetWithCost(userID U, key K, value V, createdAt time.Time, cost int64) {
	shard := c.getShard(userID)
	shard.set(userID, key, value, createdAt, cost)
}

func (s *userCacheShard[U, K, V]) set(userID U, key K, value V, createdAt time.Time, cost int64) {
	now := time.Now().UnixMilli()
	item := &userCacheItem[K, V]{
		key:       key,
		value:     value,
		cached:    now,
		createdAt: createdAt.UnixMilli(),
		cost:      cost,
		seq:       itemSeq.Add(1),
	}

	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	// Check if this is a new key (not update)
	isNew := true
	_, exists := records.records.Load(key)
	if exists {
		isNew = false
	}

	records.insert(item)
	s.keyIndex.Store(key, userID)
	s.globalKeyIdx.Store(key, globalKeyEntry{shardIdx: s.shardIdx})
	if isNew {
		s.validSize.Add(1)
	}
}

// Get retrieves a record value.
func (c *UserCache[U, K, V]) Get(userID U, key K) (V, bool) {
	shard := c.getShard(userID)
	return shard.get(userID, key)
}

func (s *userCacheShard[U, K, V]) get(userID U, key K) (V, bool) {
	var zero V

	records, ok := s.users.Load(userID)
	if !ok {
		return zero, false
	}

	item, ok := records.get(key)
	if !ok {
		return zero, false
	}

	return item.value, true
}

// GetNewest returns the N newest records for a user (sorted by createdAt desc).
func (c *UserCache[U, K, V]) GetNewest(userID U, count int) []UserRecord[K, V] {
	shard := c.getShard(userID)
	return shard.getNewest(userID, count)
}

func (s *userCacheShard[U, K, V]) getNewest(userID U, count int) []UserRecord[K, V] {
	records, ok := s.users.Load(userID)
	if !ok {
		return nil
	}

	items := records.getNewest(count)
	result := make([]UserRecord[K, V], len(items))
	for i, item := range items {
		result[i] = UserRecord[K, V]{
			Key:       item.key,
			Value:     item.value,
			CreatedAt: time.UnixMilli(item.createdAt),
			CachedAt:  time.UnixMilli(item.cached),
		}
	}
	return result
}

// GetPage returns a page of records [offset, offset+limit) sorted by createdAt desc.
func (c *UserCache[U, K, V]) GetPage(userID U, offset, limit int) []UserRecord[K, V] {
	shard := c.getShard(userID)
	return shard.getPage(userID, offset, limit)
}

func (s *userCacheShard[U, K, V]) getPage(userID U, offset, limit int) []UserRecord[K, V] {
	records, ok := s.users.Load(userID)
	if !ok {
		return nil
	}

	items := records.getPage(offset, limit)
	result := make([]UserRecord[K, V], len(items))
	for i, item := range items {
		result[i] = UserRecord[K, V]{
			Key:       item.key,
			Value:     item.value,
			CreatedAt: time.UnixMilli(item.createdAt),
			CachedAt:  time.UnixMilli(item.cached),
		}
	}
	return result
}

// GetAllSorted returns all records for a user sorted by createdAt (newest first).
func (c *UserCache[U, K, V]) GetAllSorted(userID U) []UserRecord[K, V] {
	shard := c.getShard(userID)
	return shard.getAllSorted(userID)
}

func (s *userCacheShard[U, K, V]) getAllSorted(userID U) []UserRecord[K, V] {
	records, ok := s.users.Load(userID)
	if !ok {
		return nil
	}

	items := records.getAll()
	result := make([]UserRecord[K, V], len(items))
	for i, item := range items {
		result[i] = UserRecord[K, V]{
			Key:       item.key,
			Value:     item.value,
			CreatedAt: time.UnixMilli(item.createdAt),
			CachedAt:  time.UnixMilli(item.cached),
		}
	}
	return result
}

// GetUserRecordCount returns the number of cached records for a user.
func (c *UserCache[U, K, V]) GetUserRecordCount(userID U) int {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if !ok {
		return 0
	}
	return records.count()
}

// LoadMore adds a batch of records incrementally (pagination from DB).
// Returns the number of records actually loaded.
func (c *UserCache[U, K, V]) LoadMore(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) int {
	shard := c.getShard(userID)
	return shard.loadMore(userID, items)
}

func (s *userCacheShard[U, K, V]) loadMore(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) int {
	now := time.Now().UnixMilli()

	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	loaded := 0
	for _, rec := range items {
		cost := rec.Cost
		if cost <= 0 {
			cost = 1
		}

		// Check if key already exists
		_, exists := records.records.Load(rec.Key)

		item := &userCacheItem[K, V]{
			key:       rec.Key,
			value:     rec.Value,
			cached:    now,
			createdAt: rec.CreatedAt.UnixMilli(),
			cost:      cost,
			seq:       itemSeq.Add(1),
		}

		records.insert(item)
		s.keyIndex.Store(rec.Key, userID)
		s.globalKeyIdx.Store(rec.Key, globalKeyEntry{shardIdx: s.shardIdx})
		if !exists {
			s.validSize.Add(1)
		}
		loaded++
	}

	records.cursor.loaded.Add(int64(loaded))
	return loaded
}

// GetLoadedCount returns how many records were loaded via LoadMore for a user.
func (c *UserCache[U, K, V]) GetLoadedCount(userID U) int64 {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if !ok {
		return 0
	}
	return records.cursor.loaded.Load()
}

// IsFullyLoaded returns whether all DB data has been loaded for a user.
func (c *UserCache[U, K, V]) IsFullyLoaded(userID U) bool {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if !ok {
		return false
	}
	return records.cursor.fullyLoaded.Load()
}

// SetFullyLoaded marks whether all DB data has been loaded for a user.
func (c *UserCache[U, K, V]) SetFullyLoaded(userID U, fully bool) {
	shard := c.getShard(userID)
	records, ok := shard.users.Load(userID)
	if ok {
		records.cursor.fullyLoaded.Store(fully)
	}
}

// LoadUserRecords loads multiple records for a user at once (for initial load from DB).
// All records get the same cached timestamp.
func (c *UserCache[U, K, V]) LoadUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) int {
	shard := c.getShard(userID)
	return shard.loadUserRecords(userID, items)
}

func (s *userCacheShard[U, K, V]) loadUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) int {
	now := time.Now().UnixMilli()

	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	loaded := 0
	for _, rec := range items {
		cost := rec.Cost
		if cost <= 0 {
			cost = 1
		}

		_, exists := records.records.Load(rec.Key)

		item := &userCacheItem[K, V]{
			key:       rec.Key,
			value:     rec.Value,
			cached:    now,
			createdAt: rec.CreatedAt.UnixMilli(),
			cost:      cost,
			seq:       itemSeq.Add(1),
		}

		records.insert(item)
		s.keyIndex.Store(rec.Key, userID)
		s.globalKeyIdx.Store(rec.Key, globalKeyEntry{shardIdx: s.shardIdx})
		if !exists {
			s.validSize.Add(1)
		}
		loaded++
	}

	return loaded
}

// ReplaceUserRecords replaces all records for a user.
func (c *UserCache[U, K, V]) ReplaceUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) {
	shard := c.getShard(userID)
	shard.replaceUserRecords(userID, items)
}

func (s *userCacheShard[U, K, V]) replaceUserRecords(userID U, items []struct {
	Key       K
	Value     V
	CreatedAt time.Time
	Cost      int64
}) {
	now := time.Now().UnixMilli()

	records, _ := s.users.LoadOrCompute(userID, func() (*userRecordSet[K, V], bool) {
		return newUserRecordSet[K, V](), false
	})

	// Delete old records from indices
	oldItems := records.getAll()
	for _, item := range oldItems {
		s.keyIndex.Delete(item.key)
		s.globalKeyIdx.Delete(item.key)
	}
	s.validSize.Add(-int64(len(oldItems)))

	// Clear old records
	records.clear()

	// Add new records
	for _, rec := range items {
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
			seq:       itemSeq.Add(1),
		}

		records.insert(item)
		s.keyIndex.Store(rec.Key, userID)
		s.globalKeyIdx.Store(rec.Key, globalKeyEntry{shardIdx: s.shardIdx})
	}
	s.validSize.Add(int64(len(items)))

	records.cursor.fullyLoaded.Store(true)
}

// Del removes a record.
func (c *UserCache[U, K, V]) Del(userID U, key K) {
	shard := c.getShard(userID)
	shard.del(userID, key)
}

func (s *userCacheShard[U, K, V]) del(userID U, key K) {
	records, ok := s.users.Load(userID)
	if !ok {
		return
	}

	if _, deleted := records.delete(key); deleted {
		s.keyIndex.Delete(key)
		s.globalKeyIdx.Delete(key)
		s.validSize.Add(-1)
	}
}

// DelUser removes all records for a user.
func (c *UserCache[U, K, V]) DelUser(userID U) {
	shard := c.getShard(userID)
	shard.delUser(userID)
}

func (s *userCacheShard[U, K, V]) delUser(userID U) {
	records, ok := s.users.LoadAndDelete(userID)
	if !ok {
		return
	}

	items := records.getAll()
	for _, item := range items {
		s.keyIndex.Delete(item.key)
		s.globalKeyIdx.Delete(item.key)
	}
	s.validSize.Add(-int64(len(items)))
}

// sweepRoutine runs periodic cleanup.
func (c *UserCache[U, K, V]) sweepRoutine() {
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

// sweep removes old records beyond DefaultCachedCount.
func (c *UserCache[U, K, V]) sweep() {
	maxAge := time.Duration(c.config.CacheAgeDays) * 24 * time.Hour
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	defaultCount := c.config.DefaultCachedCount

	for _, shard := range c.shards {
		shard.sweep(cutoff, defaultCount)
	}
}

func (s *userCacheShard[U, K, V]) sweep(cutoffTime int64, defaultCount int) {
	s.users.Range(func(userID U, records *userRecordSet[K, V]) bool {
		count := records.count()
		if count <= defaultCount {
			return true
		}

		// Collect keys to delete: items beyond defaultCount with old cached time
		toDelete := make([]K, 0)
		records.scanFromIndex(defaultCount, func(item *userCacheItem[K, V]) bool {
			if item.cached < cutoffTime {
				toDelete = append(toDelete, item.key)
			}
			return true
		})

		// Delete outside of lock
		deleted := 0
		for _, key := range toDelete {
			if _, ok := records.delete(key); ok {
				s.keyIndex.Delete(key)
				s.globalKeyIdx.Delete(key)
				deleted++
			}
		}
		s.validSize.Add(-int64(deleted))

		// Reset fullyLoaded if trimmed to default count
		if records.count() <= defaultCount {
			records.cursor.fullyLoaded.Store(false)
		}

		return true
	})
}

// Len returns total number of cached records.
func (c *UserCache[U, K, V]) Len() int {
	var total int64
	for _, shard := range c.shards {
		total += shard.validSize.Load()
	}
	return int(total)
}

// UserCount returns number of users with cached records.
func (c *UserCache[U, K, V]) UserCount() int {
	var total int
	for _, shard := range c.shards {
		shard.users.Range(func(u U, records *userRecordSet[K, V]) bool {
			total++
			return true
		})
	}
	return total
}

// Clear removes all cached records.
func (c *UserCache[U, K, V]) Clear() {
	c.globalKeyIdx.Clear()
	for _, shard := range c.shards {
		shard.users.Range(func(u U, records *userRecordSet[K, V]) bool {
			records.clear()
			return true
		})
		shard.users.Clear()
		shard.keyIndex.Clear()
		shard.validSize.Store(0)
	}
}

// Close stops the cleanup routine.
func (c *UserCache[U, K, V]) Close() {
	close(c.stopCleanup)
}

// ForceSweep runs cleanup immediately (for testing).
func (c *UserCache[U, K, V]) ForceSweep() {
	c.sweep()
}

// Config returns the cache configuration.
func (c *UserCache[U, K, V]) Config() UserCacheConfig {
	return c.config
}

// Range iterates over all records in the cache.
// The callback receives userID, key, value for each record.
// Return false from the callback to stop iteration.
func (c *UserCache[U, K, V]) Range(f func(userID U, key K, value V) bool) {
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
func (c *UserCache[U, K, V]) RangeRecords(f func(userID U, record UserRecord[K, V]) bool) {
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
func (c *UserCache[U, K, V]) RangeUsers(f func(userID U, recordCount int) bool) {
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
func (c *UserCache[U, K, V]) RangeByUser(userID U, f func(key K, value V) bool) {
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
