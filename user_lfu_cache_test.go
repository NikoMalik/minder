package minder

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestUserLFUCacheBasic(t *testing.T) {
	config := DefaultUserLFUConfig()
	config.DefaultCachedCount = 10
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	now := time.Now()

	// Add some records
	for i := 0; i < 5; i++ {
		cache.Set(userID, fmt.Sprintf("tx%d", i), i*100, now.Add(-time.Duration(i)*time.Hour))
	}

	// Check count
	if count := cache.GetUserRecordCount(userID); count != 5 {
		t.Errorf("Expected 5 records, got %d", count)
	}

	// Get a specific record
	if val, ok := cache.Get(userID, "tx2"); !ok || val != 200 {
		t.Errorf("Expected tx2=200, got ok=%v, val=%v", ok, val)
	}

	// Delete a record
	cache.Del(userID, "tx2")
	if _, ok := cache.Get(userID, "tx2"); ok {
		t.Error("Expected tx2 to be deleted")
	}

	if count := cache.GetUserRecordCount(userID); count != 4 {
		t.Errorf("Expected 4 records after delete, got %d", count)
	}
}

func TestUserLFUCacheGetNewest(t *testing.T) {
	config := DefaultUserLFUConfig()
	config.DefaultCachedCount = 100
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Add 20 records with different creation times
	for i := 0; i < 20; i++ {
		// Older transactions have higher indices
		createdAt := baseTime.Add(-time.Duration(i) * time.Hour)
		cache.Set(userID, fmt.Sprintf("tx%d", i), i, createdAt)
	}

	// Get 5 newest
	records := cache.GetNewest(userID, 5)
	if len(records) != 5 {
		t.Fatalf("Expected 5 records, got %d", len(records))
	}

	// Should be sorted by createdAt descending (newest first)
	// tx0 is newest (createdAt = baseTime)
	// tx1 is second (createdAt = baseTime - 1h)
	// etc.
	for i, rec := range records {
		expectedKey := fmt.Sprintf("tx%d", i)
		if rec.Key != expectedKey {
			t.Errorf("Record %d: expected key %s, got %s", i, expectedKey, rec.Key)
		}
		if rec.Value != i {
			t.Errorf("Record %d: expected value %d, got %d", i, i, rec.Value)
		}
	}
}

func TestUserLFUCacheGetAllSorted(t *testing.T) {
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Add records out of order
	cache.Set(userID, "tx3", 3, baseTime.Add(-3*time.Hour))
	cache.Set(userID, "tx1", 1, baseTime.Add(-1*time.Hour))
	cache.Set(userID, "tx5", 5, baseTime.Add(-5*time.Hour))
	cache.Set(userID, "tx2", 2, baseTime.Add(-2*time.Hour))
	cache.Set(userID, "tx4", 4, baseTime.Add(-4*time.Hour))

	records := cache.GetAllSorted(userID)
	if len(records) != 5 {
		t.Fatalf("Expected 5 records, got %d", len(records))
	}

	// Should be sorted newest first
	expectedOrder := []string{"tx1", "tx2", "tx3", "tx4", "tx5"}
	for i, rec := range records {
		if rec.Key != expectedOrder[i] {
			t.Errorf("Record %d: expected %s, got %s", i, expectedOrder[i], rec.Key)
		}
	}
}

func TestUserLFUCacheLoadUserRecords(t *testing.T) {
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Simulate loading from DB
	items := make([]struct {
		Key       string
		Value     int
		CreatedAt time.Time
		Cost      int64
	}, 50)

	for i := 0; i < 50; i++ {
		items[i] = struct {
			Key       string
			Value     int
			CreatedAt time.Time
			Cost      int64
		}{
			Key:       fmt.Sprintf("tx%d", i),
			Value:     i * 10,
			CreatedAt: baseTime.Add(-time.Duration(i) * time.Hour),
			Cost:      1,
		}
	}

	loaded := cache.LoadUserRecords(userID, items)
	if loaded != 50 {
		t.Errorf("Expected 50 loaded, got %d", loaded)
	}

	if count := cache.GetUserRecordCount(userID); count != 50 {
		t.Errorf("Expected 50 records, got %d", count)
	}

	// Verify order
	records := cache.GetNewest(userID, 10)
	for i, rec := range records {
		expectedKey := fmt.Sprintf("tx%d", i)
		if rec.Key != expectedKey {
			t.Errorf("Record %d: expected %s, got %s", i, expectedKey, rec.Key)
		}
	}
}

func TestUserLFUCacheReplaceUserRecords(t *testing.T) {
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Add initial records
	for i := 0; i < 5; i++ {
		cache.Set(userID, fmt.Sprintf("old%d", i), i, baseTime.Add(-time.Duration(i)*time.Hour))
	}

	// Replace with new records
	newItems := make([]struct {
		Key       string
		Value     int
		CreatedAt time.Time
		Cost      int64
	}, 10)

	for i := 0; i < 10; i++ {
		newItems[i] = struct {
			Key       string
			Value     int
			CreatedAt time.Time
			Cost      int64
		}{
			Key:       fmt.Sprintf("new%d", i),
			Value:     i * 100,
			CreatedAt: baseTime.Add(-time.Duration(i) * time.Minute),
			Cost:      1,
		}
	}

	cache.ReplaceUserRecords(userID, newItems)

	// Old records should be gone
	for i := 0; i < 5; i++ {
		if _, ok := cache.Get(userID, fmt.Sprintf("old%d", i)); ok {
			t.Errorf("Old record old%d should be deleted", i)
		}
	}

	// New records should exist
	if count := cache.GetUserRecordCount(userID); count != 10 {
		t.Errorf("Expected 10 records, got %d", count)
	}

	// Should be marked as expanded
	if !cache.IsUserExpanded(userID) {
		t.Error("User should be marked as expanded")
	}
}

func TestUserLFUCacheSweep(t *testing.T) {
	config := UserLFUCacheConfig{
		DefaultCachedCount: 5,
		SweepInterval:      time.Hour, // Won't trigger automatically
		CacheAgeDays:       1,         // 1 day
		TotalCapacity:      100000,
		ShardCount:         16,
	}
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	now := time.Now()

	// We need to manually set old cached times to test sweep
	// For this test, we'll just verify the mechanism works
	shard := cache.getShard(userID)
	records, _ := shard.users.LoadOrCompute(userID, func() (*userRecordSet[string, int], bool) {
		return newUserRecordSet[string, int](), false
	})

	// Add 10 records with old cached timestamps
	for i := 0; i < 10; i++ {
		item := &userCacheItem[string, int]{
			key:       fmt.Sprintf("tx%d", i),
			value:     i,
			createdAt: now.Add(-time.Duration(i) * time.Hour).UnixMilli(),
			cached:    now.Add(-48 * time.Hour).UnixMilli(), // 2 days ago (older than CacheAgeDays)
			cost:      1,
		}
		records.insert(item)

		keyHash := cache.keyHash(item.key)
		shard.policy.Add(keyHash, item.key, 1)
	}

	if count := cache.GetUserRecordCount(userID); count != 10 {
		t.Errorf("Expected 10 records before sweep, got %d", count)
	}

	// Force sweep
	cache.ForceSweep()

	// Records beyond DefaultCachedCount (5) with cached time older than 1 day should be removed
	// So records tx5-tx9 should be removed (oldest by createdAt)
	count := cache.GetUserRecordCount(userID)
	if count != 5 {
		t.Errorf("Expected 5 records after sweep, got %d", count)
	}

	// The 5 newest by createdAt should remain
	for i := 0; i < 5; i++ {
		if _, ok := cache.Get(userID, fmt.Sprintf("tx%d", i)); !ok {
			t.Errorf("Expected tx%d to remain after sweep", i)
		}
	}
}

func TestUserLFUCacheMultipleUsers(t *testing.T) {
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[int, string, string](config)
	defer cache.Close()

	baseTime := time.Now()

	// Add records for multiple users
	for userID := 1; userID <= 10; userID++ {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("user%d-tx%d", userID, i)
			value := fmt.Sprintf("value-%d-%d", userID, i)
			cache.Set(userID, key, value, baseTime.Add(-time.Duration(i)*time.Minute))
		}
	}

	// Check user count
	if userCount := cache.UserCount(); userCount != 10 {
		t.Errorf("Expected 10 users, got %d", userCount)
	}

	// Check total records
	if total := cache.Len(); total != 200 {
		t.Errorf("Expected 200 total records, got %d", total)
	}

	// Delete one user
	cache.DelUser(5)

	if userCount := cache.UserCount(); userCount != 9 {
		t.Errorf("Expected 9 users after delete, got %d", userCount)
	}

	if total := cache.Len(); total != 180 {
		t.Errorf("Expected 180 records after delete, got %d", total)
	}
}

func TestUserLFUCacheConcurrent(t *testing.T) {
	config := DefaultUserLFUConfig()
	config.TotalCapacity = 100000
	cache := NewUserLFUCache[int, string, int](config)
	defer cache.Close()

	var wg sync.WaitGroup
	const users = 50
	const recordsPerUser = 100
	baseTime := time.Now()

	// Concurrent writes
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			for i := 0; i < recordsPerUser; i++ {
				key := fmt.Sprintf("tx%d", i)
				cache.Set(userID, key, i, baseTime.Add(-time.Duration(i)*time.Minute))
			}
		}(u)
	}
	wg.Wait()

	// Concurrent reads
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			records := cache.GetNewest(userID, 50)
			if len(records) == 0 {
				t.Errorf("User %d has no records", userID)
			}
		}(u)
	}
	wg.Wait()

	// Verify
	if userCount := cache.UserCount(); userCount != users {
		t.Errorf("Expected %d users, got %d", users, userCount)
	}
}

func TestUserLFUCacheRaceConcurrent(t *testing.T) {
	config := DefaultUserLFUConfig()
	config.TotalCapacity = 10000 // Small to trigger eviction
	cache := NewUserLFUCache[int, string, int](config)
	defer cache.Close()

	var wg sync.WaitGroup
	baseTime := time.Now()

	// Concurrent writes, reads, deletes
	for i := 0; i < 100; i++ {
		wg.Add(3)

		// Writer
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				userID := (id + j) % 20
				cache.Set(userID, fmt.Sprintf("tx%d-%d", id, j), j, baseTime.Add(-time.Duration(j)*time.Minute))
			}
		}(i)

		// Reader
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				userID := (id + j) % 20
				cache.GetNewest(userID, 10)
			}
		}(i)

		// Deleter
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				userID := (id + j) % 20
				cache.Del(userID, fmt.Sprintf("tx%d-%d", id, j))
			}
		}(i)
	}

	wg.Wait()
}

func TestUserLFUCacheLFUEviction(t *testing.T) {
	config := UserLFUCacheConfig{
		DefaultCachedCount: 1000,
		SweepInterval:      time.Hour,
		CacheAgeDays:       30,
		TotalCapacity:      100, // Very small to force LFU eviction
		ShardCount:         16,
	}
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Add hot items and access them frequently
	for i := 0; i < 20; i++ {
		cache.Set(userID, fmt.Sprintf("hot%d", i), i, baseTime.Add(-time.Duration(i)*time.Hour))
	}

	// Access hot items many times
	for j := 0; j < 50; j++ {
		for i := 0; i < 20; i++ {
			cache.Get(userID, fmt.Sprintf("hot%d", i))
		}
	}

	// Add cold items - some hot items may be evicted but should survive better
	for i := 0; i < 200; i++ {
		cache.Set(userID, fmt.Sprintf("cold%d", i), i+1000, baseTime.Add(-time.Duration(i)*time.Minute))
	}

	// Check how many hot items survived
	hotSurvived := 0
	for i := 0; i < 20; i++ {
		if _, ok := cache.Get(userID, fmt.Sprintf("hot%d", i)); ok {
			hotSurvived++
		}
	}

	// Some hot items should survive due to higher frequency
	t.Logf("Hot items survived: %d/20", hotSurvived)
}

func TestUserLFUCacheGetByKeyBasic(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	config.DefaultCachedCount = 10
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user123"
	now := time.Now()

	txIDs := []string{"txA", "txB", "txC", "txD"}
	for i, txID := range txIDs {
		cache.Set(userID, txID, i*100, now.Add(-time.Duration(i)*time.Hour))
	}

	for i, txID := range txIDs {
		val, ok := cache.GetByKey(txID)
		if !ok {
			t.Errorf("GetByKey(%q) should find record", txID)
			continue
		}
		if val != i*100 {
			t.Errorf("GetByKey(%q) expected %d, got %d", txID, i*100, val)
		}
	}

	// Проверяем несуществующий ключ
	val, ok := cache.GetByKey("non-existent")
	if ok {
		t.Errorf("GetByKey(non-existent) should not find anything, got %d", val)
	}
}

func TestUserLFUCacheGetByKeyEviction(t *testing.T) {
	t.Parallel()
	config := UserLFUCacheConfig{
		DefaultCachedCount: 5,
		TotalCapacity:      10,
		ShardCount:         4,
		SweepInterval:      10 * time.Second,
		CacheAgeDays:       3,
	}
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user456"

	// Add initial item
	cache.Set(userID, "tx-evictable", 999, time.Now())

	// Verify it exists
	val, ok := cache.GetByKey("tx-evictable")
	if !ok || val != 999 {
		t.Errorf("Immediate GetByKey failed: ok=%v, val=%v", ok, val)
	}

	// Add many more items to cause eviction
	for i := 0; i < 50; i++ {
		cache.Set(userID, fmt.Sprintf("tx-filler-%d", i), i, time.Now())
	}

}

func TestUserLFUCacheGetByKeyAfterDel(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user789"
	txID := "tx-to-delete"

	cache.Set(userID, txID, 777, time.Now())

	// Verify exists before delete
	val, ok := cache.GetByKey(txID)
	if !ok || val != 777 {
		t.Errorf("GetByKey before Del should find record: ok=%v, val=%v", ok, val)
	}

	// Delete the record
	cache.Del(userID, txID)

	// GetByKey should return false after deletion
	val, ok = cache.GetByKey(txID)
	if ok {
		t.Errorf("GetByKey after Del should return false, got %d", val)
	}
}

func TestUserLFUCacheGetByKeyLFUAccess(t *testing.T) {
	t.Parallel()
	config := UserLFUCacheConfig{
		DefaultCachedCount: 5,
		TotalCapacity:      50, // enough for hot + some cold items
		ShardCount:         4,
		SweepInterval:      10 * time.Second,
		CacheAgeDays:       3,
	}
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user-hot-cold"

	// Add hot items
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("hot%d", i)
		cache.Set(userID, key, i*1000, time.Now())
	}

	// Access hot items frequently BEFORE adding cold items
	for j := 0; j < 50; j++ {
		for i := 0; i < 5; i++ {
			cache.GetByKey(fmt.Sprintf("hot%d", i))
		}
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("cold%d", i)
		cache.Set(userID, key, i*100, time.Now())
	}

	// Hot items should survive due to higher frequency
	hotFound := 0
	for i := 0; i < 5; i++ {
		if _, ok := cache.GetByKey(fmt.Sprintf("hot%d", i)); ok {
			hotFound++
		}
	}

	if hotFound < 2 {
		t.Errorf("Expected at least 2 hot items to survive via GetByKey, got %d", hotFound)
	}
}

func TestUserLFUCacheGetByKeyAfterUserDel(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	userID := "user999"
	txID := "tx-user-del"

	cache.Set(userID, txID, 12345, time.Now())

	// Verify exists before delete
	val, ok := cache.GetByKey(txID)
	if !ok || val != 12345 {
		t.Errorf("GetByKey before DelUser should find record: ok=%v, val=%v", ok, val)
	}

	cache.DelUser(userID)

	// GetByKey should return false after user deletion
	val, ok = cache.GetByKey(txID)
	if ok {
		t.Errorf("GetByKey after DelUser should return false, got %d", val)
	}
}

func TestUserLFUCacheRange(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	// Add records for multiple users
	users := []string{"user1", "user2", "user3"}
	for _, userID := range users {
		for i := 0; i < 10; i++ {
			cache.Set(userID, fmt.Sprintf("%s-tx%d", userID, i), i*100, now.Add(-time.Duration(i)*time.Hour))
		}
	}

	// Test Range - count all records
	count := 0
	cache.Range(func(userID string, key string, value int) bool {
		count++
		return true
	})
	if count != 30 {
		t.Errorf("Range expected 30 records, got %d", count)
	}

	// Test Range with early stop
	count = 0
	cache.Range(func(userID string, key string, value int) bool {
		count++
		return count < 15
	})
	if count != 15 {
		t.Errorf("Range with stop expected 15, got %d", count)
	}
}

func TestUserLFUCacheRangeRecords(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	cache.Set("user1", "tx1", 100, now)
	cache.Set("user1", "tx2", 200, now.Add(-time.Hour))

	// Test RangeRecords - verify metadata
	var records []UserRecord[string, int]
	cache.RangeRecords(func(userID string, rec UserRecord[string, int]) bool {
		if userID == "user1" {
			records = append(records, rec)
		}
		return true
	})

	if len(records) != 2 {
		t.Errorf("RangeRecords expected 2 records, got %d", len(records))
	}
}

func TestUserLFUCacheRangeUsers(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	// Add different number of records per user
	cache.Set("user1", "tx1", 100, now)
	cache.Set("user1", "tx2", 200, now)
	cache.Set("user2", "tx1", 100, now)
	cache.Set("user3", "tx1", 100, now)
	cache.Set("user3", "tx2", 200, now)
	cache.Set("user3", "tx3", 300, now)

	// Test RangeUsers
	userCounts := make(map[string]int)
	cache.RangeUsers(func(userID string, recordCount int) bool {
		userCounts[userID] = recordCount
		return true
	})

	if len(userCounts) != 3 {
		t.Errorf("RangeUsers expected 3 users, got %d", len(userCounts))
	}
	if userCounts["user1"] != 2 {
		t.Errorf("user1 expected 2 records, got %d", userCounts["user1"])
	}
	if userCounts["user2"] != 1 {
		t.Errorf("user2 expected 1 record, got %d", userCounts["user2"])
	}
	if userCounts["user3"] != 3 {
		t.Errorf("user3 expected 3 records, got %d", userCounts["user3"])
	}
}

func TestUserLFUCacheRangeByUser(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	cache.Set("user1", "tx1", 100, now)
	cache.Set("user1", "tx2", 200, now)
	cache.Set("user2", "tx1", 999, now)

	// Test RangeByUser
	var keys []string
	cache.RangeByUser("user1", func(key string, value int) bool {
		keys = append(keys, key)
		return true
	})

	if len(keys) != 2 {
		t.Errorf("RangeByUser expected 2 keys, got %d", len(keys))
	}
}

func TestUserLFUCacheRangeRaceCondition(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	config.TotalCapacity = 10000
	cache := NewUserLFUCache[int, string, int](config)
	defer cache.Close()

	now := time.Now()

	// Add initial data
	for u := 0; u < 10; u++ {
		for i := 0; i < 100; i++ {
			cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
		}
	}

	var wg sync.WaitGroup

	// Concurrent Range reads
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				sum := 0
				cache.Range(func(userID int, key string, value int) bool {
					sum += value
					return true
				})
			}
		}()
	}

	// Concurrent writes
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.Set(gid, fmt.Sprintf("new-tx%d", i), i*10, now)
			}
		}(g)
	}

	// Concurrent deletes
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				cache.Del(gid, fmt.Sprintf("tx%d-%d", gid, i))
			}
		}(g)
	}

	wg.Wait()
}

func TestUserLFUCacheLen(t *testing.T) {
	t.Parallel()
	config := DefaultUserLFUConfig()
	cache := NewUserLFUCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	if cache.Len() != 0 {
		t.Errorf("Empty cache Len expected 0, got %d", cache.Len())
	}

	// Add records
	for i := 0; i < 100; i++ {
		cache.Set("user1", fmt.Sprintf("tx%d", i), i, now)
	}

	if cache.Len() != 100 {
		t.Errorf("After adding 100, Len expected 100, got %d", cache.Len())
	}

	// Delete some
	for i := 0; i < 30; i++ {
		cache.Del("user1", fmt.Sprintf("tx%d", i))
	}

	if cache.Len() != 70 {
		t.Errorf("After deleting 30, Len expected 70, got %d", cache.Len())
	}

	// Clear
	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("After Clear, Len expected 0, got %d", cache.Len())
	}
}
