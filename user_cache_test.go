package minder

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestUserCacheBasic(t *testing.T) {
	config := DefaultUserCacheConfig()
	config.DefaultCachedCount = 10
	cache := NewUserCache[string, string, int](config)
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

func TestUserCacheGetNewest(t *testing.T) {
	config := DefaultUserCacheConfig()
	config.DefaultCachedCount = 100
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Add 20 records with different creation times
	for i := 0; i < 20; i++ {
		createdAt := baseTime.Add(-time.Duration(i) * time.Hour)
		cache.Set(userID, fmt.Sprintf("tx%d", i), i, createdAt)
	}

	// Get 5 newest
	records := cache.GetNewest(userID, 5)
	if len(records) != 5 {
		t.Fatalf("Expected 5 records, got %d", len(records))
	}

	// Should be sorted by createdAt descending (newest first)
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

func TestUserCacheGetAllSorted(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
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

func TestUserCacheSetAlwaysAccepts(t *testing.T) {
	config := UserCacheConfig{
		DefaultCachedCount: 5,
		SweepInterval:      time.Hour,
		CacheAgeDays:       30,
		ShardCount:         4,
	}
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	now := time.Now()

	// Add many records — all should be accepted (no admission control)
	for i := 0; i < 200; i++ {
		cache.Set(userID, fmt.Sprintf("tx%d", i), i, now.Add(-time.Duration(i)*time.Minute))
	}

	if count := cache.GetUserRecordCount(userID); count != 200 {
		t.Errorf("Expected 200 records (all accepted), got %d", count)
	}
}

func TestUserCacheLoadUserRecords(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

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

func TestUserCacheReplaceUserRecords(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
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

	// Should be marked as fully loaded
	if !cache.IsFullyLoaded(userID) {
		t.Error("User should be marked as fully loaded after ReplaceUserRecords")
	}
}

func TestUserCacheSweep(t *testing.T) {
	config := UserCacheConfig{
		DefaultCachedCount: 5,
		SweepInterval:      time.Hour,
		CacheAgeDays:       1,
		ShardCount:         16,
	}
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	now := time.Now()

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
			cached:    now.Add(-48 * time.Hour).UnixMilli(), // 2 days ago
			cost:      1,
			seq:       itemSeq.Add(1),
		}
		records.insert(item)
		shard.keyIndex.Store(item.key, userID)
		shard.globalKeyIdx.Store(item.key, globalKeyEntry{shardIdx: shard.shardIdx})
		shard.validSize.Add(1)
	}

	if count := cache.GetUserRecordCount(userID); count != 10 {
		t.Errorf("Expected 10 records before sweep, got %d", count)
	}

	cache.ForceSweep()

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

func TestUserCacheMultipleUsers(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[int, string, string](config)
	defer cache.Close()

	baseTime := time.Now()

	for userID := 1; userID <= 10; userID++ {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("user%d-tx%d", userID, i)
			value := fmt.Sprintf("value-%d-%d", userID, i)
			cache.Set(userID, key, value, baseTime.Add(-time.Duration(i)*time.Minute))
		}
	}

	if userCount := cache.UserCount(); userCount != 10 {
		t.Errorf("Expected 10 users, got %d", userCount)
	}

	if total := cache.Len(); total != 200 {
		t.Errorf("Expected 200 total records, got %d", total)
	}

	cache.DelUser(5)

	if userCount := cache.UserCount(); userCount != 9 {
		t.Errorf("Expected 9 users after delete, got %d", userCount)
	}

	if total := cache.Len(); total != 180 {
		t.Errorf("Expected 180 records after delete, got %d", total)
	}
}

func TestUserCacheConcurrent(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[int, string, int](config)
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

	if userCount := cache.UserCount(); userCount != users {
		t.Errorf("Expected %d users, got %d", users, userCount)
	}
}

func TestUserCacheRaceConcurrent(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[int, string, int](config)
	defer cache.Close()

	var wg sync.WaitGroup
	baseTime := time.Now()

	for i := 0; i < 100; i++ {
		wg.Add(3)

		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				userID := (id + j) % 20
				cache.Set(userID, fmt.Sprintf("tx%d-%d", id, j), j, baseTime.Add(-time.Duration(j)*time.Minute))
			}
		}(i)

		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				userID := (id + j) % 20
				cache.GetNewest(userID, 10)
			}
		}(i)

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

func TestUserCacheGetByKeyBasic(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	config.DefaultCachedCount = 10
	cache := NewUserCache[string, string, int](config)
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

	val, ok := cache.GetByKey("non-existent")
	if ok {
		t.Errorf("GetByKey(non-existent) should not find anything, got %d", val)
	}
}

func TestUserCacheGetByKeyAfterDel(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user789"
	txID := "tx-to-delete"

	cache.Set(userID, txID, 777, time.Now())

	val, ok := cache.GetByKey(txID)
	if !ok || val != 777 {
		t.Errorf("GetByKey before Del should find record: ok=%v, val=%v", ok, val)
	}

	cache.Del(userID, txID)

	val, ok = cache.GetByKey(txID)
	if ok {
		t.Errorf("GetByKey after Del should return false, got %d", val)
	}
}

func TestUserCacheGetByKeyAfterUserDel(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user999"
	txID := "tx-user-del"

	cache.Set(userID, txID, 12345, time.Now())

	val, ok := cache.GetByKey(txID)
	if !ok || val != 12345 {
		t.Errorf("GetByKey before DelUser should find record: ok=%v, val=%v", ok, val)
	}

	cache.DelUser(userID)

	val, ok = cache.GetByKey(txID)
	if ok {
		t.Errorf("GetByKey after DelUser should return false, got %d", val)
	}
}

func TestUserCacheRange(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	users := []string{"user1", "user2", "user3"}
	for _, userID := range users {
		for i := 0; i < 10; i++ {
			cache.Set(userID, fmt.Sprintf("%s-tx%d", userID, i), i*100, now.Add(-time.Duration(i)*time.Hour))
		}
	}

	count := 0
	cache.Range(func(userID string, key string, value int) bool {
		count++
		return true
	})
	if count != 30 {
		t.Errorf("Range expected 30 records, got %d", count)
	}

	count = 0
	cache.Range(func(userID string, key string, value int) bool {
		count++
		return count < 15
	})
	if count != 15 {
		t.Errorf("Range with stop expected 15, got %d", count)
	}
}

func TestUserCacheRangeRecords(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	cache.Set("user1", "tx1", 100, now)
	cache.Set("user1", "tx2", 200, now.Add(-time.Hour))

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

func TestUserCacheRangeUsers(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	cache.Set("user1", "tx1", 100, now)
	cache.Set("user1", "tx2", 200, now)
	cache.Set("user2", "tx1", 100, now)
	cache.Set("user3", "tx1", 100, now)
	cache.Set("user3", "tx2", 200, now)
	cache.Set("user3", "tx3", 300, now)

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

func TestUserCacheRangeByUser(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	cache.Set("user1", "tx1", 100, now)
	cache.Set("user1", "tx2", 200, now)
	cache.Set("user2", "tx1", 999, now)

	var keys []string
	cache.RangeByUser("user1", func(key string, value int) bool {
		keys = append(keys, key)
		return true
	})

	if len(keys) != 2 {
		t.Errorf("RangeByUser expected 2 keys, got %d", len(keys))
	}
}

func TestUserCacheRangeRaceCondition(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[int, string, int](config)
	defer cache.Close()

	now := time.Now()

	for u := 0; u < 10; u++ {
		for i := 0; i < 100; i++ {
			cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
		}
	}

	var wg sync.WaitGroup

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

	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.Set(gid, fmt.Sprintf("new-tx%d", i), i*10, now)
			}
		}(g)
	}

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

func TestUserCacheLen(t *testing.T) {
	t.Parallel()
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	now := time.Now()

	if cache.Len() != 0 {
		t.Errorf("Empty cache Len expected 0, got %d", cache.Len())
	}

	for i := 0; i < 100; i++ {
		cache.Set("user1", fmt.Sprintf("tx%d", i), i, now)
	}

	if cache.Len() != 100 {
		t.Errorf("After adding 100, Len expected 100, got %d", cache.Len())
	}

	for i := 0; i < 30; i++ {
		cache.Del("user1", fmt.Sprintf("tx%d", i))
	}

	if cache.Len() != 70 {
		t.Errorf("After deleting 30, Len expected 70, got %d", cache.Len())
	}

	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("After Clear, Len expected 0, got %d", cache.Len())
	}
}

func TestUserCacheLoadMore(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Initial set of records
	for i := 0; i < 5; i++ {
		cache.Set(userID, fmt.Sprintf("init%d", i), i, baseTime.Add(-time.Duration(i)*time.Minute))
	}

	// LoadMore — first batch
	batch1 := make([]struct {
		Key       string
		Value     int
		CreatedAt time.Time
		Cost      int64
	}, 10)
	for i := 0; i < 10; i++ {
		batch1[i] = struct {
			Key       string
			Value     int
			CreatedAt time.Time
			Cost      int64
		}{
			Key:       fmt.Sprintf("batch1-%d", i),
			Value:     i * 10,
			CreatedAt: baseTime.Add(-time.Duration(i+10) * time.Minute),
			Cost:      1,
		}
	}

	loaded := cache.LoadMore(userID, batch1)
	if loaded != 10 {
		t.Errorf("Expected 10 loaded, got %d", loaded)
	}

	if cache.GetLoadedCount(userID) != 10 {
		t.Errorf("Expected loaded count 10, got %d", cache.GetLoadedCount(userID))
	}

	if cache.GetUserRecordCount(userID) != 15 {
		t.Errorf("Expected 15 total records, got %d", cache.GetUserRecordCount(userID))
	}

	// LoadMore — second batch
	batch2 := make([]struct {
		Key       string
		Value     int
		CreatedAt time.Time
		Cost      int64
	}, 5)
	for i := 0; i < 5; i++ {
		batch2[i] = struct {
			Key       string
			Value     int
			CreatedAt time.Time
			Cost      int64
		}{
			Key:       fmt.Sprintf("batch2-%d", i),
			Value:     i * 100,
			CreatedAt: baseTime.Add(-time.Duration(i+20) * time.Minute),
			Cost:      1,
		}
	}

	loaded = cache.LoadMore(userID, batch2)
	if loaded != 5 {
		t.Errorf("Expected 5 loaded, got %d", loaded)
	}

	if cache.GetLoadedCount(userID) != 15 {
		t.Errorf("Expected loaded count 15 after two batches, got %d", cache.GetLoadedCount(userID))
	}

	if cache.GetUserRecordCount(userID) != 20 {
		t.Errorf("Expected 20 total records, got %d", cache.GetUserRecordCount(userID))
	}
}

func TestUserCachePagination(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	baseTime := time.Now()

	// Add 20 records with distinct timestamps
	for i := 0; i < 20; i++ {
		cache.Set(userID, fmt.Sprintf("tx%d", i), i, baseTime.Add(-time.Duration(i)*time.Hour))
	}

	// Page 0: first 5 records (newest)
	page0 := cache.GetPage(userID, 0, 5)
	if len(page0) != 5 {
		t.Fatalf("Page 0: expected 5, got %d", len(page0))
	}
	for i, rec := range page0 {
		expected := fmt.Sprintf("tx%d", i)
		if rec.Key != expected {
			t.Errorf("Page 0[%d]: expected %s, got %s", i, expected, rec.Key)
		}
	}

	// Page 1: records 5-9
	page1 := cache.GetPage(userID, 5, 5)
	if len(page1) != 5 {
		t.Fatalf("Page 1: expected 5, got %d", len(page1))
	}
	for i, rec := range page1 {
		expected := fmt.Sprintf("tx%d", i+5)
		if rec.Key != expected {
			t.Errorf("Page 1[%d]: expected %s, got %s", i, expected, rec.Key)
		}
	}

	// Page beyond data
	pageBeyond := cache.GetPage(userID, 20, 5)
	if len(pageBeyond) != 0 {
		t.Errorf("Page beyond: expected 0, got %d", len(pageBeyond))
	}

	// Partial last page
	pageLast := cache.GetPage(userID, 18, 5)
	if len(pageLast) != 2 {
		t.Errorf("Last page: expected 2, got %d", len(pageLast))
	}
}

func TestUserCacheLoadCursor(t *testing.T) {
	config := DefaultUserCacheConfig()
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"

	// Initially not fully loaded, loaded count is 0
	if cache.IsFullyLoaded(userID) {
		t.Error("New user should not be fully loaded")
	}
	if cache.GetLoadedCount(userID) != 0 {
		t.Errorf("New user loaded count should be 0, got %d", cache.GetLoadedCount(userID))
	}

	// Need to create the user first
	cache.Set(userID, "init", 0, time.Now())

	// Still not fully loaded
	if cache.IsFullyLoaded(userID) {
		t.Error("User should not be fully loaded after Set")
	}

	// Mark as fully loaded
	cache.SetFullyLoaded(userID, true)
	if !cache.IsFullyLoaded(userID) {
		t.Error("User should be fully loaded after SetFullyLoaded(true)")
	}

	// Unmark
	cache.SetFullyLoaded(userID, false)
	if cache.IsFullyLoaded(userID) {
		t.Error("User should not be fully loaded after SetFullyLoaded(false)")
	}
}

func TestUserCacheSweepResetsFullyLoaded(t *testing.T) {
	config := UserCacheConfig{
		DefaultCachedCount: 5,
		SweepInterval:      time.Hour,
		CacheAgeDays:       1,
		ShardCount:         16,
	}
	cache := NewUserCache[string, string, int](config)
	defer cache.Close()

	userID := "user1"
	now := time.Now()

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
			cached:    now.Add(-48 * time.Hour).UnixMilli(),
			cost:      1,
			seq:       itemSeq.Add(1),
		}
		records.insert(item)
		shard.keyIndex.Store(item.key, userID)
		shard.globalKeyIdx.Store(item.key, globalKeyEntry{shardIdx: shard.shardIdx})
		shard.validSize.Add(1)
	}

	// Mark as fully loaded
	cache.SetFullyLoaded(userID, true)
	if !cache.IsFullyLoaded(userID) {
		t.Fatal("Should be fully loaded before sweep")
	}

	cache.ForceSweep()

	// After sweep, should no longer be fully loaded (trimmed to DefaultCachedCount)
	if cache.IsFullyLoaded(userID) {
		t.Error("Should not be fully loaded after sweep trimmed records")
	}

	if count := cache.GetUserRecordCount(userID); count != 5 {
		t.Errorf("Expected 5 records after sweep, got %d", count)
	}
}
