package minder

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCacheSetOverwrite(t *testing.T) {
	cache := NewCache[string, int]()

	ok := cache.Set("key1", 100)
	if !ok {
		t.Fatal("Set should return true")
	}

	val, found := cache.Get("key1")
	if !found {
		t.Fatal("Value should be found after Set")
	}
	if val != 100 {
		t.Fatalf("Expected 100, got %d", val)
	}

	ok = cache.Set("key1", 200)
	if !ok {
		t.Fatal("Second Set should return true")
	}

	val, found = cache.Get("key1")
	if !found {
		t.Fatal("Value should be found after overwrite")
	}
	if val != 200 {
		t.Fatalf("Expected 200 after overwrite, got %d", val)
	}
}

func TestCacheSetAndGet(t *testing.T) {
	cache := NewCache[int, string]()

	cache.Set(1, "test1")
	value, found := cache.Get(1)
	assert.True(t, found)
	assert.Equal(t, "test1", value)

	_, found = cache.Get(2)
	assert.False(t, found)
}

func TestCacheValidSize(t *testing.T) {
	c := NewCache[string, int]()
	c.Set("key1", 1)
	c.SetWithTTL("key2", 2, 1*time.Millisecond)
	if got := c.Len(); got != 2 {
		t.Errorf("Expected ValidSize 2, got %d", got)
	}
	time.Sleep(2 * time.Millisecond)
	c.Get("key2") // triggers expiration
	if got := c.Len(); got != 1 {
		t.Errorf("Expected ValidSize 1 after expiration, got %d", got)
	}
	c.Del("key1")
	if got := c.Len(); got != 0 {
		t.Errorf("Expected ValidSize 0 after delete, got %d", got)
	}
}

func TestCacheDelete(t *testing.T) {
	cache := NewCache[int, string]()

	cache.Set(1, "test1")
	cache.Del(1)
	_, found := cache.Get(1)
	assert.False(t, found)
}

func TestCacheClear(t *testing.T) {
	cache := NewCache[int, string]()

	cache.Set(1, "test1")
	cache.Set(2, "test2")
	cache.Clear()

	_, found := cache.Get(1)
	assert.False(t, found)
	_, found = cache.Get(2)
	assert.False(t, found)
}

func TestCacheTTLExpiration(t *testing.T) {
	cache := NewCache[int, string]()

	cache.SetWithTTL(1, "test1", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	_, found := cache.Get(1)
	assert.False(t, found)
}

func TestCacheConcurrentAccess(t *testing.T) {
	cache := NewCache[int, string]()
	wg := sync.WaitGroup{}
	const numGoroutines = 100
	const numItems = 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			for j := 0; j < numItems; j++ {
				cache.Set(goroutineID*numItems+j, "value")
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			for j := 0; j < numItems; j++ {
				_, found := cache.Get(goroutineID*numItems + j)
				assert.True(t, found)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestCacheSetAndGetAsync(t *testing.T) {
	cache := NewCache[int, string]()
	var wg sync.WaitGroup
	setDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		cache.Set(1, "test1")
		close(setDone)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-setDone
		value, found := cache.Get(1)
		assert.True(t, found)
		assert.Equal(t, "test1", value)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-setDone
		_, found := cache.Get(2)
		assert.False(t, found)
	}()

	wg.Wait()
}

func TestCacheDeleteAsync(t *testing.T) {
	cache := NewCache[int, string]()
	var wg sync.WaitGroup
	setDone := make(chan struct{})
	deleteDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		cache.Set(1, "test1")
		close(setDone)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-setDone
		cache.Del(1)
		close(deleteDone)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-deleteDone
		_, found := cache.Get(1)
		assert.False(t, found)
	}()

	wg.Wait()
}

func TestCacheClearAsync(t *testing.T) {
	cache := NewCache[int, string]()
	var wg sync.WaitGroup
	setDone := make(chan struct{})
	clearDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		cache.Set(1, "test1")
		cache.Set(2, "test2")
		close(setDone)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-setDone
		cache.Clear()
		close(clearDone)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-clearDone
		_, found := cache.Get(1)
		assert.False(t, found)
		_, found = cache.Get(2)
		assert.False(t, found)
	}()

	wg.Wait()
}

func TestCacheSetWithTTL(t *testing.T) {
	cache := NewCache[int, string]()

	cache.SetWithTTL(1, "test1", time.Minute)
	val, found := cache.Get(1)
	assert.True(t, found)
	assert.Equal(t, "test1", val)
}

func TestCacheGetTTL(t *testing.T) {
	cache := NewCache[int, string]()

	ttl := 50 * time.Millisecond
	cache.SetWithTTL(1, "test1", ttl)

	duration, found := cache.GetTTL(1)
	assert.True(t, found)
	assert.True(t, duration > 0 && duration <= ttl)

	time.Sleep(ttl + 10*time.Millisecond)
	_, found = cache.GetTTL(1)
	assert.False(t, found)
}

func TestCacheOverwrite(t *testing.T) {
	cache := NewCache[int, string]()

	cache.Set(1, "old_value")
	cache.Set(1, "new_value")

	val, found := cache.Get(1)
	assert.True(t, found)
	assert.Equal(t, "new_value", val)
}

func TestCacheConcurrentSetTTL(t *testing.T) {
	cache := NewCache[int, string]()
	wg := sync.WaitGroup{}
	const numGoroutines = 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ttl := time.Duration(id%10+1) * 10 * time.Millisecond
			cache.SetWithTTL(id, "value", ttl)
		}(i)
	}

	wg.Wait()

	for i := 0; i < numGoroutines; i++ {
		_, found := cache.Get(i)
		assert.True(t, found, "Key %d not found", i)
	}
}

func TestCachePersistentItems(t *testing.T) {
	cache := NewCache[int, string]()

	cache.Set(1, "persistent")
	time.Sleep(100 * time.Millisecond)

	val, found := cache.Get(1)
	assert.True(t, found)
	assert.Equal(t, "persistent", val)
}

func TestCacheTTLUpdate(t *testing.T) {
	cache := NewCache[int, string]()

	cache.SetWithTTL(1, "test", 100*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cache.SetWithTTL(1, "updated", 200*time.Millisecond)

	time.Sleep(150 * time.Millisecond)
	val, found := cache.Get(1)
	assert.True(t, found)
	assert.Equal(t, "updated", val)
}
