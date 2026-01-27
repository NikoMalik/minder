package minder

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type syncMapWrapper struct {
	m sync.Map
}

func (s *syncMapWrapper) Set(k string, v int) bool {
	s.m.Store(k, v)
	return true
}

func (s *syncMapWrapper) Get(k string) (int, bool) {
	v, ok := s.m.Load(k)
	if !ok {
		return 0, false
	}
	return v.(int), true
}

func (s *syncMapWrapper) Close() {}

func BenchmarkCacheOperations(b *testing.B) {
	const totalItems = 100_0000
	const goroutines = 40

	benchCases := []struct {
		name      string
		cacheType string
	}{
		{"SingleCache", "single"},
		{"ShardedCache", "sharded"},
		{"SyncMap", "syncMap"},
		{"LFU", "lfu"},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			var cache interface {
				Set(k string, v int) bool
				Get(k string) (int, bool)
				Close()
			}

			switch bc.cacheType {
			case "single":
				cache = NewCache[string, int]()
			case "sharded":
				cache = NewShardedCache[string, int]()
			case "syncMap":
				cache = &syncMapWrapper{}
			case "lfu":
				cache = NewShardedLFUCache[string, int]()
			}
			defer cache.Close()

			b.Run("Set", func(b *testing.B) {
				b.ResetTimer()
				var wg sync.WaitGroup
				for g := 0; g < goroutines; g++ {
					wg.Add(1)
					go func(gid int) {
						defer wg.Done()
						for i := 0; i < b.N/goroutines; i++ {
							cache.Set(fmt.Sprintf("key%d-%d", gid, i), i)
						}
					}(g)
				}
				wg.Wait()
			})

			for i := 0; i < totalItems; i++ {
				cache.Set(fmt.Sprintf("key%d", i), i)
			}

			// Get
			b.Run("Get", func(b *testing.B) {
				b.ResetTimer()
				var wg sync.WaitGroup
				for g := 0; g < goroutines; g++ {
					wg.Add(1)
					go func(gid int) {
						defer wg.Done()
						for i := 0; i < b.N/goroutines; i++ {
							cache.Get(fmt.Sprintf("key%d", i%totalItems))
						}
					}(g)
				}
				wg.Wait()
			})

			// Mixed (50% Set, 50% Get)
			b.Run("Mixed", func(b *testing.B) {
				b.ResetTimer()
				var wg sync.WaitGroup
				for g := 0; g < goroutines; g++ {
					wg.Add(1)
					go func(gid int) {
						defer wg.Done()
						for i := 0; i < b.N/goroutines; i++ {
							key := fmt.Sprintf("key%d-%d", gid, i)
							if i%2 == 0 {
								cache.Set(key, i)
							} else {
								cache.Get(key)
							}
						}
					}(g)
				}
				wg.Wait()
			})
		})
	}
}

// BenchmarkUserLFUCache benchmarks UserLFUCache operations
func BenchmarkUserLFUCache(b *testing.B) {
	const goroutines = 40
	const usersCount = 100
	const recordsPerUser = 100

	config := UserLFUCacheConfig{
		DefaultCachedCount: 50,
		TotalCapacity:      1_000_000,
		ShardCount:         256,
		SweepInterval:      time.Hour,
		CacheAgeDays:       30,
	}

	b.Run("Set", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		b.ResetTimer()
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < b.N/goroutines; i++ {
					userID := (gid*1000 + i) % usersCount
					key := fmt.Sprintf("tx%d-%d", gid, i)
					cache.Set(userID, key, i, now)
				}
			}(g)
		}
		wg.Wait()
	})

	b.Run("Get", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		// Pre-populate
		for u := 0; u < usersCount; u++ {
			for i := 0; i < recordsPerUser; i++ {
				cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
			}
		}

		b.ResetTimer()
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < b.N/goroutines; i++ {
					userID := (gid + i) % usersCount
					key := fmt.Sprintf("tx%d-%d", userID, i%recordsPerUser)
					cache.Get(userID, key)
				}
			}(g)
		}
		wg.Wait()
	})

	b.Run("GetByKey", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		// Pre-populate
		for u := 0; u < usersCount; u++ {
			for i := 0; i < recordsPerUser; i++ {
				cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
			}
		}

		b.ResetTimer()
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < b.N/goroutines; i++ {
					userID := (gid + i) % usersCount
					key := fmt.Sprintf("tx%d-%d", userID, i%recordsPerUser)
					cache.GetByKey(key)
				}
			}(g)
		}
		wg.Wait()
	})

	b.Run("Mixed", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		// Pre-populate
		for u := 0; u < usersCount; u++ {
			for i := 0; i < recordsPerUser; i++ {
				cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
			}
		}

		b.ResetTimer()
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < b.N/goroutines; i++ {
					userID := (gid + i) % usersCount
					switch i % 4 {
					case 0:
						cache.Set(userID, fmt.Sprintf("new-tx%d-%d", gid, i), i, now)
					case 1:
						cache.Get(userID, fmt.Sprintf("tx%d-%d", userID, i%recordsPerUser))
					case 2:
						cache.GetByKey(fmt.Sprintf("tx%d-%d", userID, i%recordsPerUser))
					case 3:
						cache.GetNewest(userID, 10)
					}
				}
			}(g)
		}
		wg.Wait()
	})

	b.Run("GetNewest", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		// Pre-populate
		for u := 0; u < usersCount; u++ {
			for i := 0; i < recordsPerUser; i++ {
				cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now.Add(-time.Duration(i)*time.Minute))
			}
		}

		b.ResetTimer()
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < b.N/goroutines; i++ {
					userID := (gid + i) % usersCount
					cache.GetNewest(userID, 20)
				}
			}(g)
		}
		wg.Wait()
	})

	b.Run("Range", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		// Pre-populate with smaller dataset for range
		for u := 0; u < 50; u++ {
			for i := 0; i < 50; i++ {
				cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			cache.Range(func(userID int, key string, value int) bool {
				count++
				return true
			})
		}
	})

	b.Run("Len", func(b *testing.B) {
		cache := NewUserLFUCache[int, string, int](config)
		defer cache.Close()
		now := time.Now()

		// Pre-populate
		for u := 0; u < usersCount; u++ {
			for i := 0; i < recordsPerUser; i++ {
				cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
			}
		}

		b.ResetTimer()
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < b.N/goroutines; i++ {
					cache.Len()
				}
			}()
		}
		wg.Wait()
	})
}

// BenchmarkUserLFUCacheHighContention tests performance under high contention
func BenchmarkUserLFUCacheHighContention(b *testing.B) {
	const goroutines = 100
	const hotUsers = 10 // Only 10 users - high contention

	config := UserLFUCacheConfig{
		DefaultCachedCount: 100,
		TotalCapacity:      100_000,
		ShardCount:         256,
		SweepInterval:      time.Hour,
		CacheAgeDays:       30,
	}

	cache := NewUserLFUCache[int, string, int](config)
	defer cache.Close()
	now := time.Now()

	// Pre-populate hot users
	for u := 0; u < hotUsers; u++ {
		for i := 0; i < 100; i++ {
			cache.Set(u, fmt.Sprintf("tx%d-%d", u, i), i, now)
		}
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < b.N/goroutines; i++ {
				userID := i % hotUsers // All goroutines hit same users
				switch i % 3 {
				case 0:
					cache.Set(userID, fmt.Sprintf("hot-tx%d-%d", gid, i), i, now)
				case 1:
					cache.Get(userID, fmt.Sprintf("tx%d-%d", userID, i%100))
				case 2:
					cache.GetNewest(userID, 10)
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkUserLFUCacheEviction tests performance with eviction pressure
func BenchmarkUserLFUCacheEviction(b *testing.B) {
	const goroutines = 40

	config := UserLFUCacheConfig{
		DefaultCachedCount: 10,
		TotalCapacity:      1000, // Small capacity to trigger evictions
		ShardCount:         64,
		SweepInterval:      time.Hour,
		CacheAgeDays:       30,
	}

	cache := NewUserLFUCache[int, string, int](config)
	defer cache.Close()
	now := time.Now()

	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < b.N/goroutines; i++ {
				userID := gid
				key := fmt.Sprintf("tx%d-%d", gid, i)
				cache.Set(userID, key, i, now)
				// Immediately try to read - may or may not be evicted
				cache.Get(userID, key)
			}
		}(g)
	}
	wg.Wait()
}
