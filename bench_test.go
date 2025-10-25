package minder

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkCacheOperations(b *testing.B) {
	const totalItems = 100_0000
	const goroutines = 40

	benchCases := []struct {
		name      string
		cacheType string
	}{
		{"SingleCache", "single"},
		{"ShardedCache", "sharded"},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			var cache interface {
				Set(k string, v int) bool
				Get(k string) (int, bool)
				Close()
			}
			if bc.cacheType == "single" {
				cache = NewCache[string, int]()
			} else {
				cache = NewShardedCache[string, int]()
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
