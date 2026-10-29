package cache

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tidwall/btree"
)

const benchTreeSize = 500_000

var benchDegrees = []int{16, 32, 64, 128, 256}

func benchTree(degree int) (*btree.BTreeG[*userItem[int, int]], []*userItem[int, int]) {
	tr := btree.NewBTreeGOptions(userItemLess[int, int], btree.Options{Degree: degree, NoLocks: true})
	items := make([]*userItem[int, int], benchTreeSize)
	for i := range items {
		items[i] = &userItem[int, int]{key: i, value: i, createdAt: int64(i), seq: uint64(i)}
		tr.Set(items[i])
	}
	return tr, items
}

func BenchmarkUserTree(b *testing.B) {
	for _, degree := range benchDegrees {
		b.Run(fmt.Sprintf("degree=%d/set-newest", degree), func(b *testing.B) {
			tr, _ := benchTree(degree)
			next := int64(benchTreeSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				next++
				tr.Set(&userItem[int, int]{key: int(next), createdAt: next, seq: uint64(next)})
			}
		})

		b.Run(fmt.Sprintf("degree=%d/set-newest-hint", degree), func(b *testing.B) {
			tr, _ := benchTree(degree)
			var hint btree.PathHint
			next := int64(benchTreeSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				next++
				tr.SetHint(&userItem[int, int]{key: int(next), createdAt: next, seq: uint64(next)}, &hint)
			}
		})

		b.Run(fmt.Sprintf("degree=%d/replace-random", degree), func(b *testing.B) {
			tr, items := benchTree(degree)
			rng := rand.New(rand.NewPCG(1, 2))
			seq := uint64(benchTreeSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := rng.IntN(benchTreeSize)
				old := items[j]
				tr.Delete(old)
				seq++
				items[j] = &userItem[int, int]{key: old.key, createdAt: old.createdAt, seq: seq}
				tr.Set(items[j])
			}
		})

		b.Run(fmt.Sprintf("degree=%d/page-random", degree), func(b *testing.B) {
			tr, _ := benchTree(degree)
			rng := rand.New(rand.NewPCG(1, 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pivot, _ := tr.GetAt(rng.IntN(benchTreeSize))
				n := 0
				tr.Ascend(pivot, func(*userItem[int, int]) bool {
					n++
					return n < 20
				})
			}
		})

		b.Run(fmt.Sprintf("degree=%d/scan-all", degree), func(b *testing.B) {
			tr, _ := benchTree(degree)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tr.Scan(func(*userItem[int, int]) bool {
					return true
				})
			}
		})
	}
}

func benchUserCache(cachedAgo time.Duration) *UserCache[int, int, int] {
	cfg := DefaultUserCacheConfig()
	cfg.SweepInterval = time.Hour
	c := NewUserCache[int, int, int](cfg)
	now := time.Now()
	c.write(1, func(s *userSet[int, int]) {
		for i := 0; i < benchTreeSize; i++ {
			c.insertLocked(1, s, newUserItem(i, i, now.Add(time.Duration(i)*time.Millisecond), 1, now.Add(-cachedAgo).UnixMilli()))
		}
	})
	return c
}

func BenchmarkUserCacheOneUser(b *testing.B) {
	for _, writePct := range []int{0, 1, 10, 50} {
		b.Run(fmt.Sprintf("writes=%d%%", writePct), func(b *testing.B) {
			c := benchUserCache(0)
			defer c.Close()
			var next atomic.Int64
			next.Store(benchTreeSize)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					if rng.IntN(100) < writePct {
						k := int(next.Add(1))
						c.Set(1, k, k, time.Now())
						continue
					}
					c.GetPage(1, rng.IntN(benchTreeSize), 20)
				}
			})
		})
	}
}

func BenchmarkUserCacheLongLocks(b *testing.B) {
	b.Run("sweep-500k-old", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			c := benchUserCache(31 * 24 * time.Hour)
			b.StartTimer()
			c.ForceSweep()
			c.Close()
		}
	})
	b.Run("get-all-sorted", func(b *testing.B) {
		c := benchUserCache(0)
		defer c.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.GetAllSorted(1)
		}
	})
}

func BenchmarkUserCacheSweepPause(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := benchUserCache(31 * 24 * time.Hour)
		var worst time.Duration
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.ForceSweep()
		}()
		b.StartTimer()
		for running := true; running; {
			select {
			case <-done:
				running = false
			default:
				start := time.Now()
				c.GetPage(1, 0, 20)
				worst = max(worst, time.Since(start))
			}
		}
		b.ReportMetric(float64(worst.Microseconds()), "worst-read-us")
		c.Close()
	}
}
