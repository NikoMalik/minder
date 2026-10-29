package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var modes = []struct {
	name string
	mode Mode
}{
	{"ttl", ModeTTL},
	{"lfu", ModeLFU},
	{"ttl+lfu", ModeTTL | ModeLFU},
}

func TestCacheBasic(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			c := New[string, int](Config{Mode: m.mode})
			defer c.Close()

			c.Set("a", 1)
			c.Set("a", 2)
			if v, ok := c.Get("a"); !ok || v != 2 {
				t.Fatalf("expected a=2, got ok=%v v=%v", ok, v)
			}
			if c.Len() != 1 {
				t.Fatalf("expected Len 1, got %d", c.Len())
			}

			c.Del("a")
			if _, ok := c.Get("a"); ok {
				t.Fatal("expected a to be deleted")
			}
			if c.Len() != 0 {
				t.Fatalf("expected Len 0, got %d", c.Len())
			}
		})
	}
}

func TestCacheTTL(t *testing.T) {
	for _, m := range modes {
		if m.mode&ModeTTL == 0 {
			continue
		}
		t.Run(m.name, func(t *testing.T) {
			c := New[string, int](Config{Mode: m.mode})
			defer c.Close()

			c.SetWithTTL("a", 1, 50*time.Millisecond)
			c.Set("b", 2)
			if ttl, ok := c.GetTTL("a"); !ok || ttl <= 0 {
				t.Fatalf("expected positive ttl, got ok=%v ttl=%v", ok, ttl)
			}
			if _, ok := c.GetTTL("b"); ok {
				t.Fatal("expected no ttl for b")
			}

			time.Sleep(100 * time.Millisecond)
			if _, ok := c.Get("a"); ok {
				t.Fatal("expected a to be expired")
			}
			if c.Len() != 1 {
				t.Fatalf("expected Len 1 after expiry, got %d", c.Len())
			}

			n := 0
			c.Range(func(string, int) bool {
				n++
				return true
			})
			if n != 1 {
				t.Fatalf("expected Range to see 1 item, got %d", n)
			}
		})
	}
}

func TestCacheConcurrent(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			c := New[int, int](Config{Mode: m.mode})
			defer c.Close()

			var wg sync.WaitGroup
			for g := 0; g < 16; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 500; i++ {
						k := g*500 + i
						c.Set(k, k)
						c.Get(k)
						if i%3 == 0 {
							c.Del(k)
						}
					}
				}(g)
			}
			wg.Wait()

			n := 0
			c.Range(func(int, int) bool {
				n++
				return true
			})
			if n != c.Len() {
				t.Fatalf("Range saw %d items, Len is %d", n, c.Len())
			}
		})
	}
}

func TestCacheLFUEviction(t *testing.T) {
	c := New[string, int](Config{Mode: ModeLFU, ShardCount: 512, MaxCost: 512})
	defer c.Close()

	for i := 0; i < 1000; i++ {
		c.Set(fmt.Sprintf("key%d", i), i)
	}
	if int64(c.Len()) > c.MaxCost() {
		t.Fatalf("expected Len <= %d, got %d", c.MaxCost(), c.Len())
	}
}

func TestCacheLFUKeepsHot(t *testing.T) {
	c := New[int, string](Config{Mode: ModeLFU, ShardCount: 512, MaxCost: 5120})
	defer c.Close()

	for i := 0; i < 100; i++ {
		c.Set(i, "hot")
	}
	for j := 0; j < 20; j++ {
		for i := 0; i < 100; i++ {
			c.Get(i)
		}
	}
	c.FlushAccessBuffers()

	for i := 100; i < 10000; i++ {
		c.Set(i, "cold")
	}

	hot := 0
	for i := 0; i < 100; i++ {
		if _, ok := c.Get(i); ok {
			hot++
		}
	}
	if hot < 30 {
		t.Fatalf("expected at least 30 hot items to survive, got %d", hot)
	}
}

func TestCacheTTLModeHasNoCap(t *testing.T) {
	c := New[int, int](Config{Mode: ModeTTL})
	defer c.Close()

	for i := 0; i < 10000; i++ {
		if !c.Set(i, i) {
			t.Fatalf("set %d rejected", i)
		}
	}
	if c.Len() != 10000 || c.MaxCost() != 0 {
		t.Fatalf("expected Len 10000 and MaxCost 0, got %d and %d", c.Len(), c.MaxCost())
	}
}

func TestCacheLFUOnlyIgnoresTTL(t *testing.T) {
	c := New[string, int](Config{Mode: ModeLFU})
	defer c.Close()

	c.SetWithTTL("a", 1, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("expected a=1 to stay without ttl, got ok=%v v=%v", ok, v)
	}
	if _, ok := c.GetTTL("a"); ok {
		t.Fatal("expected no ttl in ModeLFU")
	}
}

func TestCacheTTLAndLFUEvictsAndExpires(t *testing.T) {
	c := New[int, int](Config{Mode: ModeTTL | ModeLFU, ShardCount: 512, MaxCost: 512})
	defer c.Close()

	for i := 0; i < 1000; i++ {
		c.SetWithTTL(i, i, 50*time.Millisecond)
	}
	if int64(c.Len()) > c.MaxCost() {
		t.Fatalf("expected Len <= %d, got %d", c.MaxCost(), c.Len())
	}

	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 1000; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("expected %d to be expired", i)
		}
	}
	if c.Len() != 0 {
		t.Fatalf("expected Len 0 after expiry, got %d", c.Len())
	}
}

func TestCacheZeroModeIsTTL(t *testing.T) {
	c := New[string, int](Config{})
	defer c.Close()

	if c.Config().Mode != ModeTTL {
		t.Fatalf("expected ModeTTL by default, got %v", c.Config().Mode)
	}
}
