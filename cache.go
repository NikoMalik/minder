package minder

import (
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

type cacheItem[V any] struct {
	value V
	// expiration time.Time //zero means no expiration
	expiration int64
}

type Cache[K comparable, V any] struct {
	store       *xsync.Map[K, *cacheItem[V]]
	stopCleanup chan struct{}
	validSize   atomic.Int64
}

func NewCache[K comparable, V any]() *Cache[K, V] {
	c := &Cache[K, V]{
		store:       xsync.NewMap[K, *cacheItem[V]](xsync.WithPresize(1_000_000), xsync.WithGrowOnly()),
		stopCleanup: make(chan struct{}),
	}
	go c.cleanupRoutine()
	return c
}

func (c *Cache[K, V]) Cap() int {
	return c.store.Stats().Capacity
}

func (c *Cache[K, V]) Size() int {
	return c.store.Size()
}

func (c *Cache[K, V]) Len() int {
	return int(c.validSize.Load())
}

func (c *Cache[K, V]) ExpiredCount() int {
	return c.Size() - c.Len()
}

func (c *Cache[K, V]) Set(k K, v V) bool {
	item := &cacheItem[V]{value: v}
	var added bool
	c.store.Compute(k, func(old *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
		if !loaded {
			added = true
		}
		return item, xsync.UpdateOp
	})

	if added {
		c.validSize.Add(1)
	}
	return true
}

func (c *Cache[K, V]) SetOrGet(k K, v V) bool {
	item := &cacheItem[V]{value: v}
	_, loaded := c.store.LoadOrStore(k, item)
	if !loaded {
		c.validSize.Add(1)
		return true
	}
	return false

}

func (c *Cache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) bool {
	item := &cacheItem[V]{
		value:      v,
		expiration: time.Now().Add(ttl).UnixMilli(),
	}
	var added bool
	c.store.Compute(k, func(old *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
		if !loaded {
			added = true
		}
		return item, xsync.UpdateOp
	})
	if added {
		c.validSize.Add(1)
	}
	return true
}

func (c *Cache[K, V]) Range(f func(key K, value V) bool) {
	now := time.Now().UnixMilli()
	c.store.Range(func(k K, item *cacheItem[V]) bool {
		if item.expiration != 0 && now > item.expiration {
			c.store.Compute(k, func(oldItem *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
				if !loaded || oldItem != item {
					return nil, xsync.CancelOp
				}
				if now > oldItem.expiration {
					c.validSize.Add(-1)
					return nil, xsync.DeleteOp
				}
				return oldItem, xsync.CancelOp
			})
			return true
		}
		return f(k, item.value)
	})
}

func (c *Cache[K, V]) Get(k K) (V, bool) {
	item, ok := c.store.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	if item.expiration != 0 && time.Now().UnixMilli() > item.expiration {
		c.store.Compute(k, func(oldItem *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
			if !loaded || oldItem != item {
				return nil, xsync.CancelOp
			}
			if time.Now().UnixMilli() > oldItem.expiration {
				c.validSize.Add(-1)
				return nil, xsync.DeleteOp
			}
			return oldItem, xsync.CancelOp
		})
		var zero V
		return zero, false
	}
	return item.value, true
}

func (c *Cache[K, V]) Del(k K) {
	_, loaded := c.store.LoadAndDelete(k)
	if loaded {
		c.validSize.Add(-1)
	}
}
func (c *Cache[K, V]) Clear() {
	c.store.Clear()
	c.validSize.Store(0)
}

func (c *Cache[K, V]) GetTTL(k K) (time.Duration, bool) {
	item, ok := c.store.Load(k)
	if !ok {
		return 0, false
	}
	if item.expiration == 0 {
		return 0, false
	}
	now := time.Now().UnixMilli()
	if now > item.expiration {
		c.store.Compute(k, func(oldItem *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
			if !loaded || oldItem != item {
				return nil, xsync.CancelOp
			}
			if now > oldItem.expiration {
				c.validSize.Add(-1)
				return nil, xsync.DeleteOp
			}
			return oldItem, xsync.CancelOp
		})
		return 0, false
	}
	return time.Duration((item.expiration - now) * int64(time.Millisecond)), true
}

func (c *Cache[K, V]) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCleanup:
			return
		}
	}
}

func (c *Cache[K, V]) cleanup() {
	now := time.Now().UnixMilli()
	c.store.Range(func(k K, item *cacheItem[V]) bool {
		if item.expiration != 0 && now > item.expiration {
			c.store.Compute(k, func(oldItem *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
				if !loaded || oldItem != item {
					return nil, xsync.CancelOp
				}
				if now > oldItem.expiration {
					c.validSize.Add(-1)
					return nil, xsync.DeleteOp
				}
				return oldItem, xsync.CancelOp
			})
		}
		return true
	})
}

func (c *Cache[K, V]) Close() {
	close(c.stopCleanup)
}
