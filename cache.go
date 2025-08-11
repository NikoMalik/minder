package cache

import (
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

const (
	iter0       = 1 << 3
	elementNum0 = 1 << 30
)

type cacheItem[V any] struct {
	value      V
	expiration time.Time //zero means no expiration
}

type Cache[K comparable, V any] struct {
	store       *xsync.Map[K, *cacheItem[V]]
	stopCleanup chan struct{}
}

func NewCache[K comparable, V any]() *Cache[K, V] {
	c := &Cache[K, V]{
		store:       xsync.NewMap[K, *cacheItem[V]](xsync.WithPresize(1_000_000), xsync.WithGrowOnly()),
		stopCleanup: make(chan struct{}),
	}
	go c.cleanupRoutine()
	return c
}

func (c *Cache[K, V]) Set(k K, v V) bool {
	item := &cacheItem[V]{
		value: v,
		// expiration.IsZero() means "always keep"
	}
	c.store.Store(k, item)
	return true
}

func (c *Cache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) bool {
	item := &cacheItem[V]{
		value:      v,
		expiration: time.Now().Add(ttl),
	}
	c.store.Store(k, item)
	return true
}

func (c *Cache[K, V]) Get(k K) (V, bool) {
	item, ok := c.store.Load(k)
	if !ok {
		var zero V
		return zero, false
	}

	// for ttl elements check time
	if !item.expiration.IsZero() && time.Now().After(item.expiration) {
		c.store.Compute(k, func(oldItem *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
			if !loaded || oldItem != item {
				return nil, xsync.CancelOp // element already done
			}
			if time.Now().After(oldItem.expiration) {
				return nil, xsync.DeleteOp // delete only if old
			}
			return oldItem, xsync.CancelOp
		})
		var zero V
		return zero, false
	}
	return item.value, true
}

func (c *Cache[K, V]) Del(k K) {
	c.store.Delete(k)
}

func (c *Cache[T, V]) Clear() {
	c.store.Clear()
}

func (c *Cache[K, V]) GetTTL(k K) (time.Duration, bool) {
	item, ok := c.store.Load(k)
	if !ok {
		return 0, false
	}

	if item.expiration.IsZero() {
		return 0, false
	}

	now := time.Now()
	if now.After(item.expiration) {
		return 0, false
	}
	return item.expiration.Sub(now), true
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
	now := time.Now()
	c.store.Range(func(k K, item *cacheItem[V]) bool {
		// delete only ttl with expuired
		if !item.expiration.IsZero() && now.After(item.expiration) {
			c.store.Compute(k, func(oldItem *cacheItem[V], loaded bool) (*cacheItem[V], xsync.ComputeOp) {
				if !loaded || oldItem != item {
					return nil, xsync.CancelOp
				}
				if now.After(oldItem.expiration) {
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
