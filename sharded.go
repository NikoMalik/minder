package minder

import (
	"hash/maphash"
	"math/bits"
	"time"

	"golang.org/x/sync/errgroup"
)

const SHARD_COUNT = 512
const SHARD_MASK = SHARD_COUNT - 1

const TOTAL = 1024_000

type ShardedCache[K comparable, V any] struct {
	shards      [SHARD_COUNT]*Cache[K, V]
	hashSeed    maphash.Seed
	stopCleanup chan struct{}
}

func NewShardedCache[K comparable, V any]() *ShardedCache[K, V] {
	sc := &ShardedCache[K, V]{
		hashSeed:    maphash.MakeSeed(),
		stopCleanup: make(chan struct{}),
	}
	perShard := TOTAL / SHARD_COUNT
	for i := 0; i < SHARD_COUNT; i++ {
		sc.shards[i] = NewCache[K, V](perShard)
	}
	go sc.cleanupRoutine()
	return sc
}

func fastRange(h uint64) uint64 {
	h64, _ := bits.Mul64(h, SHARD_COUNT)
	return h64
}

func (sc *ShardedCache[K, V]) mask(h uint64) uint64 {
	return fastRange(h >> 32)
}

func (sc *ShardedCache[K, V]) getShard(k K) *Cache[K, V] {
	hash := maphash.Comparable(sc.hashSeed, k)
	return sc.shards[sc.mask(hash)]
}

func (sc *ShardedCache[K, V]) Set(k K, v V) bool {
	return sc.getShard(k).Set(k, v)
}

func (sc *ShardedCache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) bool {
	return sc.getShard(k).SetWithTTL(k, v, ttl)
}

func (sc *ShardedCache[K, V]) Get(k K) (V, bool) {
	return sc.getShard(k).Get(k)
}

func (sc *ShardedCache[K, V]) GetTTL(k K) (time.Duration, bool) {
	return sc.getShard(k).GetTTL(k)
}

func (sc *ShardedCache[K, V]) Del(k K) {
	sc.getShard(k).Del(k)
}

// NOTE: scope in range not thread safe,use locks to change value when use Range()
func (sc *ShardedCache[K, V]) Range(f func(key K, value V) bool) {
	var eg errgroup.Group
	for i := 0; i < SHARD_COUNT; i++ {
		shard := sc.shards[i]
		eg.Go(func() error {
			shard.Range(f)
			return nil
		})
	}
	_ = eg.Wait()
}

func (sc *ShardedCache[K, V]) Len() int {
	var total int64
	for i := 0; i < SHARD_COUNT; i++ {
		total += sc.shards[i].validSize.Load()
	}
	return int(total)
}

func (sc *ShardedCache[K, V]) Clear() {
	var eg errgroup.Group
	for i := 0; i < SHARD_COUNT; i++ {
		shard := sc.shards[i]
		eg.Go(func() error {
			shard.Clear()
			return nil
		})
	}
	_ = eg.Wait()
}

func (sc *ShardedCache[K, V]) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var eg errgroup.Group
			for i := 0; i < SHARD_COUNT; i++ {
				shard := sc.shards[i]
				eg.Go(func() error {
					shard.cleanup()
					return nil
				})
			}
			_ = eg.Wait()
		case <-sc.stopCleanup:
			return
		}
	}
}

func (sc *ShardedCache[K, V]) Close() {
	close(sc.stopCleanup)
}
