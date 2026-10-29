# MemoryCache
thread-safe cache with auto-delete

## Install

```sh
go get github.com/NikoMalik/minder
```

## Cache

One cache type for all modes, `Mode` is a set of flags:

- `ModeTTL` keeps items until TTL ends or `Del`, no size limit
- `ModeLFU` keeps the most used items within `MaxCost`, new items can be rejected when the cache is full, TTL is ignored
- `ModeTTL | ModeLFU` both: size limit by LFU and items expire by TTL

```go
import "github.com/NikoMalik/minder/cache"

c := cache.New[string, int](cache.Config{
	Mode:            cache.ModeTTL | cache.ModeLFU,
	ShardCount:      256,
	MaxCost:         1_000_000,
	CleanupInterval: 5 * time.Minute,
})
defer c.Close()

c.Set("a", 1)
c.SetWithTTL("b", 2, time.Minute)
c.SetWithCost("c", 3, 10)
c.SetWithTTLAndCost("d", 4, time.Minute, 10)

v, ok := c.Get("a")
ttl, ok := c.GetTTL("b")
c.Del("a")

c.Range(func(k string, v int) bool {
	return true
})

c.Len()
c.Cap()     // free cost left, 0 without ModeLFU
c.MaxCost() // 0 without ModeLFU
c.Clear()
```

Zero fields in `Config` take values from `cache.DefaultConfig()`, zero `Mode` means `ModeTTL`.
`Set` returns `false` only in `ModeLFU` when the item was not admitted.

## UserCache

Records grouped by user and sorted by `createdAt`, newest first.
Sweep keeps `DefaultCachedCount` newest records per user and removes older ones cached more than `CacheAgeDays` ago.

```go
uc := cache.NewUserCache[int64, string, Tx](cache.DefaultUserCacheConfig())
defer uc.Close()

uc.Set(userID, txID, tx, tx.CreatedAt)

got, ok := uc.Get(userID, txID)
got, ok = uc.GetByKey(txID)

page := uc.GetPage(userID, 40, 20)
newest := uc.GetNewest(userID, 10)

uc.LoadMore(userID, []cache.UserItem[string, Tx]{
	{Key: txID, Value: tx, CreatedAt: tx.CreatedAt},
})
uc.SetFullyLoaded(userID, true)

uc.Del(userID, txID)
uc.DelUser(userID)
```

## Deprecated

`NewCache`, `NewShardedCache`, `NewShardedLFUCache` and `NewUserCache` from the root package still work, but use `github.com/NikoMalik/minder/cache` for new code.
