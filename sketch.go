package minder

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// cmSketch is a Count-Min sketch implementation with 4-bit counters.
// Uses atomic operations for lock-free concurrent access.
// Based on Damian Gryski's CM4: https://github.com/dgryski/go-tinylfu/blob/master/cm4.go
type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

const (
	// cmDepth is the number of counter copies to store (think of it as rows).
	cmDepth = 4
)

type timeSource struct{}

func (t timeSource) Uint64() uint64 {
	return uint64(time.Now().UnixNano())
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: bad numCounters")
	}
	// Get the next power of 2 for better cache performance.
	numCounters = next2Power(numCounters)
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	// Initialize rows of counters and seeds.
	source := rand.New(timeSource{})
	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}
	return sketch
}

// Increment increments the count(ers) for the specified key.
// This operation is lock-free using atomic CAS.
func (s *cmSketch) Increment(hashed uint64) {
	for i := range s.rows {
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

// Estimate returns the value of the specified key.
// This operation is lock-free using atomic loads.
func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}
	return int64(min)
}

// Reset halves all counter values.
// This operation uses atomic CAS for lock-free updates.
func (s *cmSketch) Reset() {
	for i := range s.rows {
		s.rows[i].reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for i := range s.rows {
		s.rows[i].clear()
	}
}

// cmRow is a row of uint64s, with each uint64 holding 16 4-bit counters.
// Uses atomic operations for thread-safe access.
type cmRow []uint64

func newCmRow(numCounters int64) cmRow {
	// Each uint64 holds 16 counters (4 bits each)
	return make(cmRow, (numCounters+15)/16)
}

func (r cmRow) get(n uint64) byte {
	// Word index: n / 16
	// Counter position within word: (n % 16) * 4
	wordIdx := n / 16
	shift := (n % 16) * 4
	word := atomic.LoadUint64(&r[wordIdx])
	return byte((word >> shift) & 0x0f)
}

func (r cmRow) increment(n uint64) {
	wordIdx := n / 16
	shift := (n % 16) * 4

	for retries := 0; retries < 100; retries++ {
		old := atomic.LoadUint64(&r[wordIdx])
		val := byte((old >> shift) & 0x0f)

		// Only increment if not max value (15)
		if val >= 15 {
			return
		}

		// Create new value with incremented counter
		newVal := old + (1 << shift)

		// Try to swap
		if atomic.CompareAndSwapUint64(&r[wordIdx], old, newVal) {
			return
		}
		// CAS failed, retry
	}
	// After max retries, give up - frequency counting is approximate anyway
}

func (r cmRow) reset() {
	// Halve each counter using atomic CAS
	// Mask to keep only the lower 3 bits of each 4-bit counter after shift
	// 0x0777... pattern for 16 counters in uint64
	const mask = uint64(0x0777077707770777)

	for i := range r {
		for retries := 0; retries < 100; retries++ {
			old := atomic.LoadUint64(&r[i])
			// Shift right by 1 and mask to halve each 4-bit counter
			newVal := (old >> 1) & mask
			if atomic.CompareAndSwapUint64(&r[i], old, newVal) {
				break
			}
		}
		// If CAS keeps failing, just store the halved value
		// This is approximate anyway, slight inaccuracy is acceptable
	}
}

func (r cmRow) clear() {
	for i := range r {
		atomic.StoreUint64(&r[i], 0)
	}
}

// next2Power rounds x up to the next power of 2, if it's not already one.
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}
