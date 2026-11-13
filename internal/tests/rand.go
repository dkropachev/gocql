package tests

import (
	"math/rand"
	"sync"
)

// ThreadSafeRand provides a concurrency-safe wrapper around math/rand.Rand.
// It allows safe usage of random number generation methods from multiple goroutines.
// All access to the underlying rand.Rand is synchronized via a mutex.
type ThreadSafeRand struct {
	r   *rand.Rand
	mux sync.Mutex
}

// NewThreadSafeRand creates and returns a new instance of ThreadSafeRand,
// initialized with the given seed. The resulting generator is safe for concurrent use.
func NewThreadSafeRand(seed int64) *ThreadSafeRand {
	return &ThreadSafeRand{
		r: rand.New(rand.NewSource(seed)),
	}
}

func (r *ThreadSafeRand) Intn(n int) int {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.r.Intn(n)
}

func (r *ThreadSafeRand) Int63() int64 {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.r.Int63()
}

const randCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomText(size int) string {
	result := make([]byte, size)
	for i := range result {
		result[i] = randCharset[rand.Intn(len(randCharset))]
	}
	return string(result)
}
