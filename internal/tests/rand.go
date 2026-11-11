package tests

import (
	"math/rand"
	"sync"
)

// RandInterface defines the thread-safe random number generator interface.
// It abstracts all methods provided by ThreadSafeRand.
type RandInterface interface {
	Uint64() uint64
	Uint32() uint32
	Int() int
	Intn(n int) int
	Int63() int64
	Int63n(n int64) int64
	Int31() int32
	Int31n(n int32) int32
	Float64() float64
	Float32() float32
	ExpFloat64() float64
	NormFloat64() float64
	Shuffle(n int, swap func(i, j int))
	Read(p []byte) (n int, err error)
}

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
