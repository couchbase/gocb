package counter

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

type Counter struct {
	count int32
}

func NewCounter(initCount int32) *Counter {
	return &Counter{
		count: initCount,
	}
}

func (c *Counter) GetAndIncrement() int32 {
	return atomic.AddInt32(&c.count, 1)
}

func (c *Counter) GetAndDecrement() int32 {
	return atomic.AddInt32(&c.count, -1)
}

func (c *Counter) Get() int32 {
	return atomic.LoadInt32(&c.count)
}

type Counters struct {
	lock     sync.Mutex
	counters map[string]*Counter
}

func NewCounters() *Counters {
	return &Counters{
		counters: make(map[string]*Counter),
	}
}

func (c *Counters) Get(sharedCounter *shared.Counter) (*Counter, error) {
	if sharedCounter.GetGlobal() == nil {
		return nil, errors.New("unknown counter type")
	}

	c.lock.Lock()
	if counter, ok := c.counters[sharedCounter.CounterId]; ok {
		c.lock.Unlock()
		return counter, nil
	}

	initValue := sharedCounter.GetGlobal().Count
	counter := &Counter{
		count: initValue,
	}
	c.counters[sharedCounter.CounterId] = counter
	c.lock.Unlock()

	return counter, nil
}
