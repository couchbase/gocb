package counter

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

type Counter struct {
	count atomic.Int32
}

func NewCounter(initCount int32) *Counter {
	counter := &Counter{}
	counter.count.Store(initCount)
	return counter
}

func (c *Counter) GetAndIncrement() int32 {
	return c.count.Add(1)
}

func (c *Counter) GetAndDecrement() int32 {
	return c.count.Add(-1)
}

func (c *Counter) Get() int32 {
	return c.count.Load()
}

func (c *Counter) Set(newValue int32) {
	c.count.Store(newValue)
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
	defer c.lock.Unlock()

	if counter, ok := c.counters[sharedCounter.CounterId]; ok {
		return counter, nil
	}

	counter := NewCounter(sharedCounter.GetGlobal().GetCount())
	c.counters[sharedCounter.CounterId] = counter

	return counter, nil
}

func (c *Counters) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.counters = make(map[string]*Counter)
}
