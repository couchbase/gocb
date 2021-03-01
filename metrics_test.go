package gocb

import (
	"sync"
	"sync/atomic"
)

type testCounter struct {
	count uint64
}

func (tc *testCounter) IncrementBy(val uint64) {
	atomic.AddUint64(&tc.count, val)
}

type testValueRecorder struct {
	values []uint64
	lock   sync.Mutex
}

func (tvr *testValueRecorder) RecordValue(val uint64) {
	tvr.lock.Lock()
	tvr.values = append(tvr.values, val)
	tvr.lock.Unlock()
}

type testMeter struct {
	lock      sync.Mutex
	counters  map[string]*testCounter
	recorders map[string]*testValueRecorder
}

func newTestMeter() *testMeter {
	return &testMeter{
		counters:  make(map[string]*testCounter),
		recorders: make(map[string]*testValueRecorder),
	}
}

func (tm *testMeter) Reset() {
	tm.lock.Lock()
	tm.counters = make(map[string]*testCounter)
	tm.recorders = make(map[string]*testValueRecorder)
	tm.lock.Unlock()
}

func (tc *testMeter) Counter(name string, tags map[string]string) (Counter, error) {
	key := name + ":" + tags["db.operation"]
	tc.lock.Lock()
	counter := tc.counters[key]
	if counter == nil {
		counter = &testCounter{}
		tc.counters[key] = counter
	}
	tc.lock.Unlock()
	return counter, nil
}

func (tc *testMeter) ValueRecorder(name string, tags map[string]string) (ValueRecorder, error) {
	key := name + ":" + tags["db.couchbase.service"]
	if op, ok := tags["db.operation"]; ok {
		key = key + ":" + op
	}
	tc.lock.Lock()
	recorder := tc.recorders[key]
	if recorder == nil {
		recorder = &testValueRecorder{}
		tc.recorders[key] = recorder
	}
	tc.lock.Unlock()
	return recorder, nil
}
