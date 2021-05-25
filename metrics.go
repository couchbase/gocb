package gocb

import (
	"github.com/couchbase/gocbcore/v9"
	"time"
)

// Meter handles metrics information for SDK operations.
type Meter interface {
	Counter(name string, tags map[string]string) (Counter, error)
	ValueRecorder(name string, tags map[string]string) (ValueRecorder, error)
}

// Counter is used for incrementing a synchronous count metric.
type Counter interface {
	IncrementBy(num uint64)
}

// ValueRecorder is used for grouping synchronous count metrics.
type ValueRecorder interface {
	RecordValue(val uint64)
}

// NoopMeter is a Meter implementation which performs no metrics operations.
type NoopMeter struct {
}

var (
	defaultNoopCounter       = &noopCounter{}
	defaultNoopValueRecorder = &noopValueRecorder{}
)

// Counter is used for incrementing a synchronous count metric.
func (nm *NoopMeter) Counter(name string, tags map[string]string) (Counter, error) {
	return defaultNoopCounter, nil
}

// ValueRecorder is used for grouping synchronous count metrics.
func (nm *NoopMeter) ValueRecorder(name string, tags map[string]string) (ValueRecorder, error) {
	return defaultNoopValueRecorder, nil
}

type noopCounter struct{}

func (bc *noopCounter) IncrementBy(num uint64) {
}

type noopValueRecorder struct{}

func (bc *noopValueRecorder) RecordValue(val uint64) {
}

// nolint: unused
type coreMeterWrapper struct {
	meter Meter
}

// nolint: unused
func (meter *coreMeterWrapper) Counter(name string, tags map[string]string) (gocbcore.Counter, error) {
	counter, err := meter.meter.Counter(name, tags)
	if err != nil {
		return nil, err
	}
	return &coreCounterWrapper{
		counter: counter,
	}, nil
}

// nolint: unused
func (meter *coreMeterWrapper) ValueRecorder(name string, tags map[string]string) (gocbcore.ValueRecorder, error) {
	if name == "db.couchbase.requests" {
		// gocbcore has its own requests metrics, we don't want to record those.
		return &noopValueRecorder{}, nil
	}

	recorder, err := meter.meter.ValueRecorder(name, tags)
	if err != nil {
		return nil, err
	}
	return &coreValueRecorderWrapper{
		valueRecorder: recorder,
	}, nil
}

// nolint: unused
type coreCounterWrapper struct {
	counter Counter
}

// nolint: unused
func (nm *coreCounterWrapper) IncrementBy(num uint64) {
	nm.counter.IncrementBy(num)
}

// nolint: unused
type coreValueRecorderWrapper struct {
	valueRecorder ValueRecorder
}

// nolint: unused
func (nm *coreValueRecorderWrapper) RecordValue(val uint64) {
	nm.valueRecorder.RecordValue(val)
}

func valueRecord(meter Meter, service, operation string, start time.Time) {
	recorder, err := meter.ValueRecorder(meterNameCBOperations, map[string]string{
		meterAttribServiceKey:   service,
		meterAttribOperationKey: operation,
	})
	if err != nil {
		logDebugf("Failed to create value recorder: %v", err)
		return
	}

	recorder.RecordValue(uint64(time.Since(start).Microseconds()))
}
