package gocb

import (
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"go.opentelemetry.io/otel/metric"
	"sync"
	"time"
)

// Meter handles metrics information for SDK operations.
type Meter interface {
	Counter(name string, tags map[string]string) (Counter, error)
	ValueRecorder(name string, tags map[string]string) (ValueRecorder, error)
}

type OtelAwareMeter interface {
	Wrapped() metric.Meter
	Provider() metric.MeterProvider
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

//nolint:unused
type coreMeterWrapper struct {
	meter Meter
}

//nolint:unused
func (meter *coreMeterWrapper) Counter(name string, tags map[string]string) (gocbcore.Counter, error) {
	counter, err := meter.meter.Counter(name, tags)
	if err != nil {
		return nil, err
	}
	return &coreCounterWrapper{
		counter: counter,
	}, nil
}

//nolint:unused
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

//nolint:unused
type coreCounterWrapper struct {
	counter Counter
}

//nolint:unused
func (nm *coreCounterWrapper) IncrementBy(num uint64) {
	nm.counter.IncrementBy(num)
}

//nolint:unused
type coreValueRecorderWrapper struct {
	valueRecorder ValueRecorder
}

//nolint:unused
func (nm *coreValueRecorderWrapper) RecordValue(val uint64) {
	nm.valueRecorder.RecordValue(val)
}

type meterWrapper struct {
	attribsCache          sync.Map
	meter                 Meter
	isNoopMeter           bool
	clusterLabelsProvider clusterLabelsProvider
}

func newMeterWrapper(meter Meter) *meterWrapper {
	_, ok := meter.(*NoopMeter)
	return &meterWrapper{
		meter:       meter,
		isNoopMeter: ok,
	}
}

func (mw *meterWrapper) ValueRecorder(service, operation string) (ValueRecorder, error) {
	if mw.isNoopMeter {
		// If it's a noop meter then let's not pay the overhead of creating and caching attributes.
		return defaultNoopValueRecorder, nil
	}

	var labels gocbcore.ClusterLabels
	if mw.clusterLabelsProvider != nil {
		labels = mw.clusterLabelsProvider.ClusterLabels()

	}

	key := fmt.Sprintf("%s.%s.%s.%s", service, operation, labels.ClusterUUID, labels.ClusterName)
	attribs, ok := mw.attribsCache.Load(key)

	var attribsMap map[string]string
	if ok {
		attribsMap, ok = attribs.(map[string]string)
	}
	if !ok {
		// It doesn't really matter if we end up storing the attribs against the same key multiple times. We just need
		// to have a read efficient cache that doesn't cause actual data races.
		attribsMap = map[string]string{
			meterAttribServiceKey:   service,
			meterAttribOperationKey: operation,
		}
		if labels.ClusterName != "" {
			attribsMap[meterAttribClusterNameKey] = labels.ClusterName
		}
		if labels.ClusterUUID != "" {
			attribsMap[meterAttribClusterUUIDKey] = labels.ClusterUUID
		}
		mw.attribsCache.Store(key, attribsMap)
	}

	recorder, err := mw.meter.ValueRecorder(meterNameCBOperations, attribsMap)
	if err != nil {
		return nil, err
	}

	return recorder, nil
}

func (mw *meterWrapper) ValueRecord(service, operation string, start time.Time) {
	recorder, err := mw.ValueRecorder(service, operation)
	if err != nil {
		logDebugf("Failed to create value recorder: %v", err)
		return
	}

	duration := uint64(time.Since(start).Microseconds())
	if duration == 0 {
		duration = uint64(1 * time.Microsecond)
	}

	recorder.RecordValue(duration)
}
