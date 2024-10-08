package gocb

import (
	"errors"
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

type keyspace struct {
	bucketName     string
	scopeName      string
	collectionName string
}

func (mw *meterWrapper) ValueRecorder(service, operation string, keyspace *keyspace, operationErr error) (ValueRecorder, error) {
	if mw.isNoopMeter {
		// If it's a noop meter then let's not pay the overhead of creating attributes.
		return defaultNoopValueRecorder, nil
	}

	var labels gocbcore.ClusterLabels
	if mw.clusterLabelsProvider != nil {
		labels = mw.clusterLabelsProvider.ClusterLabels()
	}

	outcome := getStandardizedOutcome(operationErr)

	key := fmt.Sprintf("%s.%s.%s.%s.%s", service, operation, labels.ClusterUUID, labels.ClusterName, outcome)
	if keyspace == nil {
		key += "..."
	} else {
		key += fmt.Sprintf(".%s.%s.%s", keyspace.bucketName, keyspace.scopeName, keyspace.collectionName)
	}

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
			meterAttribOutcomeKey:   outcome,
		}
		if labels.ClusterName != "" {
			attribsMap[meterAttribClusterNameKey] = labels.ClusterName
		}
		if labels.ClusterUUID != "" {
			attribsMap[meterAttribClusterUUIDKey] = labels.ClusterUUID
		}
		if keyspace.bucketName != "" {
			attribsMap[meterAttribBucketNameKey] = keyspace.bucketName
		}
		if keyspace.scopeName != "" {
			attribsMap[meterAttribScopeNameKey] = keyspace.scopeName
		}
		if keyspace.collectionName != "" {
			attribsMap[meterAttribCollectionNameKey] = keyspace.collectionName
		}
		mw.attribsCache.Store(key, attribsMap)
	}

	recorder, err := mw.meter.ValueRecorder(meterNameCBOperations, attribsMap)
	if err != nil {
		return nil, err
	}

	return recorder, nil
}

func (mw *meterWrapper) ValueRecord(service, operation string, start time.Time, keyspace *keyspace, err error) {
	recorder, err := mw.ValueRecorder(service, operation, keyspace, err)
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

// getStandardizedOutcome returns the name for each error as listed in RFC#58 (Error Handling)
func getStandardizedOutcome(err error) string {
	if err == nil {
		return "Success"
	}
	if errors.Is(err, ErrUnambiguousTimeout) {
		return "UnambiguousTimeout"
	}
	if errors.Is(err, ErrAmbiguousTimeout) {
		return "AmbiguousTimeout"
	}
	if errors.Is(err, ErrTimeout) {
		return "Timeout"
	}
	if errors.Is(err, ErrRequestCanceled) {
		return "RequestCanceled"
	}
	if errors.Is(err, ErrInvalidArgument) {
		return "InvalidArgument"
	}
	if errors.Is(err, ErrServiceNotAvailable) {
		return "ServiceNotAvailable"
	}
	if errors.Is(err, ErrInternalServerFailure) {
		return "InternalServerFailure"
	}
	if errors.Is(err, ErrAuthenticationFailure) {
		return "AuthenticationFailure"
	}
	if errors.Is(err, ErrTemporaryFailure) {
		return "TemporaryFailure"
	}
	if errors.Is(err, ErrParsingFailure) {
		return "ParsingFailure"
	}
	if errors.Is(err, ErrCasMismatch) {
		return "CasMismatch"
	}
	if errors.Is(err, ErrBucketNotFound) {
		return "BucketNotFound"
	}
	if errors.Is(err, ErrCollectionNotFound) {
		return "CollectionNotFound"
	}
	if errors.Is(err, ErrUnsupportedOperation) {
		return "UnsupportedOperation"
	}
	if errors.Is(err, ErrFeatureNotAvailable) {
		return "FeatureNotAvailable"
	}
	if errors.Is(err, ErrScopeNotFound) {
		return "ScopeNotFound"
	}
	if errors.Is(err, ErrIndexNotFound) {
		return "IndexNotFound"
	}
	if errors.Is(err, ErrIndexExists) {
		return "IndexExists"
	}
	if errors.Is(err, ErrEncodingFailure) {
		return "EncodingFailure"
	}
	if errors.Is(err, ErrDecodingFailure) {
		return "DecodingFailure"
	}
	if errors.Is(err, ErrRateLimitedFailure) {
		return "RateLimited"
	}
	if errors.Is(err, ErrQuotaLimitedFailure) {
		return "QuotaLimited"
	}
	if errors.Is(err, ErrDocumentNotFound) {
		return "DocumentNotFound"
	}
	if errors.Is(err, ErrDocumentUnretrievable) {
		return "DocumentUnretrievable"
	}
	if errors.Is(err, ErrDocumentLocked) {
		return "DocumentLocked"
	}
	if errors.Is(err, ErrValueTooLarge) {
		return "ValueTooLarge"
	}
	if errors.Is(err, ErrDocumentExists) {
		return "DocumentExists"
	}
	if errors.Is(err, ErrDurabilityLevelNotAvailable) {
		return "DurabilityLevelNotAvailable"
	}
	if errors.Is(err, ErrDurabilityImpossible) {
		return "DurabilityImpossible"
	}
	if errors.Is(err, ErrDurabilityAmbiguous) {
		return "DurabilityAmbiguous"
	}
	if errors.Is(err, ErrDurableWriteInProgress) {
		return "DurableWriteInProgress"
	}
	if errors.Is(err, ErrDurableWriteReCommitInProgress) {
		return "DurableWriteReCommitInProgress"
	}
	if errors.Is(err, ErrPathNotFound) {
		return "PathNotFound"
	}
	if errors.Is(err, ErrPathMismatch) {
		return "PathMismatch"
	}
	if errors.Is(err, ErrPathInvalid) {
		return "PathInvalid"
	}
	if errors.Is(err, ErrPathTooBig) {
		return "PathTooBig"
	}
	if errors.Is(err, ErrPathTooDeep) {
		return "PathTooDeep"
	}
	if errors.Is(err, ErrDocumentTooDeep) {
		return "DocumentTooDeep"
	}
	if errors.Is(err, ErrValueTooDeep) {
		return "ValueTooDeep"
	}
	if errors.Is(err, ErrValueInvalid) {
		return "ValueInvalid"
	}
	if errors.Is(err, ErrDocumentNotJSON) {
		return "DocumentNotJson"
	}
	if errors.Is(err, ErrNumberTooBig) {
		return "NumberTooBig"
	}
	if errors.Is(err, ErrDeltaInvalid) {
		return "DeltaInvalid"
	}
	if errors.Is(err, ErrPathExists) {
		return "PathExists"
	}
	if errors.Is(err, ErrXattrUnknownMacro) {
		return "XattrUnknownMacro"
	}
	if errors.Is(err, ErrXattrInvalidKeyCombo) {
		return "XattrInvalidKeyCombo"
	}
	if errors.Is(err, ErrXattrUnknownVirtualAttribute) {
		return "XattrUnknownVirtualAttribute"
	}
	if errors.Is(err, ErrXattrCannotModifyVirtualAttribute) {
		return "XattrCannotModifyVirtualAttribute"
	}
	if errors.Is(err, ErrPlanningFailure) {
		return "PlanningFailure"
	}
	if errors.Is(err, ErrIndexFailure) {
		return "IndexFailure"
	}
	if errors.Is(err, ErrPreparedStatementFailure) {
		return "PreparedStatementFailure"
	}
	if errors.Is(err, ErrCompilationFailure) {
		return "CompilationFailure"
	}
	if errors.Is(err, ErrJobQueueFull) {
		return "JobQueueFull"
	}
	if errors.Is(err, ErrDatasetNotFound) {
		return "DatasetNotFound"
	}
	if errors.Is(err, ErrDataverseNotFound) {
		return "DataverseNotFound"
	}
	if errors.Is(err, ErrDatasetExists) {
		return "DatasetExists"
	}
	if errors.Is(err, ErrDataverseExists) {
		return "DataverseExists"
	}
	if errors.Is(err, ErrLinkNotFound) {
		return "LinkNotFound"
	}
	if errors.Is(err, ErrViewNotFound) {
		return "ViewNotFound"
	}
	if errors.Is(err, ErrDesignDocumentNotFound) {
		return "DesignDocumentNotFound"
	}
	if errors.Is(err, ErrCollectionExists) {
		return "CollectionExists"
	}
	if errors.Is(err, ErrScopeExists) {
		return "ScopeExists"
	}
	if errors.Is(err, ErrUserNotFound) {
		return "UserNotFound"
	}
	if errors.Is(err, ErrGroupNotFound) {
		return "GroupNotFound"
	}
	if errors.Is(err, ErrBucketExists) {
		return "BucketExists"
	}
	if errors.Is(err, ErrUserExists) {
		return "UserExists"
	}
	if errors.Is(err, ErrBucketNotFlushable) {
		return "BucketNotFlushable"
	}
	return "CouchbaseError"
}
