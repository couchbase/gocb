package gocb

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"go.opentelemetry.io/otel/metric"
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

type meterAttributeCacheKey struct {
	service                string
	operation              string
	keyspace               keyspace
	outcome                string
	labels                 gocbcore.ClusterLabels
	usingStableConventions bool
}

func (k *meterAttributeCacheKey) createAttributeMap() map[string]string {
	if k.usingStableConventions {
		attribs := map[string]string{
			meterStableAttribSystemName:    meterAttribSystemNameValue,
			meterStableAttribService:       k.service,
			meterStableAttribOperationName: k.operation,
			meterReservedAttribUnit:        meterAttribUnitValueSeconds,
		}
		if k.outcome != "" {
			// The error.type field is omitted if the operation was successful
			attribs[meterStableAttribErrorType] = k.outcome
		}
		if k.labels.ClusterName != "" {
			attribs[meterStableAttribClusterName] = k.labels.ClusterName
		}
		if k.labels.ClusterUUID != "" {
			attribs[meterStableAttribClusterUUID] = k.labels.ClusterUUID
		}
		if k.keyspace.bucketName != "" {
			attribs[meterStableAttribBucketName] = k.keyspace.bucketName
		}
		if k.keyspace.scopeName != "" {
			attribs[meterStableAttribScopeName] = k.keyspace.scopeName
		}
		if k.keyspace.collectionName != "" {
			attribs[meterStableAttribCollectionName] = k.keyspace.collectionName
		}
		return attribs
	}
	attribs := map[string]string{
		meterLegacyAttribService:       k.service,
		meterLegacyAttribOperationName: k.operation,
		meterLegacyAttribOutcome:       k.outcome,
	}
	if k.labels.ClusterName != "" {
		attribs[meterLegacyAttribClusterName] = k.labels.ClusterName
	}
	if k.labels.ClusterUUID != "" {
		attribs[meterLegacyAttribClusterUUID] = k.labels.ClusterUUID
	}
	if k.keyspace.bucketName != "" {
		attribs[meterLegacyAttribBucketName] = k.keyspace.bucketName
	}
	if k.keyspace.scopeName != "" {
		attribs[meterLegacyAttribScopeName] = k.keyspace.scopeName
	}
	if k.keyspace.collectionName != "" {
		attribs[meterLegacyAttribCollectionName] = k.keyspace.collectionName
	}
	return attribs
}

type meterAttributeCache struct {
	cache sync.Map
}

func (c *meterAttributeCache) Get(key meterAttributeCacheKey) map[string]string {
	if attribs, ok := c.cache.Load(key); ok {
		if attribsMap, ok := attribs.(map[string]string); ok {
			return attribsMap
		}
	}

	attribsMap := key.createAttributeMap()

	// It doesn't really matter if we end up storing the attribs against the same key multiple times. We just need
	// to have a read efficient cache that doesn't cause actual data races.
	c.cache.Store(key, attribsMap)

	return attribsMap
}

type meterWrapper struct {
	cache                    meterAttributeCache
	meter                    Meter
	isNoopMeter              bool
	clusterLabelsProvider    clusterLabelsProvider
	includeLegacyConventions bool
	includeStableConventions bool
}

func newMeterWrapper(meter Meter, config ObservabilityConfig) *meterWrapper {
	var includeLegacy, includeStable bool
	if slices.Contains(config.SemanticConventionOptIn, ObservabilitySemanticConventionDatabaseDup) {
		includeLegacy = true
		includeStable = true
	} else if slices.Contains(config.SemanticConventionOptIn, ObservabilitySemanticConventionDatabase) {
		includeStable = true
	} else {
		includeLegacy = true
	}

	_, ok := meter.(*NoopMeter)
	return &meterWrapper{
		meter:                    meter,
		isNoopMeter:              ok,
		includeLegacyConventions: includeLegacy,
		includeStableConventions: includeStable,
	}
}

type keyspace struct {
	bucketName     string
	scopeName      string
	collectionName string
}

func (mw *meterWrapper) ValueRecorder(service, operation string, keyspace keyspace, operationErr error, usingStableConventions bool) (ValueRecorder, error) {
	if mw.isNoopMeter {
		// If it's a noop meter then let's not pay the overhead of creating attributes.
		return defaultNoopValueRecorder, nil
	}

	var labels gocbcore.ClusterLabels
	if mw.clusterLabelsProvider != nil {
		labels = mw.clusterLabelsProvider.ClusterLabels()
	}

	outcome := getStandardizedOutcome(operationErr, usingStableConventions)

	attribs := mw.cache.Get(meterAttributeCacheKey{
		service:                service,
		operation:              operation,
		keyspace:               keyspace,
		outcome:                outcome,
		labels:                 labels,
		usingStableConventions: usingStableConventions,
	})

	var meterName string
	if usingStableConventions {
		meterName = meterNameDBClientOperationDuration
	} else {
		meterName = meterNameCBOperations
	}

	recorder, err := mw.meter.ValueRecorder(meterName, attribs)
	if err != nil {
		return nil, err
	}

	return recorder, nil
}

func (mw *meterWrapper) valueRecordWithDuration(service, operation string, durationMicroseconds uint64, keyspace keyspace, err error) {
	if mw.includeLegacyConventions {
		recorder, err := mw.ValueRecorder(service, operation, keyspace, err, false)
		if err != nil {
			logDebugf("Failed to create value recorder: %v", err)
			return
		}

		recorder.RecordValue(durationMicroseconds)
	}

	if mw.includeStableConventions {
		recorder, err := mw.ValueRecorder(service, operation, keyspace, err, true)
		if err != nil {
			logDebugf("Failed to create value recorder: %v", err)
			return
		}

		recorder.RecordValue(durationMicroseconds)
	}
}

func (mw *meterWrapper) ValueRecord(service, operation string, start time.Time, keyspace keyspace, err error) {
	duration := uint64(time.Since(start).Microseconds())
	if duration == 0 {
		duration = uint64(1 * time.Microsecond)
	}

	mw.valueRecordWithDuration(service, operation, duration, keyspace, err)
}

// getStandardizedOutcome returns the name for each error as listed in RFC#58 (Error Handling)
func getStandardizedOutcome(err error, usingStableConventions bool) string {
	if err == nil {
		// No error/outcome field in the stable conventions if the operation was successful
		if usingStableConventions {
			return ""
		}
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
	if errors.Is(err, ErrAuthorizationFailure) {
		return "AuthorizationFailure"
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

	if usingStableConventions {
		return "_OTHER"
	}
	return "CouchbaseError"
}
