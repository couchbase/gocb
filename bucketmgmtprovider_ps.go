package gocb

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"

	"google.golang.org/grpc/status"
)

type bucketManagementProviderPs struct {
	provider admin_bucket_v1.BucketAdminServiceClient

	defaultTimeout time.Duration
	tracer         RequestTracer
	meter          *meterWrapper
}

func (bm bucketManagementProviderPs) GetBucket(bucketName string, opts *GetBucketOptions) (*BucketSettings, error) {
	start := time.Now()
	defer bm.meter.ValueRecord(meterValueServiceManagement, "manager_bucket_get_bucket", start)

	span := createSpan(bm.tracer, opts.ParentSpan, "manager_bucket_get_bucket", "management")
	span.SetAttribute("db.name", bucketName)
	span.SetAttribute("db.operation", "ListBuckets")
	defer span.End()

	req := &admin_bucket_v1.ListBucketsRequest{}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = bm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := bm.provider.ListBuckets(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, true)
		if gocbErr == nil {
			gocbErr = err
		}

		return nil, makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	for _, source := range resp.Buckets {
		if source.BucketName == bucketName {
			bucket, err := bm.psBucketToBucket(source)
			if err != nil {
				return nil, makeGenericMgmtError(err, nil, nil, err.Error())
			}

			return bucket, nil
		}
	}

	return nil, makeGenericMgmtError(ErrBucketNotFound, nil, nil, ErrBucketNotFound.Error())
}

func (bm bucketManagementProviderPs) GetAllBuckets(opts *GetAllBucketsOptions) (map[string]BucketSettings, error) {
	start := time.Now()
	defer bm.meter.ValueRecord(meterValueServiceManagement, "manager_bucket_get_all_buckets", start)

	span := createSpan(bm.tracer, opts.ParentSpan, "manager_bucket_get_all_buckets", "management")
	span.SetAttribute("db.operation", "ListBuckets")
	defer span.End()

	req := &admin_bucket_v1.ListBucketsRequest{}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = bm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := bm.provider.ListBuckets(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, true)
		if gocbErr == nil {
			gocbErr = err
		}

		return nil, makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	buckets := make(map[string]BucketSettings)
	for _, source := range resp.Buckets {
		bucket, err := bm.psBucketToBucket(source)
		if err != nil {
			return nil, makeGenericMgmtError(err, nil, nil, err.Error())
		}

		buckets[bucket.Name] = *bucket
	}

	return buckets, nil
}

func (bm bucketManagementProviderPs) CreateBucket(settings CreateBucketSettings, opts *CreateBucketOptions) error {
	start := time.Now()
	defer bm.meter.ValueRecord(meterValueServiceManagement, "manager_bucket_create_bucket", start)

	span := createSpan(bm.tracer, opts.ParentSpan, "manager_bucket_create_bucket", "management")
	span.SetAttribute("db.name", settings.Name)
	span.SetAttribute("db.operation", "CreateBucket")
	defer span.End()

	req, err := bm.settingsToCreateReq(settings)
	if err != nil {
		return makeGenericMgmtError(err, nil, nil, err.Error())
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = bm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = bm.provider.CreateBucket(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}

func (bm bucketManagementProviderPs) UpdateBucket(settings BucketSettings, opts *UpdateBucketOptions) error {
	start := time.Now()
	defer bm.meter.ValueRecord(meterValueServiceManagement, "manager_bucket_update_bucket", start)

	span := createSpan(bm.tracer, opts.ParentSpan, "manager_bucket_update_bucket", "management")
	span.SetAttribute("db.name", settings.Name)
	span.SetAttribute("db.operation", "UpdateBucket")
	defer span.End()

	req, err := bm.settingsToUpdateReq(settings)
	if err != nil {
		return makeGenericMgmtError(err, nil, nil, err.Error())
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = bm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = bm.provider.UpdateBucket(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}

func (bm bucketManagementProviderPs) DropBucket(name string, opts *DropBucketOptions) error {
	start := time.Now()
	defer bm.meter.ValueRecord(meterValueServiceManagement, "manager_bucket_drop_bucket", start)

	span := createSpan(bm.tracer, opts.ParentSpan, "manager_bucket_drop_bucket", "management")
	span.SetAttribute("db.name", name)
	span.SetAttribute("db.operation", "DeleteBucket")
	defer span.End()

	req := &admin_bucket_v1.DeleteBucketRequest{BucketName: name}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = bm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := bm.provider.DeleteBucket(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}

func (bm bucketManagementProviderPs) FlushBucket(name string, opts *FlushBucketOptions) error {
	return ErrFeatureNotAvailable
}

func (bm bucketManagementProviderPs) psBucketToBucket(source *admin_bucket_v1.ListBucketsResponse_Bucket) (*BucketSettings, error) {
	bucket := &BucketSettings{
		Name:                 source.BucketName,
		FlushEnabled:         source.FlushEnabled,
		ReplicaIndexDisabled: !source.ReplicaIndexes,
		RAMQuotaMB:           source.RamQuotaMb,
		NumReplicas:          source.NumReplicas,
		MaxExpiry:            time.Duration(source.MaxExpirySecs) * time.Second,
	}

	switch source.BucketType {
	case admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE:
		bucket.BucketType = CouchbaseBucketType
	case admin_bucket_v1.BucketType_BUCKET_TYPE_MEMCACHED:
		bucket.BucketType = MemcachedBucketType
	case admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL:
		bucket.BucketType = EphemeralBucketType
	default:
		return nil, fmt.Errorf("unrecognized bucket type %s", source.BucketType)
	}

	switch source.EvictionMode {
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL:
		bucket.EvictionPolicy = EvictionPolicyTypeFull
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED:
		bucket.EvictionPolicy = EvictionPolicyTypeNotRecentlyUsed
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY:
		bucket.EvictionPolicy = EvictionPolicyTypeValueOnly
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_NONE:
		bucket.EvictionPolicy = EvictionPolicyTypeNoEviction
	}

	switch source.CompressionMode {
	case admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF:
		bucket.CompressionMode = CompressionModeOff
	case admin_bucket_v1.CompressionMode_COMPRESSION_MODE_PASSIVE:
		bucket.CompressionMode = CompressionModePassive
	case admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE:
		bucket.CompressionMode = CompressionModeActive
	}

	if source.MinimumDurabilityLevel == nil {
		bucket.MinimumDurabilityLevel = DurabilityLevelNone
	} else {
		switch source.GetMinimumDurabilityLevel() {
		case kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY:
			bucket.MinimumDurabilityLevel = DurabilityLevelMajority
		case kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
			bucket.MinimumDurabilityLevel = DurabilityLevelMajorityAndPersistOnMaster
		case kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
			bucket.MinimumDurabilityLevel = DurabilityLevelPersistToMajority
		}
	}

	switch source.StorageBackend {
	case admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE:
		bucket.StorageBackend = StorageBackendCouchstore
	case admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA:
		bucket.StorageBackend = StorageBackendMagma
	}

	return bucket, nil
}

func (bm *bucketManagementProviderPs) settingsToCreateReq(settings CreateBucketSettings) (*admin_bucket_v1.CreateBucketRequest, error) {
	request := &admin_bucket_v1.CreateBucketRequest{}

	err := bm.validateSettings(settings.BucketSettings)
	if err != nil {
		return nil, err
	}

	request.BucketName = settings.Name
	request.NumReplicas = &settings.NumReplicas
	request.RamQuotaMb = &settings.RAMQuotaMB

	if settings.FlushEnabled {
		request.FlushEnabled = &settings.FlushEnabled
	}

	if settings.ReplicaIndexDisabled {
		replicasEnabled := false
		request.ReplicaIndexes = &replicasEnabled
	}

	request.BucketType, err = bm.bucketTypeToPS(settings.BucketType)
	if err != nil {
		return nil, err
	}

	if settings.EvictionPolicy != "" {
		request.EvictionMode, err = bm.evictionPolicyToPS(settings.EvictionPolicy, settings.BucketType)
		if err != nil {
			return nil, err
		}
	}

	if settings.MaxTTL > 0 {
		expiry := uint32(settings.MaxTTL.Seconds())
		request.MaxExpirySecs = &expiry
	}
	if settings.MaxExpiry > 0 {
		expiry := uint32(settings.MaxExpiry.Seconds())
		request.MaxExpirySecs = &expiry
	}

	if settings.CompressionMode != "" {
		request.CompressionMode, err = bm.compressionModeToPS(settings.CompressionMode)
		if err != nil {
			return nil, err
		}
	}

	if settings.MinimumDurabilityLevel > DurabilityLevelNone {
		request.MinimumDurabilityLevel, err = bm.durabilityLevelToPS(settings.MinimumDurabilityLevel)
		if err != nil {
			return nil, err
		}
	}

	if settings.StorageBackend != "" {
		request.StorageBackend, err = bm.storageBackendToPS(settings.StorageBackend)
		if err != nil {
			return nil, err
		}
	}

	if settings.ConflictResolutionType != "" {
		var conflictRes admin_bucket_v1.ConflictResolutionType
		switch settings.ConflictResolutionType {
		case ConflictResolutionTypeTimestamp:
			conflictRes = admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP
		case ConflictResolutionTypeSequenceNumber:
			conflictRes = admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER
		case ConflictResolutionTypeCustom:
			conflictRes = admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_CUSTOM
		default:
			return nil, makeInvalidArgumentsError(fmt.Sprintf("unrecognized conflict resolution type %s", settings.ConflictResolutionType))
		}
		request.ConflictResolutionType = &conflictRes
	}

	return request, nil
}

func (bm *bucketManagementProviderPs) settingsToUpdateReq(settings BucketSettings) (*admin_bucket_v1.UpdateBucketRequest, error) {
	request := &admin_bucket_v1.UpdateBucketRequest{}

	err := bm.validateSettings(settings)
	if err != nil {
		return nil, err
	}

	request.BucketName = settings.Name
	request.NumReplicas = &settings.NumReplicas
	request.RamQuotaMb = &settings.RAMQuotaMB

	if settings.FlushEnabled {
		request.FlushEnabled = &settings.FlushEnabled
	}

	if settings.ReplicaIndexDisabled {
		replicasEnabled := false
		request.ReplicaIndexes = &replicasEnabled
	}

	if settings.EvictionPolicy != "" {
		request.EvictionMode, err = bm.evictionPolicyToPS(settings.EvictionPolicy, settings.BucketType)
		if err != nil {
			return nil, err
		}
	}

	if settings.MaxTTL > 0 {
		expiry := uint32(settings.MaxTTL.Seconds())
		request.MaxExpirySecs = &expiry
	}
	if settings.MaxExpiry > 0 {
		expiry := uint32(settings.MaxExpiry.Seconds())
		request.MaxExpirySecs = &expiry
	}

	if settings.CompressionMode != "" {
		request.CompressionMode, err = bm.compressionModeToPS(settings.CompressionMode)
		if err != nil {
			return nil, err
		}
	}

	if settings.MinimumDurabilityLevel > DurabilityLevelNone {
		request.MinimumDurabilityLevel, err = bm.durabilityLevelToPS(settings.MinimumDurabilityLevel)
		if err != nil {
			return nil, err
		}
	}

	return request, nil
}

func (bm *bucketManagementProviderPs) validateSettings(settings BucketSettings) error {
	if settings.Name == "" {
		return makeInvalidArgumentsError("Name invalid, must be set.")
	}
	if settings.RAMQuotaMB < 100 {
		return makeInvalidArgumentsError("Memory quota invalid, must be greater than 100MB")
	}
	if (settings.MaxTTL > 0 || settings.MaxExpiry > 0) && settings.BucketType == MemcachedBucketType {
		return makeInvalidArgumentsError("maxExpiry is not supported for memcached buckets")
	}
	if settings.BucketType == MemcachedBucketType && settings.NumReplicas > 0 {
		return makeInvalidArgumentsError("replicas cannot be used with memcached buckets")
	}

	return nil
}

func (bm *bucketManagementProviderPs) bucketTypeToPS(bucketType BucketType) (admin_bucket_v1.BucketType, error) {
	switch bucketType {
	case CouchbaseBucketType:
		return admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE, nil
	case MemcachedBucketType:
		return admin_bucket_v1.BucketType_BUCKET_TYPE_MEMCACHED, nil
	case EphemeralBucketType:
		return admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL, nil
	default:
		return 0, makeInvalidArgumentsError(fmt.Sprintf("unrecognized bucket type %s", bucketType))
	}
}

func (bm *bucketManagementProviderPs) evictionPolicyToPS(evictionPolicy EvictionPolicyType,
	bucketType BucketType) (*admin_bucket_v1.EvictionMode, error) {
	var policy admin_bucket_v1.EvictionMode
	switch bucketType {
	case MemcachedBucketType:
		return nil, makeInvalidArgumentsError("eviction policy is not valid for memcached buckets")
	case CouchbaseBucketType:
		switch evictionPolicy {
		case EvictionPolicyTypeNoEviction:
			return nil, makeInvalidArgumentsError("eviction policy is not valid for couchbase buckets")
		case EvictionPolicyTypeNotRecentlyUsed:
			return nil, makeInvalidArgumentsError("eviction policy is not valid for couchbase buckets")
		case EvictionPolicyTypeValueOnly:
			policy = admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY
		case EvictionPolicyTypeFull:
			policy = admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL
		default:
			return nil, makeInvalidArgumentsError(fmt.Sprintf("unrecognized eviction policy %s", evictionPolicy))

		}
	case EphemeralBucketType:
		switch evictionPolicy {
		case EvictionPolicyTypeNoEviction:
			policy = admin_bucket_v1.EvictionMode_EVICTION_MODE_NONE
		case EvictionPolicyTypeNotRecentlyUsed:
			policy = admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED
		case EvictionPolicyTypeValueOnly:
			return nil, makeInvalidArgumentsError("eviction policy is not valid for ephemeral buckets")
		case EvictionPolicyTypeFull:
			return nil, makeInvalidArgumentsError("eviction policy is not valid for ephemeral buckets")
		default:
			return nil, makeInvalidArgumentsError(fmt.Sprintf("unrecognized eviction policy %s", evictionPolicy))
		}
	}
	return &policy, nil
}

func (bm *bucketManagementProviderPs) compressionModeToPS(mode CompressionMode) (*admin_bucket_v1.CompressionMode, error) {
	var compressionMode admin_bucket_v1.CompressionMode
	switch mode {
	case CompressionModeOff:
		compressionMode = admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF
	case CompressionModePassive:
		compressionMode = admin_bucket_v1.CompressionMode_COMPRESSION_MODE_PASSIVE
	case CompressionModeActive:
		compressionMode = admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE
	default:
		return nil, makeInvalidArgumentsError(fmt.Sprintf("unrecognized compression mode %s", compressionMode))
	}
	return &compressionMode, nil
}

func (bm *bucketManagementProviderPs) durabilityLevelToPS(level DurabilityLevel) (*kv_v1.DurabilityLevel, error) {
	var duraLevel kv_v1.DurabilityLevel
	switch level {
	case DurabilityLevelMajority:
		duraLevel = kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY
	case DurabilityLevelMajorityAndPersistOnMaster:
		duraLevel = kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE
	case DurabilityLevelPersistToMajority:
		duraLevel = kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY
	default:
		return nil, makeInvalidArgumentsError(fmt.Sprintf("unrecognized durability level %d", level))
	}

	return &duraLevel, nil
}

func (bm *bucketManagementProviderPs) storageBackendToPS(storage StorageBackend) (*admin_bucket_v1.StorageBackend, error) {
	var backend admin_bucket_v1.StorageBackend
	switch storage {
	case StorageBackendCouchstore:
		backend = admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE
	case StorageBackendMagma:
		backend = admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA
	default:
		return nil, makeInvalidArgumentsError(fmt.Sprintf("unrecognized storage backend %s", storage))
	}

	return &backend, nil
}
