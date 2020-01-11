package gocb

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// BucketType specifies the kind of bucket.
type BucketType string

const (
	// CouchbaseBucketType indicates a Couchbase bucket type.
	CouchbaseBucketType = BucketType("membase")

	// MemcachedBucketType indicates a Memcached bucket type.
	MemcachedBucketType = BucketType("memcached")

	// EphemeralBucketType indicates an Ephemeral bucket type.
	EphemeralBucketType = BucketType("ephemeral")
)

// ConflictResolutionType specifies the kind of conflict resolution to use for a bucket.
type ConflictResolutionType string

const (
	// ConflictResolutionTypeTimestamp specifies to use timestamp conflict resolution on the bucket.
	ConflictResolutionTypeTimestamp = ConflictResolutionType("lww")

	// ConflictResolutionTypeSequenceNumber specifies to use sequence number conflict resolution on the bucket.
	ConflictResolutionTypeSequenceNumber = ConflictResolutionType("seqno")
)

// EvictionPolicyType specifies the kind of eviction policy to use for a bucket.
type EvictionPolicyType string

const (
	// EvictionPolicyTypeFull specifies to use full eviction for a bucket.
	EvictionPolicyTypeFull = EvictionPolicyType("fullEviction")

	// EvictionPolicyTypeValueOnly specifies to use value only eviction for a bucket.
	EvictionPolicyTypeValueOnly = EvictionPolicyType("valueOnly")
)

// CompressionMode specifies the kind of compression to use for a bucket.
type CompressionMode string

const (
	// CompressionModeOff specifies to use no compression for a bucket.
	CompressionModeOff = CompressionMode("off")

	// CompressionModePassive specifies to use passive compression for a bucket.
	CompressionModePassive = CompressionMode("passive")

	// CompressionModeActive specifies to use active compression for a bucket.
	CompressionModeActive = CompressionMode("active")
)

type jsonBucketSettings struct {
	Name        string `json:"name"`
	Controllers struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
	ReplicaIndex bool `json:"replicaIndex"`
	Quota        struct {
		RAM    int `json:"ram"`
		RawRAM int `json:"rawRAM"`
	} `json:"quota"`
	ReplicaNumber          int    `json:"replicaNumber"`
	BucketType             string `json:"bucketType"`
	ConflictResolutionType string `json:"conflictResolutionType"`
	EvictionPolicy         string `json:"evictionPolicy"`
	MaxTTL                 int    `json:"maxTTL"`
	CompressionMode        string `json:"compressionMode"`
}

// BucketSettings holds information about the settings for a bucket.
type BucketSettings struct {
	// Name is the name of the bucket and is required.
	Name string
	// FlushEnabled specifies whether or not to enable flush on the bucket.
	FlushEnabled bool
	// ReplicaIndexDisabled specifies whether or not to disable replica index.
	ReplicaIndexDisabled bool // inverted so that zero value matches server default.
	//  is the memory quota to assign to the bucket and is required.
	RAMQuotaMB int
	// NumReplicas is the number of replicas servers per vbucket and is required.
	// NOTE: If not set this will set 0 replicas.
	NumReplicas int
	// BucketType is the type of bucket this is. Defaults to CouchbaseBucketType.
	BucketType      BucketType
	EvictionPolicy  EvictionPolicyType
	MaxTTL          int
	CompressionMode CompressionMode
}

func (bs *BucketSettings) fromData(data jsonBucketSettings) error {
	bs.Name = data.Name
	bs.FlushEnabled = data.Controllers.Flush != ""
	bs.ReplicaIndexDisabled = !data.ReplicaIndex
	bs.RAMQuotaMB = data.Quota.RawRAM / 1024 / 1024
	bs.NumReplicas = data.ReplicaNumber
	bs.EvictionPolicy = EvictionPolicyType(data.EvictionPolicy)
	bs.MaxTTL = data.MaxTTL
	bs.CompressionMode = CompressionMode(data.CompressionMode)

	switch data.BucketType {
	case "membase":
		bs.BucketType = CouchbaseBucketType
	case "memcached":
		bs.BucketType = MemcachedBucketType
	case "ephemeral":
		bs.BucketType = EphemeralBucketType
	default:
		return errors.New("unrecognized bucket type string")
	}

	return nil
}

// BucketManager provides methods for performing bucket management operations.
// See BucketManager for methods that allow creating and removing buckets themselves.
type BucketManager struct {
	httpClient           httpProvider
	globalTimeout        time.Duration
	defaultRetryStrategy *retryStrategyWrapper
	tracer               requestTracer
}

// GetBucketOptions is the set of options available to the bucket manager GetBucket operation.
type GetBucketOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetBucket returns settings for a bucket on the cluster.
func (bm *BucketManager) GetBucket(bucketName string, opts *GetBucketOptions) (*BucketSettings, error) {
	if opts == nil {
		opts = &GetBucketOptions{}
	}

	span := bm.tracer.StartSpan("GetBucket", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := bm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	return bm.get(span.Context(), bucketName, retryStrategy)
}

func (bm *BucketManager) get(tracectx requestSpanContext, bucketName string,
	strategy *retryStrategyWrapper) (*BucketSettings, error) {
	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s", bucketName),
		Method:        "GET",
		IsIdempotent:  true,
		RetryStrategy: strategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := bm.tracer.StartSpan("dispatch", tracectx)
	resp, err := bm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		return nil, makeHTTPBadStatusError("failed to get bucket", req, resp)
	}

	var bucketData jsonBucketSettings
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&bucketData)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	var settings BucketSettings
	err = settings.fromData(bucketData)
	if err != nil {
		return nil, err
	}

	return &settings, nil
}

// GetAllBucketsOptions is the set of options available to the bucket manager GetAll operation.
type GetAllBucketsOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAllBuckets returns a list of all active buckets on the cluster.
func (bm *BucketManager) GetAllBuckets(opts *GetAllBucketsOptions) (map[string]BucketSettings, error) {
	if opts == nil {
		opts = &GetAllBucketsOptions{}
	}

	span := bm.tracer.StartSpan("GetAllBuckets", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := bm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          "/pools/default/buckets",
		Method:        "GET",
		IsIdempotent:  true,
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := bm.tracer.StartSpan("dispatch", span.Context())
	resp, err := bm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		return nil, makeHTTPBadStatusError("failed to get all buckets", req, resp)
	}

	var bucketsData []*jsonBucketSettings
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&bucketsData)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	buckets := make(map[string]BucketSettings, len(bucketsData))
	for _, bucketData := range bucketsData {
		var bucket BucketSettings
		err := bucket.fromData(*bucketData)
		if err != nil {
			return nil, err
		}

		buckets[bucket.Name] = bucket
	}

	return buckets, nil
}

// CreateBucketSettings are the settings available when creating a bucket.
type CreateBucketSettings struct {
	BucketSettings
	ConflictResolutionType ConflictResolutionType
}

// CreateBucketOptions is the set of options available to the bucket manager CreateBucket operation.
type CreateBucketOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// CreateBucket creates a bucket on the cluster.
func (bm *BucketManager) CreateBucket(settings CreateBucketSettings, opts *CreateBucketOptions) error {
	if opts == nil {
		opts = &CreateBucketOptions{}
	}

	span := bm.tracer.StartSpan("CreateBucket", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := bm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts, err := bm.settingsToPostData(&settings.BucketSettings)
	if err != nil {
		return err
	}

	if settings.ConflictResolutionType != "" {
		posts.Add("conflictResolutionType", string(settings.ConflictResolutionType))
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          "/pools/default/buckets",
		Method:        "POST",
		Body:          []byte(posts.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := bm.tracer.StartSpan("dispatch", span.Context())
	resp, err := bm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 202 {
		return makeHTTPBadStatusError("failed to create bucket", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// UpdateBucketOptions is the set of options available to the bucket manager UpdateBucket operation.
type UpdateBucketOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// UpdateBucket updates a bucket on the cluster.
func (bm *BucketManager) UpdateBucket(settings BucketSettings, opts *UpdateBucketOptions) error {
	if opts == nil {
		opts = &UpdateBucketOptions{}
	}

	span := bm.tracer.StartSpan("UpdateBucket", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := bm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts, err := bm.settingsToPostData(&settings)
	if err != nil {
		return err
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s", settings.Name),
		Method:        "POST",
		Body:          []byte(posts.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := bm.tracer.StartSpan("dispatch", span.Context())
	resp, err := bm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		return makeHTTPBadStatusError("failed to update bucket", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// DropBucketOptions is the set of options available to the bucket manager DropBucket operation.
type DropBucketOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// DropBucket will delete a bucket from the cluster by name.
func (bm *BucketManager) DropBucket(name string, opts *DropBucketOptions) error {
	if opts == nil {
		opts = &DropBucketOptions{}
	}

	span := bm.tracer.StartSpan("DropBucket", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := bm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s", name),
		Method:        "DELETE",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := bm.tracer.StartSpan("dispatch", span.Context())
	resp, err := bm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		return makeHTTPBadStatusError("failed to drop bucket", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// FlushBucketOptions is the set of options available to the bucket manager FlushBucket operation.
type FlushBucketOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// FlushBucket will delete all the of the data from a bucket.
// Keep in mind that you must have flushing enabled in the buckets configuration.
func (bm *BucketManager) FlushBucket(name string, opts *FlushBucketOptions) error {
	if opts == nil {
		opts = &FlushBucketOptions{}
	}

	span := bm.tracer.StartSpan("FlushBucket", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := bm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/controller/doFlush", name),
		Method:        "POST",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := bm.tracer.StartSpan("dispatch", span.Context())
	resp, err := bm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		return makeHTTPBadStatusError("failed to flush bucket", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

func (bm *BucketManager) settingsToPostData(settings *BucketSettings) (url.Values, error) {
	posts := url.Values{}

	if settings.Name == "" {
		return nil, makeInvalidArgumentsError("Name invalid, must be set.")
	}

	if settings.RAMQuotaMB < 100 {
		return nil, makeInvalidArgumentsError("Memory quota invalid, must be greater than 100MB")
	}

	posts.Add("name", settings.Name)
	// posts.Add("saslPassword", settings.Password)

	if settings.FlushEnabled {
		posts.Add("flushEnabled", "1")
	} else {
		posts.Add("flushEnabled", "0")
	}

	if settings.ReplicaIndexDisabled {
		posts.Add("replicaIndex", "0")
	} else {
		posts.Add("replicaIndex", "1")
	}

	switch settings.BucketType {
	case CouchbaseBucketType:
		posts.Add("bucketType", string(settings.BucketType))
		posts.Add("replicaNumber", fmt.Sprintf("%d", settings.NumReplicas))
	case MemcachedBucketType:
		posts.Add("bucketType", string(settings.BucketType))
		if settings.NumReplicas > 0 {
			return nil, makeInvalidArgumentsError("replicas cannot be used with memcached buckets")
		}
	case EphemeralBucketType:
		posts.Add("bucketType", string(settings.BucketType))
		posts.Add("replicaNumber", fmt.Sprintf("%d", settings.NumReplicas))
	default:
		return nil, makeInvalidArgumentsError("Unrecognized bucket type")
	}

	posts.Add("ramQuotaMB", fmt.Sprintf("%d", settings.RAMQuotaMB))

	if settings.EvictionPolicy != "" {
		posts.Add("evictionPolicy", string(settings.EvictionPolicy))
	}

	if settings.MaxTTL > 0 {
		posts.Add("maxTTL", fmt.Sprintf("%d", settings.MaxTTL))
	}

	if settings.CompressionMode != "" {
		posts.Add("compressionMode", string(settings.CompressionMode))
	}

	return posts, nil
}
