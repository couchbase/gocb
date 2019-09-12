package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

// BucketManager provides methods for performing bucket management operations.
// See BucketManager for methods that allow creating and removing buckets themselves.
// Volatile: This API is subject to change at any time.
type BucketManager struct {
	httpClient    httpProvider
	globalTimeout time.Duration
}

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

type bucketDataIn struct {
	Name        string `json:"name"`
	Controllers struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
	ReplicaIndex bool `json:"replicaIndex"`
	Quota        struct {
		Ram    int `json:"ram"`
		RawRam int `json:"rawRAM"`
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

// CreateBucketSettings are the settings available when creating a bucket.
type CreateBucketSettings struct {
	BucketSettings
	ConflictResolutionType ConflictResolutionType
}

func bucketDataInToSettings(bucketData *bucketDataIn) (string, BucketSettings) {
	settings := BucketSettings{
		Name: bucketData.Name,
		// Password:               bucketData.SaslPassword,
		FlushEnabled:         bucketData.Controllers.Flush != "",
		ReplicaIndexDisabled: !bucketData.ReplicaIndex,
		RAMQuotaMB:           bucketData.Quota.RawRam,
		NumReplicas:          bucketData.ReplicaNumber,
		EvictionPolicy:       EvictionPolicyType(bucketData.EvictionPolicy),
		MaxTTL:               bucketData.MaxTTL,
		CompressionMode:      CompressionMode(bucketData.CompressionMode),
	}

	if settings.RAMQuotaMB > 0 {
		settings.RAMQuotaMB = settings.RAMQuotaMB / 1024 / 1024
	}

	switch bucketData.BucketType {
	case "membase":
		settings.BucketType = CouchbaseBucketType
	case "memcached":
		settings.BucketType = MemcachedBucketType
	case "ephemeral":
		settings.BucketType = EphemeralBucketType
	default:
		logDebugf("Unrecognized bucket type string.")
	}

	return bucketData.Name, settings
}

func contextFromMaybeTimeout(ctx context.Context, timeout time.Duration, globalTimeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		// no operation level timeouts set, use global level
		timeout = globalTimeout
	}

	if ctx == nil {
		// no context provided so just make a new one
		return context.WithTimeout(context.Background(), timeout)
	}

	// a context has been provided so add whatever timeout to it. WithTimeout will pick the shortest anyway.
	return context.WithTimeout(ctx, timeout)
}

// GetBucketOptions is the set of options available to the bucket manager GetBucket operation.
type GetBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetBucket returns settings for a bucket on the cluster.
func (bm *BucketManager) GetBucket(bucketName string, opts *GetBucketOptions) (*BucketSettings, error) {
	if opts == nil {
		opts = &GetBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, bm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	return bm.get(ctx, bucketName)
}

func (bm *BucketManager) get(ctx context.Context, bucketName string) (*BucketSettings, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s", bucketName),
		Method:  "GET",
		Context: ctx,
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, bucketManagerError{message: string(data), statusCode: resp.StatusCode}
	}

	var bucketData *bucketDataIn
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&bucketData)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	_, settings := bucketDataInToSettings(bucketData)

	return &settings, nil
}

// GetAllBucketsOptions is the set of options available to the bucket manager GetAll operation.
type GetAllBucketsOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetAllBuckets returns a list of all active buckets on the cluster.
func (bm *BucketManager) GetAllBuckets(opts *GetAllBucketsOptions) (map[string]BucketSettings, error) {
	if opts == nil {
		opts = &GetAllBucketsOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, bm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    "/pools/default/buckets",
		Method:  "GET",
		Context: ctx,
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, bucketManagerError{message: string(data), statusCode: resp.StatusCode}
	}

	var bucketsData []*bucketDataIn
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
		name, settings := bucketDataInToSettings(bucketData)
		buckets[name] = settings
	}

	return buckets, nil
}

// CreateBucketOptions is the set of options available to the bucket manager CreateBucket operation.
type CreateBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// CreateBucket creates a bucket on the cluster.
func (bm *BucketManager) CreateBucket(settings CreateBucketSettings, opts *CreateBucketOptions) error {
	if opts == nil {
		opts = &CreateBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, bm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	posts, err := bm.settingsToPostData(&settings.BucketSettings)
	if err != nil {
		return err
	}

	if settings.ConflictResolutionType != "" {
		posts.Add("conflictResolutionType", string(settings.ConflictResolutionType))
	}

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        "/pools/default/buckets",
		Method:      "POST",
		Body:        []byte(posts.Encode()),
		ContentType: "application/x-www-form-urlencoded",
		Context:     ctx,
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 202 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		bodyMessage := string(data)
		return bucketManagerError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// UpdateBucketOptions is the set of options available to the bucket manager UpdateBucket operation.
type UpdateBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// UpdateBucket updates a bucket on the cluster.
func (bm *BucketManager) UpdateBucket(settings BucketSettings, opts *UpdateBucketOptions) error {
	if opts == nil {
		opts = &UpdateBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, bm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	posts, err := bm.settingsToPostData(&settings)
	if err != nil {
		return err
	}

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        fmt.Sprintf("/pools/default/buckets/%s", settings.Name),
		Method:      "POST",
		Body:        []byte(posts.Encode()),
		ContentType: "application/x-www-form-urlencoded",
		Context:     ctx,
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return bucketManagerError{message: string(data), statusCode: resp.StatusCode}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// DropBucketOptions is the set of options available to the bucket manager DropBucket operation.
type DropBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// DropBucket will delete a bucket from the cluster by name.
func (bm *BucketManager) DropBucket(name string, opts *DropBucketOptions) error {
	if opts == nil {
		opts = &DropBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, bm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s", name),
		Method:  "DELETE",
		Context: ctx,
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return bucketManagerError{message: string(data), statusCode: resp.StatusCode}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// FlushBucketOptions is the set of options available to the bucket manager FlushBucket operation.
type FlushBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// FlushBucket will delete all the of the data from a bucket.
// Keep in mind that you must have flushing enabled in the buckets configuration.
func (bm *BucketManager) FlushBucket(name string, opts *FlushBucketOptions) error {
	if opts == nil {
		opts = &FlushBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, bm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s/controller/doFlush", name),
		Method:  "POST",
		Context: ctx,
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return bucketManagerError{message: string(data), statusCode: resp.StatusCode}
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
		return nil, configurationError{message: "Name invalid, must be set."}
	}

	if settings.RAMQuotaMB < 100 {
		return nil, configurationError{message: "Memory quota invalid, must be greater than 100MB"}
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
			return nil, configurationError{message: "replicas cannot be used with memcached buckets"}
		}
	case EphemeralBucketType:
		posts.Add("bucketType", string(settings.BucketType))
		posts.Add("replicaNumber", fmt.Sprintf("%d", settings.NumReplicas))
	default:
		return nil, bucketManagerError{message: "Unrecognized bucket type"}
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
