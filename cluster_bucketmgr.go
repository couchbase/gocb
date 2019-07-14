package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

// BucketManager provides methods for performing bucket management operations.
// See BucketManager for methods that allow creating and removing buckets themselves.
type BucketManager struct {
	httpClient httpProvider
}

// BucketType specifies the kind of bucket.
type BucketType string

const (
	// Couchbase indicates a Couchbase bucket type.
	Couchbase = BucketType("couchbase")

	// Memcached indicates a Memcached bucket type.
	Memcached = BucketType("memcached")

	// Ephemeral indicates an Ephemeral bucket type.
	Ephemeral = BucketType("ephemeral")
)

// AuthType specifies the kind of auth to use for a bucket.
type AuthType string

const (
	// AuthTypeNone disables authentication.
	AuthTypeNone = AuthType("none")

	// AuthTypeSasl specifies to use sasl authentication.
	AuthTypeSasl = AuthType("sasl")
)

// ConflictResolutionType specifies the kind of conflict resolution to use for a bucket.
type ConflictResolutionType string

const (
	// ConflictResolutionTypeLww speficies to use lww conflict resolution on the bucket.
	ConflictResolutionTypeLww = ConflictResolutionType("lww")

	// ConflictResolutionTypeSeqNo speficies to use seqno conflict resolution on the bucket.
	ConflictResolutionTypeSeqNo = ConflictResolutionType("seqno")
)

// EvictionPolicyType specifies the kind of eviction policy to use for a bucket.
type EvictionPolicyType string

const (
	// EvictionPolicyTypeFull specifies to use full eviction for a bucket.
	EvictionPolicyTypeFull = ConflictResolutionType("fullEviction")

	// EvictionPolicyTypeValueOnly specifies to use value only eviction for a bucket.
	EvictionPolicyTypeValueOnly = ConflictResolutionType("valueOnly")
)

// CompressionMode specifies the kind of compression to use for a bucket.
type CompressionMode string

const (
	// CompressionModeOff specifies to use no compression for a bucket.
	CompressionModeOff = ConflictResolutionType("off")

	// CompressionModePassive specifies to use passive compression for a bucket.
	CompressionModePassive = ConflictResolutionType("passive")

	// CompressionModeActive specifies to use active compression for a bucket.
	CompressionModeActive = ConflictResolutionType("active")
)

type bucketDataIn struct {
	Name         string `json:"name"`
	SaslPassword string `json:"saslPassword"`
	Controllers  struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
	ReplicaIndex bool `json:"replicaIndex"`
	Quota        struct {
		Ram    int `json:"ram"`
		RawRam int `json:"rawRAM"`
	} `json:"quota"`
	ReplicaNumber          int    `json:"replicaNumber"`
	BucketType             string `json:"bucketType"`
	AuthType               string `json:"authType"`
	ConflictResolutionType string `json:"conflictResolutionType"`
	EvictionPolicy         string `json:"evictionPolicy"`
	MaxTTL                 int    `json:"maxTTL"`
	CompressionMode        string `json:"compressionMode"`
	ProxyPort              int    `json:"proxyPort"`
}

// BucketSettings holds information about the settings for a bucket.
type BucketSettings struct {
	Name                   string
	Password               string
	FlushEnabled           bool
	ReplicaIndexDisabled   bool // inverted so that zero value matches server default.
	RAMQuotaMB             int
	NumReplicas            int
	BucketType             BucketType
	AuthType               AuthType
	ConflictResolutionType ConflictResolutionType
	EvictionPolicy         EvictionPolicyType
	MaxTTL                 int
	CompressionMode        CompressionMode
	ProxyPort              int
}

func bucketDataInToSettings(bucketData *bucketDataIn) (string, BucketSettings) {
	settings := BucketSettings{
		Name:                   bucketData.Name,
		Password:               bucketData.SaslPassword,
		FlushEnabled:           bucketData.Controllers.Flush != "",
		ReplicaIndexDisabled:   !bucketData.ReplicaIndex,
		RAMQuotaMB:             bucketData.Quota.Ram,
		NumReplicas:            bucketData.ReplicaNumber,
		AuthType:               AuthType(bucketData.AuthType),
		ConflictResolutionType: ConflictResolutionType(bucketData.ConflictResolutionType),
		EvictionPolicy:         EvictionPolicyType(bucketData.EvictionPolicy),
		MaxTTL:                 bucketData.MaxTTL,
		CompressionMode:        CompressionMode(bucketData.CompressionMode),
		ProxyPort:              bucketData.ProxyPort,
	}

	switch bucketData.BucketType {
	case "membase":
		settings.BucketType = Couchbase
	case "memcached":
		settings.BucketType = Memcached
	case "ephemeral":
		settings.BucketType = Ephemeral
	default:
		// LogDebugf("Unrecognized bucket type string.") TODO: log something here
	}

	if bucketData.AuthType != string(AuthTypeSasl) {
		settings.Password = ""
	}

	return bucketData.Name, settings
}

// GetBucketOptions is the set of options available to the bucket manager Get operation.
type GetBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

func contextFromMaybeTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}

	return ctx, cancel
}

// Get returns settings for a bucket on the cluster.
func (bm *BucketManager) Get(bucketName string, opts *GetBucketOptions) (*BucketSettings, error) {
	if opts == nil {
		opts = &GetBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

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
		return nil, bucketManagerError{message: string(data), statusCode: resp.StatusCode, bucketMissing: resp.StatusCode == 404}
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

// GetAllBucketOptions is the set of options available to the bucket manager GetAll operation.
type GetAllBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetAll returns a list of all active buckets on the cluster.
func (bm *BucketManager) GetAll(opts *GetAllBucketOptions) (map[string]BucketSettings, error) {
	if opts == nil {
		opts = &GetAllBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
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

// CreateBucketOptions is the set of options available to the bucket manager Create operation.
type CreateBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Create creates a bucket on the cluster.
func (bm *BucketManager) Create(settings BucketSettings, opts *CreateBucketOptions) error {
	if opts == nil {
		opts = &CreateBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	data, err := bm.settingsToPostData(&settings)
	if err != nil {
		return err
	}

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        "/pools/default/buckets",
		Method:      "POST",
		Body:        data,
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
			message:      bodyMessage,
			statusCode:   resp.StatusCode,
			bucketExists: strings.Contains(bodyMessage, "already exist"),
		}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// UpsertBucketOptions is the set of options available to the bucket manager Upsert operation.
type UpsertBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Upsert creates or updates a bucket on the cluster.
func (bm *BucketManager) Upsert(settings BucketSettings, opts *UpsertBucketOptions) error {
	if opts == nil {
		opts = &UpsertBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	data, err := bm.settingsToPostData(&settings)
	if err != nil {
		return err
	}

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        fmt.Sprintf("/pools/default/buckets/%s", settings.Name),
		Method:      "POST",
		Body:        data,
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

// DropBucketOptions is the set of options available to the bucket manager Drop operation.
type DropBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Drop will delete a bucket from the cluster by name.
func (bm *BucketManager) Drop(name string, opts *DropBucketOptions) error {
	if opts == nil {
		opts = &DropBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
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
		return bucketManagerError{message: string(data), statusCode: resp.StatusCode, bucketMissing: resp.StatusCode == 404}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// FlushBucketOptions is the set of options available to the bucket manager Flush operation.
type FlushBucketOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Flush will delete all the of the data from a bucket.
// Keep in mind that you must have flushing enabled in the buckets configuration.
func (bm *BucketManager) Flush(name string, opts *FlushBucketOptions) error {
	if opts == nil {
		opts = &FlushBucketOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
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
		return bucketManagerError{message: string(data), statusCode: resp.StatusCode, bucketMissing: resp.StatusCode == 404}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

func (bm *BucketManager) settingsToPostData(settings *BucketSettings) ([]byte, error) {
	posts := url.Values{}

	posts.Add("name", settings.Name)
	posts.Add("saslPassword", settings.Password)

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

	if settings.RAMQuotaMB == 0 {
		return nil, bucketManagerError{message: "Memory quota invalid"}
	}

	posts.Add("replicaNumber", fmt.Sprintf("%d", settings.NumReplicas))

	switch settings.BucketType {
	case Couchbase:
		posts.Add("bucketType", "couchbase")
	case Memcached:
		posts.Add("bucketType", "memcached")
	case Ephemeral:
		posts.Add("bucketType", "ephemeral")
	default:
		return nil, bucketManagerError{message: "Unrecognized bucket type"}
	}

	if settings.AuthType == "" {
		posts.Add("authType", "sasl")
	} else {
		posts.Add("authType", string(settings.AuthType))
	}

	posts.Add("ramQuotaMB", fmt.Sprintf("%d", settings.RAMQuotaMB))

	if settings.ConflictResolutionType != "" {
		posts.Add("conflictResolutionType", string(settings.ConflictResolutionType))
	}

	if settings.EvictionPolicy != "" {
		posts.Add("evictionPolicy", string(settings.EvictionPolicy))
	}

	if settings.MaxTTL > 0 {
		posts.Add("maxTTL", fmt.Sprintf("%d", settings.MaxTTL))
	}

	if settings.CompressionMode != "" {
		posts.Add("compressionMode", string(settings.CompressionMode))
	}

	if settings.ProxyPort > 0 {
		posts.Add("proxyPort", fmt.Sprintf("%d", settings.ProxyPort))
	}

	return []byte(posts.Encode()), nil
}
