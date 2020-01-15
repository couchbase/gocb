package gocb

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/pkg/errors"
)

// Cluster represents a connection to a specific Couchbase cluster.
type Cluster struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	connectionsLock sync.RWMutex
	connections     map[string]client
	clusterClient   client

	clusterLock sync.RWMutex
	queryCache  map[string]*queryCacheEntry

	sb stateBlock

	supportsEnhancedStatements int32

	supportsGCCCP bool
}

// IoConfig specifies IO related configuration options.
type IoConfig struct {
	DisableMutationTokens  bool
	DisableServerDurations bool
}

// TimeoutsConfig specifies options for various operation timeouts.
type TimeoutsConfig struct {
	ConnectTimeout    time.Duration
	KVTimeout         time.Duration
	ViewTimeout       time.Duration
	QueryTimeout      time.Duration
	AnalyticsTimeout  time.Duration
	SearchTimeout     time.Duration
	ManagementTimeout time.Duration
}

// OrphanReporterConfig specifies options for controlling the orphan
// reporter which records when the SDK receives responses for requests
// that are no longer in the system (usually due to being timed out).
type OrphanReporterConfig struct {
	Disabled       bool
	ReportInterval time.Duration
	SampleSize     int
}

// ClusterOptions is the set of options available for creating a Cluster.
type ClusterOptions struct {
	// Authenticator specifies the authenticator to use with the cluster.
	Authenticator Authenticator

	// Timeouts specifies various operation timeouts.
	TimeoutsConfig TimeoutsConfig

	// Transcoder is used for trancoding data used in KV operations.
	Transcoder Transcoder

	// RetryStrategy is used to automatically retry operations if they fail.
	RetryStrategy RetryStrategy

	// Tracer specifies the tracer to use for requests.
	// VOLATILE: This API is subject to change at any time.
	Tracer requestTracer

	// OrphanReporterConfig specifies options for the orphan reporter.
	OrphanReporterConfig OrphanReporterConfig

	// CircuitBreakerConfig specifies options for the circuit breakers.
	CircuitBreakerConfig CircuitBreakerConfig

	// IoConfig specifies IO related configuration options.
	IoConfig IoConfig
}

// ClusterCloseOptions is the set of options available when
// disconnecting from a Cluster.
type ClusterCloseOptions struct {
}

// Connect creates and returns a Cluster instance created using the
// provided options and a connection string.
func Connect(connStr string, opts ClusterOptions) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	if connSpec.Scheme == "http" {
		return nil, errors.New("http scheme is not supported, use couchbase or couchbases instead")
	}

	connectTimeout := 10000 * time.Millisecond
	kvTimeout := 2500 * time.Millisecond
	viewTimeout := 75000 * time.Millisecond
	queryTimeout := 75000 * time.Millisecond
	analyticsTimeout := 75000 * time.Millisecond
	searchTimeout := 75000 * time.Millisecond
	managementTimeout := 75000 * time.Millisecond
	if opts.TimeoutsConfig.ConnectTimeout > 0 {
		connectTimeout = opts.TimeoutsConfig.ConnectTimeout
	}
	if opts.TimeoutsConfig.KVTimeout > 0 {
		kvTimeout = opts.TimeoutsConfig.KVTimeout
	}
	if opts.TimeoutsConfig.ViewTimeout > 0 {
		viewTimeout = opts.TimeoutsConfig.ViewTimeout
	}
	if opts.TimeoutsConfig.QueryTimeout > 0 {
		queryTimeout = opts.TimeoutsConfig.QueryTimeout
	}
	if opts.TimeoutsConfig.AnalyticsTimeout > 0 {
		analyticsTimeout = opts.TimeoutsConfig.AnalyticsTimeout
	}
	if opts.TimeoutsConfig.SearchTimeout > 0 {
		searchTimeout = opts.TimeoutsConfig.SearchTimeout
	}
	if opts.TimeoutsConfig.ManagementTimeout > 0 {
		managementTimeout = opts.TimeoutsConfig.SearchTimeout
	}
	if opts.Transcoder == nil {
		opts.Transcoder = NewJSONTranscoder()
	}
	if opts.RetryStrategy == nil {
		opts.RetryStrategy = NewBestEffortRetryStrategy(nil)
	}

	useMutationTokens := true
	useServerDurations := true
	if opts.IoConfig.DisableMutationTokens {
		useMutationTokens = false
	}
	if opts.IoConfig.DisableServerDurations {
		useServerDurations = false
	}

	var initialTracer requestTracer
	if opts.Tracer != nil {
		initialTracer = opts.Tracer
	} else {
		initialTracer = newThresholdLoggingTracer(nil)
	}
	tracerAddRef(initialTracer)

	cluster := &Cluster{
		cSpec:       connSpec,
		auth:        opts.Authenticator,
		connections: make(map[string]client),
		sb: stateBlock{
			ConnectTimeout:         connectTimeout,
			QueryTimeout:           queryTimeout,
			AnalyticsTimeout:       analyticsTimeout,
			SearchTimeout:          searchTimeout,
			ViewTimeout:            viewTimeout,
			KvTimeout:              kvTimeout,
			DuraTimeout:            40000 * time.Millisecond,
			DuraPollTimeout:        100 * time.Millisecond,
			Transcoder:             opts.Transcoder,
			UseMutationTokens:      useMutationTokens,
			ManagementTimeout:      managementTimeout,
			RetryStrategyWrapper:   newRetryStrategyWrapper(opts.RetryStrategy),
			OrphanLoggerEnabled:    !opts.OrphanReporterConfig.Disabled,
			OrphanLoggerInterval:   opts.OrphanReporterConfig.ReportInterval,
			OrphanLoggerSampleSize: opts.OrphanReporterConfig.SampleSize,
			UseServerDurations:     useServerDurations,
			Tracer:                 initialTracer,
			CircuitBreakerConfig:   opts.CircuitBreakerConfig,
		},

		queryCache: make(map[string]*queryCacheEntry),
	}

	err = cluster.parseExtraConnStrOptions(connSpec)
	if err != nil {
		return nil, err
	}

	csb := &clientStateBlock{
		BucketName: "",
	}
	cli := newClient(cluster, csb)
	err = cli.buildConfig()
	if err != nil {
		return nil, err
	}

	err = cli.connect()
	if err != nil {
		return nil, err
	}
	cluster.clusterClient = cli
	cluster.supportsGCCCP = cli.supportsGCCCP()

	return cluster, nil
}

func (c *Cluster) parseExtraConnStrOptions(spec gocbconnstr.ConnSpec) error {
	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	if valStr, ok := fetchOption("query_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("query_timeout option must be a number")
		}
		c.sb.QueryTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("analytics_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("analytics_timeout option must be a number")
		}
		c.sb.AnalyticsTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("search_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("search_timeout option must be a number")
		}
		c.sb.SearchTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("view_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("view_timeout option must be a number")
		}
		c.sb.ViewTimeout = time.Duration(val) * time.Millisecond
	}

	return nil
}

// Bucket connects the cluster to server(s) and returns a new Bucket instance.
func (c *Cluster) Bucket(bucketName string) *Bucket {
	b := newBucket(&c.sb, bucketName)
	cli := c.takeClusterClient()
	if cli == nil {
		// We've already taken the cluster client for a different bucket or something like that so
		// we need to connect a new client.
		cli = c.getClient(&b.sb.clientStateBlock)
		err := cli.buildConfig()
		if err == nil {
			err = cli.connect()
			if err != nil {
				cli.setBootstrapError(err)
			}
		} else {
			cli.setBootstrapError(err)
		}
	} else {
		err := cli.selectBucket(bucketName)
		if err != nil {
			cli.setBootstrapError(err)
		}
	}
	c.connectionsLock.Lock()
	c.connections[b.hash()] = cli
	c.connectionsLock.Unlock()
	b.cacheClient(cli)

	return b
}

func (c *Cluster) takeClusterClient() client {
	c.connectionsLock.Lock()
	defer c.connectionsLock.Unlock()

	if c.clusterClient != nil {
		cli := c.clusterClient
		c.clusterClient = nil
		return cli
	}

	return nil
}

func (c *Cluster) getClient(sb *clientStateBlock) client {
	c.connectionsLock.Lock()

	hash := sb.Hash()
	if cli, ok := c.connections[hash]; ok {
		c.connectionsLock.Unlock()
		return cli
	}
	c.connectionsLock.Unlock()

	cli := newClient(c, sb)

	return cli
}

func (c *Cluster) randomClient() (client, error) {
	c.connectionsLock.RLock()
	if len(c.connections) == 0 {
		c.connectionsLock.RUnlock()
		return nil, errors.New("not connected to cluster")
	}
	var randomClient client
	var firstError error
	for _, c := range c.connections { // This is ugly
		if c.connected() {
			randomClient = c
			break
		} else if firstError == nil {
			firstError = c.getBootstrapError()
		}
	}
	c.connectionsLock.RUnlock()
	if randomClient == nil {
		if firstError == nil {
			return nil, errors.New("not connected to cluster")
		}

		return nil, firstError
	}

	return randomClient, nil
}

func (c *Cluster) authenticator() Authenticator {
	return c.auth
}

func (c *Cluster) connSpec() gocbconnstr.ConnSpec {
	return c.cSpec
}

// Close shuts down all buckets in this cluster and invalidates any references this cluster has.
func (c *Cluster) Close(opts *ClusterCloseOptions) error {
	var overallErr error

	c.clusterLock.Lock()
	for key, conn := range c.connections {
		err := conn.close()
		if err != nil {
			logWarnf("Failed to close a client in cluster close: %s", err)
			overallErr = err
		}

		delete(c.connections, key)
	}
	if c.clusterClient != nil {
		err := c.clusterClient.close()
		if err != nil {
			logWarnf("Failed to close cluster client in cluster close: %s", err)
			overallErr = err
		}
	}
	c.clusterLock.Unlock()

	if c.sb.Tracer != nil {
		tracerDecRef(c.sb.Tracer)
		c.sb.Tracer = nil
	}

	return overallErr
}

func (c *Cluster) clusterOrRandomClient() (client, error) {
	var cli client
	c.connectionsLock.RLock()
	if c.clusterClient == nil {
		c.connectionsLock.RUnlock()
		var err error
		cli, err = c.randomClient()
		if err != nil {
			return nil, err
		}
	} else {
		cli = c.clusterClient
		c.connectionsLock.RUnlock()
		if !cli.supportsGCCCP() {
			return nil, errors.New("cluster-level operations not supported due to cluster version")
		}
	}

	return cli, nil
}

func (c *Cluster) getDiagnosticsProvider() (diagnosticsProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getDiagnosticsProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getQueryProvider() (queryProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getQueryProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getAnalyticsProvider() (analyticsProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getAnalyticsProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getSearchProvider() (searchProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getSearchProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getHTTPProvider() (httpProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) supportsEnhancedPreparedStatements() bool {
	return atomic.LoadInt32(&c.supportsEnhancedStatements) > 0
}

func (c *Cluster) setSupportsEnhancedPreparedStatements(supports bool) {
	if supports {
		atomic.StoreInt32(&c.supportsEnhancedStatements, 1)
	} else {
		atomic.StoreInt32(&c.supportsEnhancedStatements, 0)
	}
}

type clusterHTTPWrapper struct {
	c *Cluster
}

func (cw clusterHTTPWrapper) DoHTTPRequest(req *gocbcore.HTTPRequest) (*gocbcore.HTTPResponse, error) {
	provider, err := cw.c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return provider.DoHTTPRequest(req)
}

// Users returns a UserManager for managing users.
func (c *Cluster) Users() *UserManager {
	provider := clusterHTTPWrapper{c}

	return &UserManager{
		httpClient:           provider,
		globalTimeout:        c.sb.ManagementTimeout,
		defaultRetryStrategy: c.sb.RetryStrategyWrapper,
		tracer:               c.sb.Tracer,
	}
}

// Buckets returns a BucketManager for managing buckets.
func (c *Cluster) Buckets() *BucketManager {
	provider := clusterHTTPWrapper{c}

	return &BucketManager{
		httpClient:           provider,
		globalTimeout:        c.sb.ManagementTimeout,
		defaultRetryStrategy: c.sb.RetryStrategyWrapper,
		tracer:               c.sb.Tracer,
	}
}

// AnalyticsIndexes returns an AnalyticsIndexManager for managing analytics indexes.
func (c *Cluster) AnalyticsIndexes() *AnalyticsIndexManager {
	return &AnalyticsIndexManager{
		cluster: c,
		tracer:  c.sb.Tracer,
	}
}

// QueryIndexes returns a QueryIndexManager for managing query indexes.
func (c *Cluster) QueryIndexes() *QueryIndexManager {
	return &QueryIndexManager{
		cluster: c,
		tracer:  c.sb.Tracer,
	}
}

// SearchIndexes returns a SearchIndexManager for managing search indexes.
func (c *Cluster) SearchIndexes() *SearchIndexManager {
	return &SearchIndexManager{
		cluster: c,
		tracer:  c.sb.Tracer,
	}
}
