package gocb

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v8"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

// Cluster represents a connection to a specific Couchbase cluster.
type Cluster struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	connectionsLock sync.RWMutex
	connections     map[string]client

	clusterLock sync.RWMutex
	queryCache  map[string]*n1qlCache

	sb stateBlock
}

// ClusterOptions is the set of options available for creating a Cluster.
type ClusterOptions struct {
	Authenticator     Authenticator
	ConnectTimeout    time.Duration
	KVTimeout         time.Duration
	ViewTimeout       time.Duration
	QueryTimeout      time.Duration
	AnalyticsTimeout  time.Duration
	SearchTimeout     time.Duration
	ManagementTimeout time.Duration
	EnableTracing     bool
}

// ClusterCloseOptions is the set of options available when disconnecting from a Cluster.
type ClusterCloseOptions struct {
}

// NewCluster creates and returns a Cluster instance created using the provided options and connection string.
// The connection string properties are copied from (and should stay in sync with) the gocbcore agent.FromConnStr comment.
// Supported connSpecStr options are:
//   cacertpath (string) - Path to the CA certificate
//   certpath (string) - Path to your authentication certificate
//   keypath (string) - Path to your authentication key
//   config_total_timeout (int) - Maximum period to attempt to connect to cluster in ms.
//   config_node_timeout (int) - Maximum period to attempt to connect to a node in ms.
//   http_redial_period (int) - Maximum period to keep HTTP config connections open in ms.
//   http_retry_delay (int) - Period to wait between retrying nodes for HTTP config in ms.
//   config_poll_floor_interval (int) - Minimum time to wait between fetching configs via CCCP in ms.
//   config_poll_interval (int) - Period to wait between CCCP config polling in ms.
//   kv_pool_size (int) - The number of connections to establish per node.
//   max_queue_size (int) - The maximum size of the operation queues per node.
//   use_kverrmaps (bool) - Whether to enable error maps from the server.
//   use_enhanced_errors (bool) - Whether to enable enhanced error information.
//   fetch_mutation_tokens (bool) - Whether to fetch mutation tokens for operations.
//   compression (bool) - Whether to enable network-wise compression of documents.
//   compression_min_size (int) - The minimal size of the document to consider compression.
//   compression_min_ratio (float64) - The minimal compress ratio (compressed / original) for the document to be sent compressed.
//   server_duration (bool) - Whether to enable fetching server operation durations.
//   http_max_idle_conns (int) - Maximum number of idle http connections in the pool.
//   http_max_idle_conns_per_host (int) - Maximum number of idle http connections in the pool per host.
//   http_idle_conn_timeout (int) - Maximum length of time for an idle connection to stay in the pool in ms.
//   network (string) - The network type to use.
//   orphaned_response_logging (bool) - Whether to enable orphan response logging.
//   orphaned_response_logging_interval (int) - How often to log orphan responses in ms.
//   orphaned_response_logging_sample_size (int) - The number of samples to include in each orphaned response log.
//   operation_tracing (bool) - Whether to enable tracing.
//   n1ql_timeout (int) - Maximum execution time for n1ql queries in ms.
//   fts_timeout (int) - Maximum execution time for fts searches in ms.
//   analytics_timeout (int) - Maximum execution time for analytics queries in ms.
func NewCluster(connStr string, opts ClusterOptions) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	connectTimeout := 10000 * time.Millisecond
	kvTimeout := 2500 * time.Millisecond
	viewTimeout := 75000 * time.Millisecond
	queryTimeout := 75000 * time.Millisecond
	analyticsTimeout := 75000 * time.Millisecond
	searchTimeout := 75000 * time.Millisecond
	if opts.ConnectTimeout > 0 {
		connectTimeout = opts.ConnectTimeout
	}
	if opts.KVTimeout > 0 {
		kvTimeout = opts.KVTimeout
	}
	if opts.ViewTimeout > 0 {
		viewTimeout = opts.ViewTimeout
	}
	if opts.QueryTimeout > 0 {
		queryTimeout = opts.QueryTimeout
	}
	if opts.AnalyticsTimeout > 0 {
		analyticsTimeout = opts.AnalyticsTimeout
	}
	if opts.SearchTimeout > 0 {
		searchTimeout = opts.SearchTimeout
	}

	cluster := &Cluster{
		cSpec:       connSpec,
		auth:        opts.Authenticator,
		connections: make(map[string]client),
		sb: stateBlock{
			ConnectTimeout:         connectTimeout,
			N1qlRetryBehavior:      StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, ExponentialDelayFunction),
			AnalyticsRetryBehavior: StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, ExponentialDelayFunction),
			SearchRetryBehavior:    StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, ExponentialDelayFunction),
			QueryTimeout:           queryTimeout,
			AnalyticsTimeout:       analyticsTimeout,
			SearchTimeout:          searchTimeout,
			ViewTimeout:            viewTimeout,
			KvTimeout:              kvTimeout,
			DuraTimeout:            40000 * time.Millisecond,
			DuraPollTimeout:        100 * time.Millisecond,
		},

		queryCache: make(map[string]*n1qlCache),
	}

	cluster.sb.client = cluster.getClient

	err = cluster.parseExtraConnStrOptions(connSpec)
	if err != nil {
		return nil, err
	}

	if !opentracing.IsGlobalTracerRegistered() && opts.EnableTracing {
		// TODO: we'd add threshold logging here
		opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	}

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

	if valStr, ok := fetchOption("n1ql_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("n1ql_timeout option must be a number")
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
func (c *Cluster) Bucket(bucketName string, opts *BucketOptions) *Bucket {
	if opts == nil {
		opts = &BucketOptions{}
	}
	b := newBucket(&c.sb, bucketName, *opts)
	b.connect()
	return b
}

func (c *Cluster) getClient(sb *clientStateBlock) client {
	c.connectionsLock.Lock()
	defer c.connectionsLock.Unlock()

	hash := sb.Hash()
	if cli, ok := c.connections[hash]; ok {
		return cli
	}

	cli := newClient(c, sb)
	c.connections[hash] = cli

	return cli
}

func (c *Cluster) randomClient() (client, error) {
	c.connectionsLock.RLock()
	if len(c.connections) == 0 {
		c.connectionsLock.RUnlock()
		return nil, nil // TODO: return an error
	}
	var randomClient client
	for _, c := range c.connections { // This is ugly
		randomClient = c
		break
	}
	c.connectionsLock.RUnlock()
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
		if err != nil && gocbcore.ErrorCause(err) != gocbcore.ErrShutdown {
			logWarnf("Failed to close a client in cluster close: %s", err)
			overallErr = err
		}

		delete(c.connections, key)
	}
	c.clusterLock.Unlock()

	return overallErr
}

func (c *Cluster) getHTTPProvider() (httpProvider, error) {
	client, err := c.randomClient()
	if err != nil {
		return nil, err
	}

	provider, err := client.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}
