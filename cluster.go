package gocb

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"strconv"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v10"
	gocbconnstr "github.com/couchbase/gocbcore/v10/connstr"
)

// Cluster represents a connection to a specific Couchbase cluster.
type Cluster struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	connectionManager connectionManager

	useServerDurations bool
	useMutationTokens  bool

	timeoutsConfig TimeoutsConfig

	transcoder           Transcoder
	retryStrategyWrapper *retryStrategyWrapper

	orphanLoggerEnabled    bool
	orphanLoggerInterval   time.Duration
	orphanLoggerSampleSize uint32

	tracer RequestTracer
	meter  *meterWrapper

	circuitBreakerConfig CircuitBreakerConfig
	securityConfig       SecurityConfig
	internalConfig       InternalConfig
	transactionsConfig   TransactionsConfig
	compressionConfig    CompressionConfig

	transactions *Transactions
}

// IoConfig specifies IO related configuration options.
type IoConfig struct {
	DisableMutationTokens  bool
	DisableServerDurations bool
}

// TimeoutsConfig specifies options for various operation timeouts.
type TimeoutsConfig struct {
	ConnectTimeout   time.Duration
	KVTimeout        time.Duration
	KVDurableTimeout time.Duration
	// Volatile: This option is subject to change at any time.
	KVScanTimeout     time.Duration
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
	SampleSize     uint32
}

// SecurityConfig specifies options for controlling security related
// items such as TLS root certificates and verification skipping.
type SecurityConfig struct {
	TLSRootCAs    *x509.CertPool
	TLSSkipVerify bool

	// AllowedSaslMechanisms is the list of mechanisms that the SDK can use to attempt authentication.
	// Note that if you add PLAIN to the list, this will cause credential leakage on the network
	// since PLAIN sends the credentials in cleartext. It is disabled by default to prevent downgrade attacks. We
	// recommend using a TLS connection if using PLAIN.
	AllowedSaslMechanisms []SaslMechanism
}

// CompressionConfig specifies options for controlling compression applied to documents before sending to Couchbase
// Server.
type CompressionConfig struct {
	Disabled bool

	// MinSize specifies the minimum size of the document to consider compression.
	MinSize uint32
	// MinRatio specifies the minimal compress ratio (compressed / original) for the document to be sent compressed.
	MinRatio float64
}

// InternalConfig specifies options for controlling various internal
// items.
// Internal: This should never be used and is not supported.
type InternalConfig struct {
	TLSRootCAProvider    func() *x509.CertPool
	ConnectionBufferSize uint
}

// ClusterOptions is the set of options available for creating a Cluster.
type ClusterOptions struct {
	// Authenticator specifies the authenticator to use with the cluster.
	Authenticator Authenticator

	// Username & Password specifies the cluster username and password to
	// authenticate with.  This is equivalent to passing PasswordAuthenticator
	// as the Authenticator parameter with the same values.
	Username string
	Password string

	// Timeouts specifies various operation timeouts.
	TimeoutsConfig TimeoutsConfig

	// Transcoder is used for trancoding data used in KV operations.
	Transcoder Transcoder

	// RetryStrategy is used to automatically retry operations if they fail.
	RetryStrategy RetryStrategy

	// Tracer specifies the tracer to use for requests.
	Tracer RequestTracer

	Meter Meter

	// OrphanReporterConfig specifies options for the orphan reporter.
	OrphanReporterConfig OrphanReporterConfig

	// CircuitBreakerConfig specifies options for the circuit breakers.
	CircuitBreakerConfig CircuitBreakerConfig

	// IoConfig specifies IO related configuration options.
	IoConfig IoConfig

	// SecurityConfig specifies security related configuration options.
	SecurityConfig SecurityConfig

	// TransactionsConfig specifies transactions related configuration options.
	TransactionsConfig TransactionsConfig

	// CompressionConfig specifies compression related configuration options.
	CompressionConfig CompressionConfig

	// Internal: This should never be used and is not supported.
	InternalConfig InternalConfig
}

// ClusterCloseOptions is the set of options available when
// disconnecting from a Cluster.
type ClusterCloseOptions struct {
}

func clusterFromOptions(opts ClusterOptions) *Cluster {
	if opts.Authenticator == nil {
		opts.Authenticator = PasswordAuthenticator{
			Username: opts.Username,
			Password: opts.Password,
		}
	}

	connectTimeout := 10000 * time.Millisecond
	kvTimeout := 2500 * time.Millisecond
	kvDurableTimeout := 10000 * time.Millisecond
	kvScanTimeout := 10000 * time.Millisecond
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
	if opts.TimeoutsConfig.KVDurableTimeout > 0 {
		kvDurableTimeout = opts.TimeoutsConfig.KVDurableTimeout
	}
	if opts.TimeoutsConfig.KVScanTimeout > 0 {
		kvScanTimeout = opts.TimeoutsConfig.KVScanTimeout
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
		managementTimeout = opts.TimeoutsConfig.ManagementTimeout
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

	var initialTracer RequestTracer
	if opts.Tracer != nil {
		initialTracer = opts.Tracer
	} else {
		initialTracer = NewThresholdLoggingTracer(nil)
	}
	tracerAddRef(initialTracer)

	meter := opts.Meter
	if meter == nil {
		agMeter := NewLoggingMeter(nil)
		meter = agMeter
	}

	return &Cluster{
		auth: opts.Authenticator,
		timeoutsConfig: TimeoutsConfig{
			ConnectTimeout:    connectTimeout,
			QueryTimeout:      queryTimeout,
			AnalyticsTimeout:  analyticsTimeout,
			SearchTimeout:     searchTimeout,
			ViewTimeout:       viewTimeout,
			KVTimeout:         kvTimeout,
			KVDurableTimeout:  kvDurableTimeout,
			KVScanTimeout:     kvScanTimeout,
			ManagementTimeout: managementTimeout,
		},
		transcoder:             opts.Transcoder,
		useMutationTokens:      useMutationTokens,
		retryStrategyWrapper:   newRetryStrategyWrapper(opts.RetryStrategy),
		orphanLoggerEnabled:    !opts.OrphanReporterConfig.Disabled,
		orphanLoggerInterval:   opts.OrphanReporterConfig.ReportInterval,
		orphanLoggerSampleSize: opts.OrphanReporterConfig.SampleSize,
		useServerDurations:     useServerDurations,
		tracer:                 initialTracer,
		meter:                  newMeterWrapper(meter),
		circuitBreakerConfig:   opts.CircuitBreakerConfig,
		securityConfig:         opts.SecurityConfig,
		internalConfig:         opts.InternalConfig,
		transactionsConfig:     opts.TransactionsConfig,
		compressionConfig:      opts.CompressionConfig,
	}
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

	cluster := clusterFromOptions(opts)
	cluster.cSpec = connSpec

	err = cluster.parseExtraConnStrOptions(connSpec)
	if err != nil {
		return nil, err
	}

	cli := newConnectionMgr()
	err = cli.buildConfig(cluster)
	if err != nil {
		return nil, err
	}

	err = cli.connect()
	if err != nil {
		return nil, err
	}
	cluster.connectionManager = cli

	cluster.transactions, err = cluster.initTransactions(cluster.transactionsConfig)
	if err != nil {
		return nil, err
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

	if valStr, ok := fetchOption("query_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("query_timeout option must be a number")
		}
		c.timeoutsConfig.QueryTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("analytics_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("analytics_timeout option must be a number")
		}
		c.timeoutsConfig.AnalyticsTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("search_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("search_timeout option must be a number")
		}
		c.timeoutsConfig.SearchTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("view_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("view_timeout option must be a number")
		}
		c.timeoutsConfig.ViewTimeout = time.Duration(val) * time.Millisecond
	}

	return nil
}

// Bucket connects the cluster to server(s) and returns a new Bucket instance.
func (c *Cluster) Bucket(bucketName string) *Bucket {
	b := newBucket(c, bucketName)
	err := c.connectionManager.openBucket(bucketName)
	if err != nil {
		b.setBootstrapError(err)
	}

	return b
}

func (c *Cluster) authenticator() Authenticator {
	return c.auth
}

func (c *Cluster) connSpec() gocbconnstr.ConnSpec {
	return c.cSpec
}

// WaitUntilReadyOptions is the set of options available to the WaitUntilReady operations.
type WaitUntilReadyOptions struct {
	DesiredState ClusterState
	ServiceTypes []ServiceType

	// Using a deadlined Context with WaitUntilReady will cause the shorter of the provided timeout and context deadline
	// to cause cancellation.
	Context context.Context

	// VOLATILE: This API is subject to change at any time.
	RetryStrategy RetryStrategy
}

// WaitUntilReady will wait for the cluster object to be ready for use.
// At present this will wait until memd connections have been established with the server and are ready
// to be used before performing a ping against the specified services which also
// exist in the cluster map.
// If no services are specified then ServiceTypeManagement, ServiceTypeQuery, ServiceTypeSearch, ServiceTypeAnalytics
// will be pinged.
// Valid service types are: ServiceTypeManagement, ServiceTypeQuery, ServiceTypeSearch, ServiceTypeAnalytics.
func (c *Cluster) WaitUntilReady(timeout time.Duration, opts *WaitUntilReadyOptions) error {
	if opts == nil {
		opts = &WaitUntilReadyOptions{}
	}

	cli := c.connectionManager
	if cli == nil {
		return errors.New("cluster is not connected")
	}

	provider, err := cli.getWaitUntilReadyProvider("")
	if err != nil {
		return err
	}

	desiredState := opts.DesiredState
	if desiredState == 0 {
		desiredState = ClusterStateOnline
	}

	gocbcoreServices := make([]gocbcore.ServiceType, len(opts.ServiceTypes))
	for i, svc := range opts.ServiceTypes {
		gocbcoreServices[i] = gocbcore.ServiceType(svc)
	}

	wrapper := c.retryStrategyWrapper
	if opts.RetryStrategy != nil {
		wrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	err = provider.WaitUntilReady(
		opts.Context,
		time.Now().Add(timeout),
		gocbcore.WaitUntilReadyOptions{
			DesiredState:  gocbcore.ClusterState(desiredState),
			ServiceTypes:  gocbcoreServices,
			RetryStrategy: wrapper,
		},
	)
	if err != nil {
		return maybeEnhanceCoreErr(err)
	}

	return nil
}

// Close shuts down all buckets in this cluster and invalidates any references this cluster has.
func (c *Cluster) Close(opts *ClusterCloseOptions) error {
	var overallErr error

	// This needs to be closed first.
	if c.transactions != nil {
		err := c.transactions.close()
		if err != nil {
			logWarnf("Failed to close transactions in cluster close: %s", err)
		}
		c.transactions = nil
	}

	if c.connectionManager != nil {
		err := c.connectionManager.close()
		if err != nil {
			logWarnf("Failed to close cluster connectionManager in cluster close: %s", err)
			overallErr = err
		}
	}

	if c.tracer != nil {
		tracerDecRef(c.tracer)
		c.tracer = nil
	}
	if c.meter != nil {
		if meter, ok := c.meter.meter.(*LoggingMeter); ok {
			meter.close()
		}
		c.meter = nil
	}

	return overallErr
}

func (c *Cluster) getDiagnosticsProvider() (diagnosticsProvider, error) {
	provider, err := c.connectionManager.getDiagnosticsProvider("")
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getQueryProvider() (queryProvider, error) {
	provider, err := c.connectionManager.getQueryProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getAnalyticsProvider() (analyticsProvider, error) {
	provider, err := c.connectionManager.getAnalyticsProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getSearchProvider() (searchProvider, error) {
	provider, err := c.connectionManager.getSearchProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getHTTPProvider() (httpProvider, error) {
	provider, err := c.connectionManager.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return provider, nil
}

// Users returns a UserManager for managing users.
func (c *Cluster) Users() *UserManager {
	return &UserManager{
		provider: c,
		tracer:   c.tracer,
		meter:    c.meter,
	}
}

// Buckets returns a BucketManager for managing buckets.
func (c *Cluster) Buckets() *BucketManager {
	return &BucketManager{
		provider: c,
		tracer:   c.tracer,
		meter:    c.meter,
	}
}

// AnalyticsIndexes returns an AnalyticsIndexManager for managing analytics indexes.
func (c *Cluster) AnalyticsIndexes() *AnalyticsIndexManager {
	return &AnalyticsIndexManager{
		aProvider:     c,
		mgmtProvider:  c,
		globalTimeout: c.timeoutsConfig.ManagementTimeout,
		tracer:        c.tracer,
		meter:         c.meter,
	}
}

// QueryIndexes returns a QueryIndexManager for managing query indexes.
func (c *Cluster) QueryIndexes() *QueryIndexManager {
	return &QueryIndexManager{
		base: &baseQueryIndexManager{
			provider:      c,
			globalTimeout: c.timeoutsConfig.ManagementTimeout,
			tracer:        c.tracer,
			meter:         c.meter,
		},
	}
}

// SearchIndexes returns a SearchIndexManager for managing search indexes.
func (c *Cluster) SearchIndexes() *SearchIndexManager {
	return &SearchIndexManager{
		mgmtProvider: c,
		tracer:       c.tracer,
		meter:        c.meter,
	}
}

// EventingFunctions returns a EventingFunctionManager for managing eventing functions.
// UNCOMMITTED: This API may change in the future.
func (c *Cluster) EventingFunctions() *EventingFunctionManager {
	return &EventingFunctionManager{
		mgmtProvider: c,
		tracer:       c.tracer,
		meter:        c.meter,
	}
}

// Transactions returns a Transactions instance for performing transactions.
func (c *Cluster) Transactions() *Transactions {
	return c.transactions
}
