package gocb

import (
	"fmt"
	gocbcore "github.com/couchbase/gocbcore/v10"
	"github.com/couchbaselabs/gocbconnstr/v2"
	"strconv"
	"time"
)

type connectionManager interface {
	openBucket(bucketName string) error
	connection(bucketName string) (*gocbcore.Agent, error)
	close() error

	getKvProvider(bucketName string) (kvProvider, error)
	getKvBulkProvider(bucketName string) (kvBulkProvider, error)
	getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error)
	getViewProvider(bucketName string) (viewProvider, error)
	getViewIndexProvider(bucketName string) (viewIndexProvider, error)
	getQueryProvider() (queryProvider, error)
	getQueryIndexProvider() (queryIndexProvider, error)
	getAnalyticsProvider() (analyticsProvider, error)
	getAnalyticsIndexProvider() (analyticsIndexProvider, error)
	getSearchProvider() (searchProvider, error)
	getHTTPProvider(bucketName string) (httpProvider, error)
	getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error)
	getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error)
	getCollectionsManagementProvider(bucketName string) (collectionsManagementProvider, error)
	getBucketManagementProvider() (bucketManagementProvider, error)
	getSearchIndexProvider() (searchIndexProvider, error)
	getSearchCapabilitiesProvider() (searchCapabilityVerifier, error)
	getEventingManagementProvider() (eventingManagementProvider, error)
	getUserManagerProvider() (userManagerProvider, error)
	getInternalProvider() (internalProvider, error)

	initTransactions(cluster *Cluster) error
	getTransactionsProvider() (transactionsProvider, error)

	getMeter() *meterWrapper

	SetAuthenticator(opts SetAuthenticatorOptions) error

	opController
}

type opController interface {
	MarkOpBeginning()
	MarkOpCompleted()
}

type providerController[P any] struct {
	get func() (P, error)
	opController

	// Metrics-related fields
	meter    *meterWrapper
	keyspace *keyspace
	service  string
}

func autoOpControl[T any, P any](controller *providerController[P], operation string, opFn func(P) (T, error)) (T, error) {
	controller.MarkOpBeginning()
	defer controller.MarkOpCompleted()

	p, err := controller.get()
	if err != nil {
		var emptyT T
		return emptyT, err
	}

	start := time.Now()
	retT, err := opFn(p)

	if operation != "" && controller.meter != nil {
		defer controller.meter.ValueRecord(controller.service, operation, start, controller.keyspace, err)
	}

	if err != nil {
		var emptyT T
		return emptyT, err
	}

	return retT, nil
}

func autoOpControlErrorOnly[P any](controller *providerController[P], operation string, opFn func(P) error) error {
	_, err := autoOpControl(controller, operation, func(provider P) (struct{}, error) {
		err := opFn(provider)
		return struct{}{}, err
	})

	return err
}

type newConnectionMgrOptions struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	tracer *tracerWrapper
	meter  *meterWrapper

	useServerDurations bool
	useMutationTokens  bool

	timeoutsConfig TimeoutsConfig

	transcoder           Transcoder
	retryStrategyWrapper *coreRetryStrategyWrapper

	orphanLoggerEnabled    bool
	orphanLoggerInterval   time.Duration
	orphanLoggerSampleSize uint32

	circuitBreakerConfig CircuitBreakerConfig
	securityConfig       SecurityConfig
	internalConfig       InternalConfig
	transactionsConfig   TransactionsConfig
	compressionConfig    CompressionConfig
	appTelemetryConfig   AppTelemetryConfig
	compressor           *compressor

	preferredServerGroup string
}

func connectionMgrOptionsFromOptions(opts ClusterOptions) newConnectionMgrOptions {
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

	return newConnectionMgrOptions{
		cSpec:              gocbconnstr.ConnSpec{},
		auth:               opts.Authenticator,
		tracer:             newTracerWrapper(initialTracer),
		meter:              newMeterWrapper(meter),
		useServerDurations: useServerDurations,
		useMutationTokens:  useMutationTokens,
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
		retryStrategyWrapper:   newCoreRetryStrategyWrapper(opts.RetryStrategy),
		orphanLoggerEnabled:    !opts.OrphanReporterConfig.Disabled,
		orphanLoggerInterval:   opts.OrphanReporterConfig.ReportInterval,
		orphanLoggerSampleSize: opts.OrphanReporterConfig.SampleSize,
		circuitBreakerConfig:   opts.CircuitBreakerConfig,
		securityConfig:         opts.SecurityConfig,
		internalConfig:         opts.InternalConfig,
		transactionsConfig:     opts.TransactionsConfig,
		compressionConfig:      opts.CompressionConfig,
		appTelemetryConfig:     opts.AppTelemetryConfig,
		compressor: &compressor{
			CompressionEnabled:  !opts.CompressionConfig.Disabled,
			CompressionMinSize:  opts.CompressionConfig.MinSize,
			CompressionMinRatio: opts.CompressionConfig.MinRatio,
		},
		preferredServerGroup: opts.PreferredServerGroup,
	}
}

func (c *newConnectionMgrOptions) parseExtraConnStrOptions(spec gocbconnstr.ConnSpec) error {
	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	if valStr, ok := fetchOption("kv_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kv_timeout option must be a number")
		}
		c.timeoutsConfig.KVTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("kv_durable_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kv_durable_timeout option must be a number")
		}
		c.timeoutsConfig.KVDurableTimeout = time.Duration(val) * time.Millisecond
	}

	// Volatile: This option is subject to change at any time.
	if valStr, ok := fetchOption("kv_scan_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kv_scan_timeout option must be a number")
		}
		c.timeoutsConfig.KVScanTimeout = time.Duration(val) * time.Millisecond
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

	if valStr, ok := fetchOption("management_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("management_timeout option must be a number")
		}
		c.timeoutsConfig.ManagementTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("disable_app_telemetry"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return fmt.Errorf("disable_app_telemetry option must be a boolean")
		}
		c.appTelemetryConfig.Disabled = val
	}

	return nil
}

func connectConnectionMgr(protocol string, opts newConnectionMgrOptions) (connectionManager, error) {
	switch protocol {
	case "couchbase2":
		return connectPsConnectionMgr(opts)
	default:
		return connectStdConnectionMgr(opts)
	}
}
