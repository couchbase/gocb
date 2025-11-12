package gocb

import (
	"crypto/x509"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocbcore/v10"
)

type stdConnectionMgr struct {
	lock       sync.Mutex
	agentgroup *gocbcore.AgentGroup

	retryStrategyWrapper *coreRetryStrategyWrapper
	transcoder           Transcoder
	timeouts             TimeoutsConfig
	tracer               *tracerWrapper
	meter                *meterWrapper
	txns                 *transactionsProviderCore
	appTelemetryReporter *gocbcore.TelemetryReporter
	preferredServerGroup string
	compressor           *compressor
	useMutationTokens    bool
	useServerDurations   bool

	closed      atomic.Bool
	activeOpsWg sync.WaitGroup
}

func connectStdConnectionMgr(opts newConnectionMgrOptions) (*stdConnectionMgr, error) {
	breakerCfg := opts.circuitBreakerConfig

	var completionCallback func(err error) bool
	if breakerCfg.CompletionCallback != nil {
		completionCallback = func(err error) bool {
			wrappedErr := maybeEnhanceKVErr(err, "", "", "", "")
			return breakerCfg.CompletionCallback(wrappedErr)
		}
	}

	var authMechanisms []gocbcore.AuthMechanism
	for _, mech := range opts.securityConfig.AllowedSaslMechanisms {
		authMechanisms = append(authMechanisms, gocbcore.AuthMechanism(mech))
	}

	var appTelemetryReporter *gocbcore.TelemetryReporter
	if !opts.appTelemetryConfig.Disabled {
		appTelemetryReporter = gocbcore.CreateTelemetryReporter(gocbcore.TelemetryReporterConfig{
			ExternalEndpoint: opts.appTelemetryConfig.ExternalEndpoint,
			Backoff:          opts.appTelemetryConfig.Backoff,
			PingInterval:     opts.appTelemetryConfig.PingInterval,
			PingTimeout:      opts.appTelemetryConfig.PingTimeout,
		})
	}

	config := &gocbcore.AgentGroupConfig{
		AgentConfig: gocbcore.AgentConfig{
			UserAgent: Identifier(),
			SecurityConfig: gocbcore.SecurityConfig{
				AuthMechanisms: authMechanisms,
			},
			IoConfig: gocbcore.IoConfig{
				UseCollections:             true,
				UseDurations:               opts.useServerDurations,
				UseMutationTokens:          opts.useMutationTokens,
				UseOutOfOrderResponses:     true,
				UseClusterMapNotifications: true,
			},
			KVConfig: gocbcore.KVConfig{
				ConnectTimeout:       opts.timeoutsConfig.ConnectTimeout,
				ConnectionBufferSize: opts.internalConfig.ConnectionBufferSize,
			},
			DefaultRetryStrategy: opts.retryStrategyWrapper,
			CircuitBreakerConfig: gocbcore.CircuitBreakerConfig{
				Enabled:                  !breakerCfg.Disabled,
				VolumeThreshold:          breakerCfg.VolumeThreshold,
				ErrorThresholdPercentage: breakerCfg.ErrorThresholdPercentage,
				SleepWindow:              breakerCfg.SleepWindow,
				RollingWindow:            breakerCfg.RollingWindow,
				CanaryTimeout:            breakerCfg.CanaryTimeout,
				CompletionCallback:       completionCallback,
			},
			OrphanReporterConfig: gocbcore.OrphanReporterConfig{
				Enabled:        opts.orphanLoggerEnabled,
				ReportInterval: opts.orphanLoggerInterval,
				SampleSize:     int(opts.orphanLoggerSampleSize),
			},
			TracerConfig: gocbcore.TracerConfig{
				NoRootTraceSpans: true,
				Tracer:           &coreRequestTracerWrapper{tracer: opts.tracer.tracer},
			},
			MeterConfig: gocbcore.MeterConfig{
				// At the moment we only support our own operations metric so there's no point in setting a meter for gocbcore.
				Meter: nil,
			},
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled:  !opts.compressionConfig.Disabled,
				MinSize:  int(opts.compressionConfig.MinSize),
				MinRatio: opts.compressionConfig.MinRatio,
			},
			TelemetryConfig: gocbcore.TelemetryConfig{
				TelemetryReporter: appTelemetryReporter,
			},
		},
	}

	err := config.FromConnStr(opts.cSpec.String())
	if err != nil {
		return nil, err
	}

	config.SecurityConfig.Auth = &coreAuthWrapper{
		auth: opts.auth,
	}

	if config.SecurityConfig.UseTLS {
		config.SecurityConfig.TLSRootCAProvider = opts.internalConfig.TLSRootCAProvider

		if config.SecurityConfig.TLSRootCAProvider == nil && (opts.securityConfig.TLSRootCAs != nil ||
			opts.securityConfig.TLSSkipVerify) {
			config.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
				if opts.securityConfig.TLSSkipVerify {
					return nil
				}

				return opts.securityConfig.TLSRootCAs
			}
		}
	}

	agentGroup, err := gocbcore.CreateAgentGroup(config)
	if err != nil {
		return nil, maybeEnhanceKVErr(err, "", "", "", "")
	}

	opts.meter.clusterLabelsProvider = agentGroup.Internal()
	opts.tracer.clusterLabelsProvider = agentGroup.Internal()

	txns := newTransactionsProviderCore(opts.transactionsConfig, transactionsProviderCoreOptions{
		TimeoutsConfig:       opts.timeoutsConfig,
		PreferredServerGroup: opts.preferredServerGroup,
	}, agentGroup)

	return &stdConnectionMgr{
		lock:                 sync.Mutex{},
		agentgroup:           agentGroup,
		retryStrategyWrapper: opts.retryStrategyWrapper,
		transcoder:           opts.transcoder,
		timeouts:             opts.timeoutsConfig,
		tracer:               opts.tracer,
		meter:                opts.meter,
		txns:                 txns,
		appTelemetryReporter: appTelemetryReporter,
		preferredServerGroup: opts.preferredServerGroup,
		compressor:           opts.compressor,
		useMutationTokens:    opts.useMutationTokens,
		useServerDurations:   opts.useServerDurations,
		closed:               atomic.Bool{},
		activeOpsWg:          sync.WaitGroup{},
	}, nil
}

func (c *stdConnectionMgr) openBucket(bucketName string) error {
	if err := c.canPerformOp(); err != nil {
		return err
	}

	if c.agentgroup == nil {
		return errors.New("cluster not yet connected")
	}

	return c.agentgroup.OpenBucket(bucketName)
}

func (c *stdConnectionMgr) canPerformOp() error {
	if c.closed.Load() {
		return ErrShutdown
	}

	return nil
}

func (c *stdConnectionMgr) MarkOpBeginning() {
	c.activeOpsWg.Add(1)
}

func (c *stdConnectionMgr) MarkOpCompleted() {
	c.activeOpsWg.Done()
}

func (c *stdConnectionMgr) getKvProvider(bucketName string) (kvProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}
	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}
	return &kvProviderCore{
		agent:            agent,
		snapshotProvider: &stdCoreConfigSnapshotProvider{agent: agent},

		kvTimeout:            c.timeouts.KVTimeout,
		kvDurableTimeout:     c.timeouts.KVDurableTimeout,
		kvScanTimeout:        c.timeouts.KVScanTimeout,
		transcoder:           c.transcoder,
		useMutationTokens:    c.useMutationTokens,
		retryStrategyWrapper: c.retryStrategyWrapper,

		tracer:               c.tracer,
		preferredServerGroup: c.preferredServerGroup,
	}, nil
}

func (c *stdConnectionMgr) getKvBulkProvider(bucketName string) (kvBulkProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}
	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}
	return &kvBulkProviderCore{
		agent:                agent,
		kvTimeout:            c.timeouts.KVTimeout,
		transcoder:           c.transcoder,
		retryStrategyWrapper: c.retryStrategyWrapper,
		tracer:               c.tracer,
		meter:                c.meter,
	}, nil
}

func (c *stdConnectionMgr) getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}
	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}
	return agent.Internal(), nil
}

func (c *stdConnectionMgr) getViewProvider(bucketName string) (viewProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}

	return &viewProviderCore{
		provider:             &viewProviderWrapper{provider: agent},
		retryStrategyWrapper: c.retryStrategyWrapper,
		transcoder:           c.transcoder,
		timeouts:             c.timeouts,
		tracer:               c.tracer,
		bucketName:           bucketName,
	}, nil
}

func (c *stdConnectionMgr) getViewIndexProvider(bucketName string) (viewIndexProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	provider, err := c.getHTTPProvider(bucketName)
	if err != nil {
		return nil, err
	}

	return &viewIndexProviderCore{
		mgmtProvider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		bucketName: bucketName,
		tracer:     c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getQueryProvider() (queryProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	return &queryProviderCore{
		provider: &queryProviderWrapper{provider: c.agentgroup},

		retryStrategyWrapper: c.retryStrategyWrapper,
		transcoder:           c.transcoder,
		timeouts:             c.timeouts,
		tracer:               c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getQueryIndexProvider() (queryIndexProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	return &queryProviderCore{
		provider: &queryProviderWrapper{provider: c.agentgroup},

		retryStrategyWrapper: c.retryStrategyWrapper,
		transcoder:           c.transcoder,
		timeouts:             c.timeouts,
		tracer:               c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getAnalyticsProvider() (analyticsProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	mgmtProvider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return &analyticsProviderCore{
		provider: &analyticsProviderWrapper{provider: c.agentgroup},
		mgmtProvider: &mgmtProviderCore{
			provider:             mgmtProvider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},

		retryStrategyWrapper: c.retryStrategyWrapper,
		transcoder:           c.transcoder,
		analyticsTimeout:     c.timeouts.AnalyticsTimeout,
		tracer:               c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getAnalyticsIndexProvider() (analyticsIndexProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	mgmtProvider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return &analyticsProviderCore{
		provider: &analyticsProviderWrapper{provider: c.agentgroup},
		mgmtProvider: &mgmtProviderCore{
			provider:             mgmtProvider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},

		retryStrategyWrapper: c.retryStrategyWrapper,
		transcoder:           c.transcoder,
		analyticsTimeout:     c.timeouts.AnalyticsTimeout,
		tracer:               c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getSearchProvider() (searchProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	return &searchProviderCore{
		provider:             &searchProviderWrapper{agent: c.agentgroup},
		retryStrategyWrapper: c.retryStrategyWrapper,
		transcoder:           c.transcoder,
		timeouts:             c.timeouts,
		tracer:               c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getSearchIndexProvider() (searchIndexProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	provider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	capVerifier, err := c.getSearchCapabilitiesProvider()
	if err != nil {
		return nil, err
	}

	return &searchIndexProviderCore{
		mgmtProvider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		searchCapVerifier: capVerifier,
		tracer:            c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getSearchCapabilitiesProvider() (searchCapabilityVerifier, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	return c.agentgroup.Internal(), nil
}

func (c *stdConnectionMgr) getHTTPProvider(bucketName string) (httpProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	if bucketName == "" {
		return &httpProviderWrapper{
			provider: c.agentgroup,
		}, nil
	}

	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}

	return &httpProviderWrapper{
		provider: agent,
	}, nil
}

func (c *stdConnectionMgr) getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	if bucketName == "" {
		return &diagnosticsProviderCore{
			provider: &diagnosticsProviderWrapper{provider: c.agentgroup},
			tracer:   c.tracer,
			timeouts: c.timeouts,
		}, nil
	}

	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}

	return &diagnosticsProviderCore{
		provider: &diagnosticsProviderWrapper{provider: agent},
		tracer:   c.tracer,
		timeouts: c.timeouts,
	}, nil
}

func (c *stdConnectionMgr) getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	if bucketName == "" {
		return &waitUntilReadyProviderCore{
			provider:             c.agentgroup,
			retryStrategyWrapper: c.retryStrategyWrapper,
		}, nil
	}

	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("provider not yet connected")
	}

	return &waitUntilReadyProviderCore{
		provider:             agent,
		retryStrategyWrapper: c.retryStrategyWrapper,
	}, nil
}

func (c *stdConnectionMgr) getCollectionsManagementProvider(bucketName string) (collectionsManagementProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	provider, err := c.getHTTPProvider(bucketName)
	if err != nil {
		return nil, err
	}

	capabilityProvider, err := c.getKvCapabilitiesProvider(bucketName)
	if err != nil {
		return nil, err
	}

	return &collectionsManagementProviderCore{
		mgmtProvider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		featureVerifier: capabilityProvider,
		bucketName:      bucketName,
		tracer:          c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getBucketManagementProvider() (bucketManagementProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	provider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return &bucketManagementProviderCore{
		mgmtProvider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		tracer: c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getEventingManagementProvider() (eventingManagementProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	provider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return &eventingManagementProviderCore{
		mgmtProvider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		tracer: c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getUserManagerProvider() (userManagerProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	provider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return &userManagerProviderCore{
		provider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		tracer: c.tracer,
	}, nil
}

func (c *stdConnectionMgr) getInternalProvider() (internalProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	provider, err := c.getHTTPProvider("")
	if err != nil {
		return nil, err
	}

	return &internalProviderCore{
		provider: &mgmtProviderCore{
			provider:             provider,
			mgmtTimeout:          c.timeouts.ManagementTimeout,
			retryStrategyWrapper: c.retryStrategyWrapper,
		},
		tracer: c.tracer,
		meter:  c.meter,
	}, nil

}

// initTransactions must only be called during cluster setup to prevent races.
func (c *stdConnectionMgr) initTransactions(cluster *Cluster) error {
	err := c.txns.Init(cluster)
	if err != nil {
		return err
	}

	return nil
}

func (c *stdConnectionMgr) getTransactionsProvider() (transactionsProvider, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	return c.txns, nil
}

func (c *stdConnectionMgr) connection(bucketName string) (*gocbcore.Agent, error) {
	if err := c.canPerformOp(); err != nil {
		return nil, err
	}

	if c.agentgroup == nil {
		return nil, errors.New("cluster not yet connected")
	}

	agent := c.agentgroup.GetAgent(bucketName)
	if agent == nil {
		return nil, errors.New("bucket not yet connected")
	}
	return agent, nil
}

func (c *stdConnectionMgr) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrShutdown
	}

	if c.txns != nil {
		err := c.txns.close()
		if err != nil {
			logWarnf("Failed to close transactions in cluster close: %s", err)
		}
		c.txns = nil
	}

	c.lock.Lock()
	if c.agentgroup == nil {
		c.lock.Unlock()
		return errors.New("cluster not yet connected")
	}
	defer c.lock.Unlock()

	err := c.agentgroup.Close()

	logDebugf("Waiting for any active requests to complete")
	c.activeOpsWg.Wait()

	if c.tracer != nil {
		tracerDecRef(c.tracer.tracer)
		c.tracer = nil
	}
	if c.meter != nil {
		if meter, ok := c.meter.meter.(*LoggingMeter); ok {
			meter.close()
		}
		c.meter = nil
	}
	if c.appTelemetryReporter != nil {
		c.appTelemetryReporter.Close()
		c.appTelemetryReporter = nil
	}

	return err
}

func (c *stdConnectionMgr) getMeter() *meterWrapper {
	return c.meter
}
