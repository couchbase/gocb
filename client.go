package gocb

import (
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

type client interface {
	Hash() string
	connect() error
	buildConfig() error
	getKvProvider() (kvProvider, error)
	getViewProvider() (viewProvider, error)
	getQueryProvider() (queryProvider, error)
	getAnalyticsProvider() (analyticsProvider, error)
	getSearchProvider() (searchProvider, error)
	getHTTPProvider() (httpProvider, error)
	getDiagnosticsProvider() (diagnosticsProvider, error)
	close() error
	setBootstrapError(err error)
	selectBucket(bucketName string) error
	supportsGCCCP() bool
	connected() bool
	getBootstrapError() error
}

type stdClient struct {
	cluster      *Cluster
	state        clientStateBlock
	lock         sync.Mutex
	agent        *gocbcore.Agent
	bootstrapErr error
	isConnected  bool
	config       *gocbcore.AgentConfig
}

func newClient(cluster *Cluster, sb *clientStateBlock) *stdClient {
	client := &stdClient{
		cluster: cluster,
		state:   *sb,
	}
	return client
}

func (c *stdClient) Hash() string {
	return c.state.Hash()
}

func (c *stdClient) buildConfig() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	breakerCfg := c.cluster.sb.CircuitBreakerConfig

	var completionCallback func(err error) bool
	if breakerCfg.CompletionCallback != nil {
		completionCallback = func(err error) bool {
			wrappedErr := maybeEnhanceKVErr(err, c.state.BucketName, "", "", "")
			return breakerCfg.CompletionCallback(wrappedErr)
		}
	}

	config := &gocbcore.AgentConfig{
		UserAgent:              Identifier(),
		ConnectTimeout:         c.cluster.sb.ConnectTimeout,
		UseMutationTokens:      c.cluster.sb.UseMutationTokens,
		KVConnectTimeout:       7000 * time.Millisecond,
		UseDurations:           c.cluster.sb.UseServerDurations,
		UseCollections:         true,
		BucketName:             c.state.BucketName,
		UseZombieLogger:        c.cluster.sb.OrphanLoggerEnabled,
		ZombieLoggerInterval:   c.cluster.sb.OrphanLoggerInterval,
		ZombieLoggerSampleSize: int(c.cluster.sb.OrphanLoggerSampleSize),
		NoRootTraceSpans:       true,
		Tracer:                 &requestTracerWrapper{c.cluster.sb.Tracer},
		CircuitBreakerConfig: gocbcore.CircuitBreakerConfig{
			Enabled:                  !breakerCfg.Disabled,
			VolumeThreshold:          breakerCfg.VolumeThreshold,
			ErrorThresholdPercentage: breakerCfg.ErrorThresholdPercentage,
			SleepWindow:              breakerCfg.SleepWindow,
			RollingWindow:            breakerCfg.RollingWindow,
			CanaryTimeout:            breakerCfg.CanaryTimeout,
			CompletionCallback:       completionCallback,
		},
	}

	err := config.FromConnStr(c.cluster.connSpec().String())
	if err != nil {
		return err
	}

	config.Auth = &coreAuthWrapper{
		auth: c.cluster.authenticator(),
	}

	c.config = config
	return nil
}

func (c *stdClient) connect() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	agent, err := gocbcore.CreateAgent(c.config)
	if err != nil {
		return maybeEnhanceKVErr(err, c.state.BucketName, "", "", "")
	}

	c.agent = agent
	c.isConnected = true
	return nil
}

func (c *stdClient) setBootstrapError(err error) {
	c.bootstrapErr = err
}

func (c *stdClient) getBootstrapError() error {
	return c.bootstrapErr
}

func (c *stdClient) getKvProvider() (kvProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getViewProvider() (viewProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getQueryProvider() (queryProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getAnalyticsProvider() (analyticsProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getSearchProvider() (searchProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getHTTPProvider() (httpProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getDiagnosticsProvider() (diagnosticsProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	return c.agent, nil
}

func (c *stdClient) connected() bool {
	return c.isConnected
}

func (c *stdClient) selectBucket(bucketName string) error {
	return c.agent.SelectBucket(bucketName, time.Now().Add(c.cluster.sb.ConnectTimeout))
}

func (c *stdClient) supportsGCCCP() bool {
	return c.agent.UsingGCCCP()
}

func (c *stdClient) close() error {
	c.lock.Lock()
	if c.agent == nil {
		c.lock.Unlock()
		return errors.New("cluster not yet connected")
	}
	c.isConnected = false
	c.lock.Unlock()
	return c.agent.Close()
}
