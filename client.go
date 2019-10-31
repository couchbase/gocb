package gocb

import (
	"context"
	"fmt"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

type client interface {
	Hash() string
	connect() error
	buildConfig() error
	openCollection(ctx context.Context, scopeName string, collectionName string)
	getKvProvider() (kvProvider, error)
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

	auth := c.cluster.auth
	breakerCfg := c.cluster.sb.CircuitBreakerConfig

	var completionCallback func(err error) bool
	if breakerCfg.CompletionCallback != nil {
		completionCallback = func(err error) bool {
			return breakerCfg.CompletionCallback(maybeEnhanceKVErr(err, "", false))
		}
	}

	config := &gocbcore.AgentConfig{
		UserString:           Identifier(),
		ConnectTimeout:       c.cluster.sb.ConnectTimeout,
		UseMutationTokens:    c.cluster.sb.UseMutationTokens,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		UseDurations:         c.cluster.sb.UseServerDurations,
		UseCollections:       true,
		UseEnhancedErrors:    true,
		BucketName:           c.state.BucketName,
		AuthMechanisms: []gocbcore.AuthMechanism{
			gocbcore.ScramSha512AuthMechanism, gocbcore.ScramSha256AuthMechanism, gocbcore.ScramSha1AuthMechanism, gocbcore.PlainAuthMechanism,
		},
		UseZombieLogger:        c.cluster.sb.OrphanLoggerEnabled,
		ZombieLoggerInterval:   c.cluster.sb.OrphanLoggerInterval,
		ZombieLoggerSampleSize: c.cluster.sb.OrphanLoggerSampleSize,
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

	useCertificates := config.TlsConfig != nil && len(config.TlsConfig.Certificates) > 0
	if useCertificates {
		if auth == nil {
			return configurationError{message: "invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together"}
		}
		_, ok := auth.(CertAuthenticator)
		if !ok {
			return configurationError{message: "invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together"}
		}
	}

	_, ok := auth.(CertAuthenticator)
	if ok && !useCertificates {
		return configurationError{message: "invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together"}
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
		return maybeEnhanceKVErr(err, "", false)
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
		return nil, configurationError{message: "cluster not yet connected"}
	}
	return c.agent, nil
}

func (c *stdClient) getHTTPProvider() (httpProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, configurationError{message: "cluster not yet connected"}
	}
	return c.agent, nil
}

func (c *stdClient) getDiagnosticsProvider() (diagnosticsProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	return c.agent, nil
}

func (c *stdClient) openCollection(ctx context.Context, scopeName string, collectionName string) {
	if scopeName == "_default" && collectionName == "_default" {
		return
	}

	if c.agent == nil {
		c.bootstrapErr = configurationError{message: "cluster not yet connected"}
		return
	}

	// if the collection/scope are none default and the collection ID can't be found then error
	if !c.agent.HasCollectionsSupport() {
		c.bootstrapErr = configurationError{message: "Collections not supported by server"}
		return
	}

	waitCh := make(chan struct{})
	var colErr error

	op, err := c.agent.GetCollectionID(scopeName, collectionName, gocbcore.GetCollectionIDOptions{}, func(manifestID uint64, cid uint32, err error) {
		if err != nil {
			colErr = maybeEnhanceKVErr(err, fmt.Sprintf("%s.%s", scopeName, collectionName), false)
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})
	if err != nil {
		c.bootstrapErr = maybeEnhanceKVErr(err, fmt.Sprintf("%s.%s", scopeName, collectionName), false)
		return
	}

	select {
	case <-ctx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				colErr = timeoutError{}
			} else {
				colErr = ctx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	c.bootstrapErr = colErr
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
		return configurationError{message: "cluster not yet connected"}
	}
	c.isConnected = false
	c.lock.Unlock()
	return c.agent.Close()
}
