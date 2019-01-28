package gocb

import (
	"context"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/pkg/errors"

	"gopkg.in/couchbase/gocbcore.v8"
)

type client interface {
	Hash() string
	connect()
	openCollection(ctx context.Context, traceCtx opentracing.SpanContext, scopeName string, collectionName string)
	getKvProvider() (kvProvider, error)
	getHTTPProvider() (httpProvider, error)
	close() error
}

type stdClient struct {
	cluster      *Cluster
	state        clientStateBlock
	lock         sync.Mutex
	agent        *gocbcore.Agent
	bootstrapErr error
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

// TODO: This probably needs to be deadlined...
func (c *stdClient) connect() {
	c.lock.Lock()
	defer c.lock.Unlock()

	auth := c.cluster.auth

	config := &gocbcore.AgentConfig{
		UserString:           "gocb/" + Version(),
		ConnectTimeout:       c.cluster.sb.ConnectTimeout,
		UseMutationTokens:    c.state.UseMutationTokens,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		UseDurations:         true,
		NoRootTraceSpans:     true,
		UseCollections:       true,
		UseEnhancedErrors:    true,
	}

	err := config.FromConnStr(c.cluster.connSpec().String())
	if err != nil {
		c.bootstrapErr = err
	}

	useCertificates := config.TlsConfig != nil && len(config.TlsConfig.Certificates) > 0
	if useCertificates {
		if auth == nil {
			c.bootstrapErr = configurationError{message: "invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together"}
		}
		_, ok := auth.(CertAuthenticator)
		if !ok {
			c.bootstrapErr = configurationError{message: "invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together"}
		}
	}

	_, ok := auth.(CertAuthenticator)
	if ok && !useCertificates {
		c.bootstrapErr = configurationError{message: "invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together"}
	}

	config.BucketName = c.state.BucketName
	config.UseMutationTokens = c.state.UseMutationTokens
	config.Auth = &coreAuthWrapper{
		auth:       c.cluster.authenticator(),
		bucketName: c.state.BucketName,
	}

	agent, err := gocbcore.CreateAgent(config)
	if err != nil {
		c.bootstrapErr = maybeEnhanceErr(err, "")
	}

	c.agent = agent
}

func (c *stdClient) getKvProvider() (kvProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("Cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getHTTPProvider() (httpProvider, error) {
	if c.bootstrapErr != nil {
		return nil, c.bootstrapErr
	}

	if c.agent == nil {
		return nil, errors.New("Cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) openCollection(ctx context.Context, traceCtx opentracing.SpanContext, scopeName string, collectionName string) {
	if scopeName == "_default" && collectionName == "_default" {
		return
	}

	if c.agent == nil {
		c.bootstrapErr = errors.New("Cluster not yet connected")
		return
	}

	// if the collection/scope are none default and the collection ID can't be found then error
	if !c.agent.HasCollectionsSupport() {
		c.bootstrapErr = errors.New("Collections not supported by server")
		return
	}

	waitCh := make(chan struct{})
	var colErr error

	op, err := c.agent.GetCollectionID(scopeName, collectionName,
		gocbcore.GetCollectionIDOptions{TraceContext: traceCtx}, func(manifestID uint64, cid uint32, err error) {
			if err != nil {
				colErr = err
				waitCh <- struct{}{}
				return
			}

			waitCh <- struct{}{}
		})
	if err != nil {
		c.bootstrapErr = err
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

func (c *stdClient) close() error {
	if c.agent == nil {
		return errors.New("Cluster not yet connected")
	}
	return c.agent.Close()
}
