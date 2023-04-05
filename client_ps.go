package gocb

import (
	"fmt"
	"sync"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcoreps"
)

type psConnectionMgr struct {
	host   string
	lock   sync.Mutex
	config *gocbcoreps.DialOptions
	agent  *gocbcoreps.RoutingClient

	timeouts TimeoutsConfig
	tracer   RequestTracer
	meter    *meterWrapper
}

func (c *psConnectionMgr) connect() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	client, err := gocbcoreps.Dial(c.host, c.config)
	if err != nil {
		return err
	}

	c.agent = client

	return nil
}

func (c *psConnectionMgr) openBucket(bucketName string) error {
	return nil
}

func (c *psConnectionMgr) buildConfig(cluster *Cluster) error {
	c.host = fmt.Sprintf("%s:%d", cluster.connSpec().Addresses[0].Host, cluster.connSpec().Addresses[0].Port)

	creds, err := cluster.authenticator().Credentials(AuthCredsRequest{})
	if err != nil {
		return err
	}

	c.config = &gocbcoreps.DialOptions{
		Username:           creds[0].Username,
		Password:           creds[0].Password,
		InsecureSkipVerify: cluster.securityConfig.TLSSkipVerify,
		ClientCertificate:  cluster.securityConfig.TLSRootCAs,
	}

	return nil
}
func (c *psConnectionMgr) getKvProvider(bucketName string) (kvProvider, error) {
	kv := c.agent.KvV1()
	return &kvProviderPs{client: kv}, nil
}
func (c *psConnectionMgr) getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error) {
	return &gocbcore.AgentInternal{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getViewProvider(bucketName string) (viewProvider, error) {
	return &viewProviderWrapper{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getQueryProvider() (queryProvider, error) {
	return &queryProviderWrapper{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getAnalyticsProvider() (analyticsProvider, error) {
	return &analyticsProviderWrapper{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getSearchProvider() (searchProvider, error) {
	return &searchProviderWrapper{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getHTTPProvider(bucketName string) (httpProvider, error) {
	return &httpProviderWrapper{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error) {
	return &diagnosticsProviderWrapper{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error) {
	return &waitUntilReadyProviderPs{}, nil
}
func (c *psConnectionMgr) connection(bucketName string) (*gocbcore.Agent, error) {
	return &gocbcore.Agent{}, ErrFeatureNotAvailable
}
func (c *psConnectionMgr) close() error {
	return c.agent.Close()
}
