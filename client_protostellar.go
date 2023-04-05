package gocb

import (
	"errors"
	"sync"

	gocbcore "github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcoreps"
)

type protoStellarConnectionMgr struct {
	host   string
	lock   sync.Mutex
	config *gocbcoreps.DialOptions
	agent  *gocbcoreps.RoutingClient
}

func (c *protoStellarConnectionMgr) connect() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	client, err := gocbcoreps.Dial(c.host, c.config)
	if err != nil {
		return err
	}

	c.agent = client

	return nil
}

func (c *protoStellarConnectionMgr) openBucket(bucketName string) error {
	c.agent.OpenBucket(bucketName)
	return nil
}

func (c *protoStellarConnectionMgr) buildConfig(cluster *Cluster) error
func (c *protoStellarConnectionMgr) getKvProvider(bucketName string) (kvProvider, error)
func (c *protoStellarConnectionMgr) getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error)
func (c *protoStellarConnectionMgr) getViewProvider(bucketName string) (viewProvider, error)
func (c *protoStellarConnectionMgr) getQueryProvider() (queryProvider, error)
func (c *protoStellarConnectionMgr) getAnalyticsProvider() (analyticsProvider, error)
func (c *protoStellarConnectionMgr) getSearchProvider() (searchProvider, error)
func (c *protoStellarConnectionMgr) getHTTPProvider(bucketName string) (httpProvider, error)
func (c *protoStellarConnectionMgr) getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error)
func (c *protoStellarConnectionMgr) getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error)
func (c *protoStellarConnectionMgr) connection(bucketName string) (*gocbcore.Agent, error) {
	return nil, errors.New("is not implemented")
}
func (c *protoStellarConnectionMgr) close() error
