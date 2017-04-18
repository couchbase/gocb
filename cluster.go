package gocb

import (
	"errors"
	"fmt"
	"gopkg.in/couchbase/gocbcore.v6"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Cluster represents a connection to a specific Couchbase cluster.
type Cluster struct {
	auth             Authenticator
	agentConfig      gocbcore.AgentConfig
	n1qlTimeout      time.Duration
	ftsTimeout       time.Duration
	analyticsTimeout time.Duration

	clusterLock sync.RWMutex
	queryCache  map[string]*n1qlCache
	bucketList  []*Bucket
	httpCli     *http.Client

	analyticsHosts []string
}

// Connect creates a new Cluster object for a specific cluster.
func Connect(connSpecStr string) (*Cluster, error) {
	spec, err := gocbconnstr.Parse(connSpecStr)
	if err != nil {
		return nil, err
	}

	if spec.Bucket != "" {
		return nil, errors.New("Connection string passed to Connect() must not have any bucket specified!")
	}

	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	config := gocbcore.AgentConfig{
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
	}
	err = config.FromConnStr(connSpecStr)
	if err != nil {
		return nil, err
	}

	httpCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: config.TlsConfig,
		},
	}

	cluster := &Cluster{
		agentConfig: config,
		n1qlTimeout: 75 * time.Second,
		ftsTimeout:  75 * time.Second,

		httpCli:    httpCli,
		queryCache: make(map[string]*n1qlCache),
	}

	if valStr, ok := fetchOption("n1ql_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("n1ql_timeout option must be a number")
		}
		cluster.n1qlTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("fts_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fts_timeout option must be a number")
		}
		cluster.ftsTimeout = time.Duration(val) * time.Millisecond
	}

	return cluster, nil
}

// ConnectTimeout returns the maximum time to wait when attempting to connect to a bucket.
func (c *Cluster) ConnectTimeout() time.Duration {
	return c.agentConfig.ConnectTimeout
}

// SetConnectTimeout sets the maximum time to wait when attempting to connect to a bucket.
func (c *Cluster) SetConnectTimeout(timeout time.Duration) {
	c.agentConfig.ConnectTimeout = timeout
}

// ServerConnectTimeout returns the maximum time to attempt to connect to a single node.
func (c *Cluster) ServerConnectTimeout() time.Duration {
	return c.agentConfig.ServerConnectTimeout
}

// SetServerConnectTimeout sets the maximum time to attempt to connect to a single node.
func (c *Cluster) SetServerConnectTimeout(timeout time.Duration) {
	c.agentConfig.ServerConnectTimeout = timeout
}

// N1qlTimeout returns the maximum time to wait for a cluster-level N1QL query to complete.
func (c *Cluster) N1qlTimeout() time.Duration {
	return c.n1qlTimeout
}

// SetN1qlTimeout sets the maximum time to wait for a cluster-level N1QL query to complete.
func (c *Cluster) SetN1qlTimeout(timeout time.Duration) {
	c.n1qlTimeout = timeout
}

// FtsTimeout returns the maximum time to wait for a cluster-level FTS query to complete.
func (c *Cluster) FtsTimeout() time.Duration {
	return c.ftsTimeout
}

// SetFtsTimeout sets the maximum time to wait for a cluster-level FTS query to complete.
func (c *Cluster) SetFtsTimeout(timeout time.Duration) {
	c.ftsTimeout = timeout
}

// AnalyticsTimeout returns the maximum time to wait for a cluster-level Analytics query to complete.
func (c *Cluster) AnalyticsTimeout() time.Duration {
	return c.analyticsTimeout
}

// SetAnalyticsTimeout sets the maximum time to wait for a cluster-level Analytics query to complete.
func (c *Cluster) SetAnalyticsTimeout(timeout time.Duration) {
	c.analyticsTimeout = timeout
}

// NmvRetryDelay returns the time to wait between retrying an operation due to not my vbucket.
func (c *Cluster) NmvRetryDelay() time.Duration {
	return c.agentConfig.NmvRetryDelay
}

// SetNmvRetryDelay sets the time to wait between retrying an operation due to not my vbucket.
func (c *Cluster) SetNmvRetryDelay(delay time.Duration) {
	c.agentConfig.NmvRetryDelay = delay
}

// InvalidateQueryCache forces the internal cache of prepared queries to be cleared.
func (c *Cluster) InvalidateQueryCache() {
	c.clusterLock.Lock()
	c.queryCache = make(map[string]*n1qlCache)
	c.clusterLock.Unlock()
}

func (c *Cluster) makeAgentConfig(bucket, username, password string, forceMt bool) (*gocbcore.AgentConfig, error) {
	config := c.agentConfig

	config.BucketName = bucket
	config.Username = username
	config.Password = password

	if forceMt {
		config.UseMutationTokens = true
	}

	return &config, nil
}

// Authenticate specifies an Authenticator interface to use to authenticate with cluster services.
func (c *Cluster) Authenticate(auth Authenticator) error {
	c.auth = auth
	return nil
}

func (c *Cluster) openBucket(bucket, password string, forceMt bool) (*Bucket, error) {
	username := bucket
	if password == "" {
		if c.auth != nil {
			userPass := c.auth.bucketMemd(bucket)
			username = userPass.Username
			password = userPass.Password
		}
	}

	agentConfig, err := c.makeAgentConfig(bucket, username, password, forceMt)
	if err != nil {
		return nil, err
	}

	b, err := createBucket(c, agentConfig)
	if err != nil {
		return nil, err
	}

	c.clusterLock.Lock()
	c.bucketList = append(c.bucketList, b)
	c.clusterLock.Unlock()

	return b, nil
}

// OpenBucket opens a new connection to the specified bucket.
func (c *Cluster) OpenBucket(bucket, password string) (*Bucket, error) {
	return c.openBucket(bucket, password, false)
}

// OpenBucketWithMt opens a new connection to the specified bucket and enables mutation tokens.
// MutationTokens allow you to execute queries and durability requirements with very specific
// operation-level consistency.
func (c *Cluster) OpenBucketWithMt(bucket, password string) (*Bucket, error) {
	return c.openBucket(bucket, password, true)
}

func (c *Cluster) closeBucket(bucket *Bucket) {
	c.clusterLock.Lock()
	for i, e := range c.bucketList {
		if e == bucket {
			c.bucketList = append(c.bucketList[0:i], c.bucketList[i+1:]...)
			break
		}
	}
	c.clusterLock.Unlock()
}

// Manager returns a ClusterManager object for performing cluster management operations on this cluster.
func (c *Cluster) Manager(username, password string) *ClusterManager {
	userPass := userPassPair{username, password}
	if username == "" && password == "" {
		if c.auth != nil {
			userPass = c.auth.clusterMgmt()
		}
	}

	var mgmtHosts []string
	for _, host := range c.agentConfig.HttpAddrs {
		if c.agentConfig.TlsConfig != nil {
			mgmtHosts = append(mgmtHosts, "https://"+host)
		} else {
			mgmtHosts = append(mgmtHosts, "http://"+host)
		}
	}

	tlsConfig := c.agentConfig.TlsConfig
	return &ClusterManager{
		hosts:    mgmtHosts,
		username: userPass.Username,
		password: userPass.Password,
		httpCli: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
	}
}

// StreamingBucket represents a bucket connection used for streaming data over DCP.
type StreamingBucket struct {
	client *gocbcore.Agent
}

// IoRouter returns the underlying gocb agent managing connections.
func (b *StreamingBucket) IoRouter() *gocbcore.Agent {
	return b.client
}

// OpenStreamingBucket opens a new connection to the specified bucket for the purpose of streaming data.
func (c *Cluster) OpenStreamingBucket(streamName, bucket, password string) (*StreamingBucket, error) {
	username := bucket
	if password == "" {
		if c.auth != nil {
			userPass := c.auth.bucketMemd(bucket)
			username = userPass.Username
			password = userPass.Password
		}
	}

	agentConfig, err := c.makeAgentConfig(bucket, username, password, false)
	if err != nil {
		return nil, err
	}
	cli, err := gocbcore.CreateDcpAgent(agentConfig, streamName, 0)
	if err != nil {
		return nil, err
	}

	return &StreamingBucket{
		client: cli,
	}, nil
}

func (c *Cluster) randomBucket() (*Bucket, error) {
	c.clusterLock.RLock()
	if len(c.bucketList) == 0 {
		c.clusterLock.RUnlock()
		return nil, ErrNoOpenBuckets
	}
	bucket := c.bucketList[0]
	c.clusterLock.RUnlock()
	return bucket, nil
}
