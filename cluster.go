package gocb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/couchbase/gocb/gocbcore"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

type Cluster struct {
	spec                 connSpec
	auth                 Authenticator
	connectTimeout       time.Duration
	serverConnectTimeout time.Duration
	n1qlTimeout          time.Duration
	tlsConfig            *tls.Config

	clusterLock sync.RWMutex
	queryCache  map[string]*n1qlCache
	bucketList  []*Bucket
}

func Connect(connSpecStr string) (*Cluster, error) {
	spec, err := parseConnSpec(connSpecStr)
	if err != nil {
		return nil, err
	}
	if spec.Bucket != "" {
		return nil, errors.New("Connection string passed to Connect() must not have any bucket specified!")
	}

	csResolveDnsSrv(&spec)

	// Get bootstrap_on option to determine which, if any, of the bootstrap nodes should be cleared
	switch spec.Options.Get("bootstrap_on") {
	case "http":
		spec.MemcachedHosts = nil
		if len(spec.HttpHosts) == 0 {
			return nil, errors.New("bootstrap_on=http but no HTTP hosts in connection string")
		}
	case "cccp":
		spec.HttpHosts = nil
		if len(spec.MemcachedHosts) == 0 {
			return nil, errors.New("bootstrap_on=cccp but no CCCP/Memcached hosts in connection string")
		}
	case "both":
	case "":
		// Do nothing
		break
	default:
		return nil, errors.New("bootstrap_on={http,cccp,both}")
	}

	cluster := &Cluster{
		spec:                 spec,
		connectTimeout:       60000 * time.Millisecond,
		serverConnectTimeout: 7000 * time.Millisecond,
		n1qlTimeout:          75 * time.Second,

		queryCache: make(map[string]*n1qlCache),
	}
	return cluster, nil
}

func (c *Cluster) ConnectTimeout() time.Duration {
	return c.connectTimeout
}
func (c *Cluster) SetConnectTimeout(timeout time.Duration) {
	c.connectTimeout = timeout
}
func (c *Cluster) ServerConnectTimeout() time.Duration {
	return c.serverConnectTimeout
}
func (c *Cluster) SetServerConnectTimeout(timeout time.Duration) {
	c.serverConnectTimeout = timeout
}

func specToHosts(spec connSpec) ([]string, []string, bool) {
	var memdHosts []string
	var httpHosts []string

	for _, specHost := range spec.HttpHosts {
		httpHosts = append(httpHosts, specHost.HostPort())
	}

	for _, specHost := range spec.MemcachedHosts {
		memdHosts = append(memdHosts, specHost.HostPort())
	}

	return memdHosts, httpHosts, spec.Scheme.IsSSL()
}

func (c *Cluster) makeAgentConfig(bucket, password string) (*gocbcore.AgentConfig, error) {
	authFn := func(srv gocbcore.AuthClient, deadline time.Time) error {
		// Build PLAIN auth data
		userBuf := []byte(bucket)
		passBuf := []byte(password)
		authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
		authData[0] = 0
		copy(authData[1:], userBuf)
		authData[1+len(userBuf)] = 0
		copy(authData[1+len(userBuf)+1:], passBuf)

		// Execute PLAIN authentication
		_, err := srv.ExecSaslAuth([]byte("PLAIN"), authData, deadline)

		return err
	}

	memdHosts, httpHosts, isSslHosts := specToHosts(c.spec)

	var tlsConfig *tls.Config
	if isSslHosts {

		certpath := c.spec.Options.Get("certpath")

		tlsConfig = &tls.Config{}
		if certpath == "" {
			tlsConfig.InsecureSkipVerify = true
		} else {
			cacert, err := ioutil.ReadFile(certpath)
			if err != nil {
				return nil, err
			}

			roots := x509.NewCertPool()
			ok := roots.AppendCertsFromPEM(cacert)
			if !ok {
				return nil, ErrInvalidCert
			}
			tlsConfig.RootCAs = roots
		}
	}

	return &gocbcore.AgentConfig{
		MemdAddrs:            memdHosts,
		HttpAddrs:            httpHosts,
		TlsConfig:            tlsConfig,
		BucketName:           bucket,
		Password:             password,
		AuthHandler:          authFn,
		UseMutationTokens:    false,
		ConnectTimeout:       c.connectTimeout,
		ServerConnectTimeout: c.serverConnectTimeout,
	}, nil
}

func (c *Cluster) Authenticate(auth Authenticator) error {
	c.auth = auth
	return nil
}

func (c *Cluster) OpenBucket(bucket, password string) (*Bucket, error) {
	if password == "" {
		if c.auth != nil {
			password = c.auth.bucketMemd(bucket)
		}
	}

	agentConfig, err := c.makeAgentConfig(bucket, password)
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

func (c *Cluster) Manager(username, password string) *ClusterManager {
	userPass := userPassPair{username, password}
	if username == "" || password == "" {
		if c.auth != nil {
			userPass = c.auth.clusterMgmt()
		}
	}

	_, httpHosts, isSslHosts := specToHosts(c.spec)
	var mgmtHosts []string

	for _, host := range httpHosts {
		if isSslHosts {
			mgmtHosts = append(mgmtHosts, "https://"+host)
		} else {
			mgmtHosts = append(mgmtHosts, "http://"+host)
		}
	}

	var tlsConfig *tls.Config
	if isSslHosts {
		tlsConfig = c.tlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
	}

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

func (c *Cluster) N1qlTimeout() time.Duration {
	return c.n1qlTimeout
}
func (c *Cluster) SetN1qlTimeout(timeout time.Duration) {
	c.n1qlTimeout = timeout
}

func (c *Cluster) InvalidateQueryCache() {
	c.clusterLock.Lock()
	c.queryCache = make(map[string]*n1qlCache)
	c.clusterLock.Unlock()
}

type StreamingBucket struct {
	client *gocbcore.Agent
}

func (b *StreamingBucket) IoRouter() *gocbcore.Agent {
	return b.client
}

func (c *Cluster) OpenStreamingBucket(streamName, bucket, password string) (*StreamingBucket, error) {
	agentConfig, err := c.makeAgentConfig(bucket, password)
	if err != nil {
		return nil, err
	}
	cli, err := gocbcore.CreateDcpAgent(agentConfig, streamName)
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
