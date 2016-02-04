package gocb

import (
	"crypto/tls"
	"errors"
	"github.com/couchbase/gocb/gocbcore"
	"net/http"
	"time"
)

type Cluster struct {
	spec                 connSpec
	connectTimeout       time.Duration
	serverConnectTimeout time.Duration
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

func (c *Cluster) makeAgentConfig(bucket, password string) *gocbcore.AgentConfig {
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
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
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
	}
}

func (c *Cluster) OpenBucket(bucket, password string) (*Bucket, error) {
	agentConfig := c.makeAgentConfig(bucket, password)
	return createBucket(agentConfig)
}

func (c *Cluster) Manager(username, password string) *ClusterManager {
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
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	return &ClusterManager{
		hosts:    mgmtHosts,
		username: username,
		password: password,
		httpCli: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
	}
}

type StreamingBucket struct {
	client *gocbcore.Agent
}

func (b *StreamingBucket) IoRouter() *gocbcore.Agent {
	return b.client
}

func (c *Cluster) OpenStreamingBucket(streamName, bucket, password string) (*StreamingBucket, error) {
	cli, err := gocbcore.CreateDcpAgent(c.makeAgentConfig(bucket, password), streamName)
	if err != nil {
		return nil, err
	}

	return &StreamingBucket{
		client: cli,
	}, nil
}
