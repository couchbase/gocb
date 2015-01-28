package gocouchbase

import (
	"crypto/tls"
	"fmt"
	"github.com/couchbaselabs/gocouchbaseio"
	"net/http"
	"time"
)

type Cluster struct {
	spec              connSpec
	connectionTimeout time.Duration
}

func Connect(connSpecStr string) (*Cluster, error) {
	spec := parseConnSpec(connSpecStr)
	if spec.Scheme == "" {
		spec.Scheme = "http"
	}
	if spec.Scheme != "couchbase" && spec.Scheme != "couchbases" && spec.Scheme != "http" {
		panic("Unsupported Scheme!")
	}
	csResolveDnsSrv(&spec)
	cluster := &Cluster{
		spec:              spec,
		connectionTimeout: 10000 * time.Millisecond,
	}
	return cluster, nil
}

func specToHosts(spec connSpec) ([]string, []string, bool) {
	var memdHosts []string
	var httpHosts []string
	isHttpHosts := spec.Scheme == "http"
	isSslHosts := spec.Scheme == "couchbases"
	for _, specHost := range spec.Hosts {
		cccpPort := specHost.Port
		httpPort := specHost.Port
		if isHttpHosts || cccpPort == 0 {
			if !isSslHosts {
				cccpPort = 11210
			} else {
				cccpPort = 11207
			}
		}
		if !isHttpHosts || httpPort == 0 {
			if !isSslHosts {
				httpPort = 8091
			} else {
				httpPort = 18091
			}
		}

		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, cccpPort))
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, httpPort))
	}

	return memdHosts, httpHosts, isSslHosts
}

func (c *Cluster) OpenBucket(bucket, password string) (*Bucket, error) {
	memdHosts, httpHosts, isSslHosts := specToHosts(c.spec)

	authFn := func(srv gocouchbaseio.AuthClient) error {
		// Build PLAIN auth data
		userBuf := []byte(bucket)
		passBuf := []byte(password)
		authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
		authData[0] = 0
		copy(authData[1:], userBuf)
		authData[1+len(userBuf)] = 0
		copy(authData[1+len(userBuf)+1:], passBuf)

		// Execute PLAIN authentication
		_, err := srv.ExecSaslAuth([]byte("PLAIN"), authData)

		return err
	}

	cli, err := gocouchbaseio.CreateAgent(memdHosts, httpHosts, isSslHosts, nil, bucket, authFn)
	if err != nil {
		return nil, err
	}

	return &Bucket{
		client: cli,
		httpCli: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		},
	}, nil
}

type StreamingBucket struct {
	client *gocouchbaseio.Agent
}

func (b *StreamingBucket) IoRouter() *gocouchbaseio.Agent {
	return b.client
}

func (c *Cluster) OpenStreamingBucket(streamName, bucket, password string) (*StreamingBucket, error) {
	var memdHosts []string
	var httpHosts []string
	isHttpHosts := c.spec.Scheme == "http"
	isSslHosts := c.spec.Scheme == "couchbases"
	for _, specHost := range c.spec.Hosts {
		if specHost.Port == 0 {
			if !isHttpHosts {
				if !isSslHosts {
					specHost.Port = 11210
				} else {
					specHost.Port = 11207
				}
			} else {
				panic("HTTP configuration not yet supported")
				//specHost.Port = 8091
			}
		}
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	authFn := func(srv gocouchbaseio.AuthClient) error {
		// Build PLAIN auth data
		userBuf := []byte(bucket)
		passBuf := []byte(password)
		authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
		authData[0] = 0
		copy(authData[1:], userBuf)
		authData[1+len(userBuf)] = 0
		copy(authData[1+len(userBuf)+1:], passBuf)

		// Execute PLAIN authentication
		_, err := srv.ExecSaslAuth([]byte("PLAIN"), authData)

		return err
	}

	cli, err := gocouchbaseio.CreateDcpAgent(memdHosts, httpHosts, isSslHosts, nil, bucket, authFn, streamName)
	if err != nil {
		return nil, err
	}

	return &StreamingBucket{
		client: cli,
	}, nil
}
