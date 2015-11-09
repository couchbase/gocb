package gocbcore

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net/http"
	"math/rand"
	"time"
	"sync"
)

// This class represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	bucket    string
	password  string
	tlsConfig *tls.Config
	initFn    memdInitFunc
	useMutationTokens bool

	routingInfo routeDataPtr
	numVbuckets int

	serverFailuresLock sync.Mutex
	serverFailures map[string]time.Time

	httpCli *http.Client

	serverConnectTimeout time.Duration
	serverWaitTimeout time.Duration
}

// The timeout for each server connection, including all authentication steps.
func (c *Agent) ServerConnectTimeout() time.Duration {
	return c.serverConnectTimeout
}
func (c *Agent) SetServerConnectTimeout(timeout time.Duration) {
	c.serverConnectTimeout = timeout
}

// Returns a pre-configured HTTP Client for communicating with
//   Couchbase Server.  You must still specify authentication
//   information for any dispatched requests.
func (c *Agent) HttpClient() *http.Client {
	return c.httpCli
}

type AuthFunc func(client AuthClient, deadline time.Time) error

type AgentConfig struct {
	MemdAddrs   []string
	HttpAddrs   []string
	TlsConfig   *tls.Config
	BucketName  string
	Password    string
	AuthHandler AuthFunc
	UseMutationTokens bool

	ConnectTimeout       time.Duration
	ServerConnectTimeout time.Duration
}

func CreateAgent(config *AgentConfig) (*Agent, error) {
	initFn := func(pipeline *memdPipeline, deadline time.Time) error {
		return config.AuthHandler(&authClient{pipeline}, deadline)
	}
	return createAgent(config, initFn)
}

func CreateDcpAgent(config *AgentConfig, dcpStreamName string) (*Agent, error) {
	// We wrap the authorization system to force DCP channel opening
	//   as part of the "initialization" for any servers.
	dcpInitFn := func(pipeline *memdPipeline, deadline time.Time) error {
		if err := config.AuthHandler(&authClient{pipeline}, deadline); err != nil {
			return err
		}
		return doOpenDcpChannel(pipeline, dcpStreamName, deadline)
	}
	return createAgent(config, dcpInitFn)
}

func createAgent(config *AgentConfig, initFn memdInitFunc) (*Agent, error) {
	c := &Agent{
		bucket:    config.BucketName,
		password:  config.Password,
		tlsConfig: config.TlsConfig,
		initFn:    initFn,
		httpCli: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: config.TlsConfig,
			},
		},
		useMutationTokens: config.UseMutationTokens,
		serverFailures: make(map[string]time.Time),
		serverConnectTimeout: config.ServerConnectTimeout,
		serverWaitTimeout: 5 * time.Second,
	}

	deadline := time.Now().Add(config.ConnectTimeout)
	if err := c.connect(config.MemdAddrs, config.HttpAddrs, deadline); err != nil {
		return nil, err
	}
	return c, nil
}

type AuthErrorType interface {
	AuthError() bool
}

func isAuthError(err error) bool {
	te, ok := err.(interface {
		AuthError() bool
	})
	return ok && te.AuthError()
}

func (c *Agent) cccpLooper() {
	tickTime := time.Second * 10
	maxWaitTime := time.Second * 3

	logDebugf("CCCP Looper starting.")

	for {
		// Wait 10 seconds
		time.Sleep(tickTime)

		routingInfo := c.routingInfo.get()
		if routingInfo == nil {
			// If we have a blank routingInfo, it indicates the client is shut down.
			break;
		}

		numServers := len(routingInfo.servers)
		if numServers == 0 {
			logDebugf("CCCPPOLL: No servers")
			continue
		}

		srvIdx := rand.Intn(numServers)
		srv := routingInfo.servers[srvIdx]

		// Force config refresh from random node
		cccpBytes, err := doCccpRequest(srv, time.Now().Add(maxWaitTime))
		if err != nil {
			logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
			continue
		}

		bk, err := parseConfig(cccpBytes, srv.Hostname())
		if err != nil {
			logDebugf("CCCPPOLL: Failed to parse CCCP config. %v", err)
			continue
		}

		logDebugf("CCCPPOLL: Received new config")
		c.updateConfig(bk)
	}
}

func (c *Agent) connect(memdAddrs, httpAddrs []string, deadline time.Time) error {
	logDebugf("Attempting to connect...")

	for _, thisHostPort := range memdAddrs {
		logDebugf("Trying server at %s", thisHostPort)

		srvDeadlineTm := time.Now().Add(c.serverConnectTimeout)
		if srvDeadlineTm.After(deadline) {
			srvDeadlineTm = deadline
		}

		srv := CreateMemdPipeline(thisHostPort)

		logDebugf("Trying to connect")
		err := c.connectPipeline(srv, srvDeadlineTm)
		if err != nil {
			if isAuthError(err) {
				return err
			}
			logDebugf("Connecting failed! %v", err)
			continue
		}

		logDebugf("Attempting to request CCCP configuration")
		cccpBytes, err := doCccpRequest(srv, srvDeadlineTm)
		if err != nil {
			logDebugf("Failed to retrieve CCCP config. %v", err)
			srv.Close()
			continue
		}

		bk, err := parseConfig(cccpBytes, srv.Hostname())
		if err != nil {
			srv.Close()
			continue
		}

		if !bk.supportsCccp() {
			// No CCCP support, fall back to HTTP!
			srv.Close()
			break
		}

		routeCfg := buildRouteConfig(bk, c.IsSecure())
		if !routeCfg.IsValid() {
			// Something is invalid about this config, keep trying
			srv.Close()
			continue
		}

		logDebugf("Successfully connected")

		// Build some fake routing data, this is used to essentially 'pass' the
		//   server connection we already have over to the config update function.
		c.routingInfo.update(nil, &routeData{
			servers: []*memdPipeline{srv},
		})

		c.numVbuckets = len(routeCfg.vbMap)
		c.applyConfig(routeCfg)

		srv.SetHandlers(c.handleServerNmv, c.handleServerDeath)

		go c.cccpLooper();

		return nil
	}

	signal := make(chan error, 1)

	var epList []string
	for _, hostPort := range httpAddrs {
		if !c.IsSecure() {
			epList = append(epList, fmt.Sprintf("http://%s", hostPort))
		} else {
			epList = append(epList, fmt.Sprintf("https://%s", hostPort))
		}
	}
	c.routingInfo.update(nil, &routeData{
		mgmtEpList: epList,
	})

	var bk *cfgBucket

	logDebugf("Starting HTTP looper! %v", epList)
	go c.httpLooper(func(cfg *cfgBucket, err error) {
		bk = cfg
		signal <- err
	})

	err := <-signal
	if err != nil {
		return err
	}

	routeCfg := buildRouteConfig(bk, c.IsSecure())
	c.numVbuckets = len(routeCfg.vbMap)
	c.applyConfig(routeCfg)

	return nil
}

func (agent *Agent) Close() {
	// Clear the routingInfo so no new operations are performed
	//   and retrieve the last active routing configuration
	routingInfo := agent.routingInfo.clear()

	// Loop all the currently running servers and drain their
	//   requests as errors (this also closes the server conn).
	for _, s := range routingInfo.servers {
		s.Drain(func (req *memdQRequest) {
			req.Callback(nil, shutdownError{})
		})
	}

	// Clear any extraneous queues that may still contain
	//   requests which are not pending on a server queue.
	if routingInfo.deadQueue != nil {
		routingInfo.deadQueue.Drain(func (req *memdQRequest) {
			req.Callback(nil, shutdownError{})
		}, nil)
	}
	if routingInfo.waitQueue != nil {
		routingInfo.waitQueue.Drain(func (req *memdQRequest) {
			req.Callback(nil, shutdownError{})
		}, nil)
	}
}

func (c *Agent) IsSecure() bool {
	return c.tlsConfig != nil
}

func (c *Agent) KeyToVbucket(key []byte) uint16 {
	return uint16(cbCrc(key) % uint32(c.NumVbuckets()))
}

func (c *Agent) NumVbuckets() int {
	return c.numVbuckets
}

func (c *Agent) NumReplicas() int {
	return len(c.routingInfo.get().vbMap[0]) - 1
}

func (agent *Agent) CapiEps() []string {
	routingInfo := agent.routingInfo.get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.capiEpList
}

func (agent *Agent) MgmtEps() []string {
	routingInfo := agent.routingInfo.get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.mgmtEpList
}

func (agent *Agent) N1qlEps() []string {
	routingInfo := agent.routingInfo.get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.n1qlEpList
}

func doCccpRequest(pipeline *memdPipeline, deadline time.Time) ([]byte, error) {
	resp, err := pipeline.ExecuteRequest(&memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGetClusterConfig,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
	}, deadline)
	if err != nil {
		return nil, err
	}

	return resp.Value, nil
}

func doOpenDcpChannel(pipeline *memdPipeline, streamName string, deadline time.Time) error {
	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], 1)

	_, err := pipeline.ExecuteRequest(&memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdDcpOpenConnection,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      []byte(streamName),
			Value:    nil,
		},
	}, deadline)
	if err != nil {
		return err
	}

	return nil
}
