package gocbcore

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net/http"
)

// This class represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	bucket    string
	password  string
	tlsConfig *tls.Config
	initFn    memdInitFunc

	routingInfo routeDataPtr
	numVbuckets int

	httpCli *http.Client
}

type AuthFunc func(AuthClient) error

func CreateDcpAgent(memdAddrs, httpAddrs []string, tlsConfig *tls.Config, bucketName, password string, authFn AuthFunc, dcpStreamName string) (*Agent, error) {
	// We wrap the authorization system to force DCP channel opening
	//   as part of the "initialization" for any servers.
	dcpInitFn := func(pipeline *memdPipeline) error {
		if err := authFn(&authClient{pipeline}); err != nil {
			return err
		}
		return doOpenDcpChannel(pipeline, dcpStreamName)
	}
	return createAgent(memdAddrs, httpAddrs, tlsConfig, bucketName, password, dcpInitFn)
}

func CreateAgent(memdAddrs, httpAddrs []string, tlsConfig *tls.Config, bucketName, password string, authFn AuthFunc) (*Agent, error) {
	initFn := func(pipeline *memdPipeline) error {
		return authFn(&authClient{pipeline})
	}
	return createAgent(memdAddrs, httpAddrs, tlsConfig, bucketName, password, initFn)
}

func createAgent(memdAddrs, httpAddrs []string, tlsConfig *tls.Config, bucketName, password string, initFn memdInitFunc) (*Agent, error) {
	c := &Agent{
		bucket:    bucketName,
		password:  password,
		tlsConfig: tlsConfig,
		initFn:    initFn,
		httpCli:   &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}},
	}
	if err := c.connect(memdAddrs, httpAddrs); err != nil {
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

func (c *Agent) connect(memdAddrs, httpAddrs []string) error {
	logDebugf("Attempting to connect...")

	for _, thisHostPort := range memdAddrs {
		logDebugf("Trying server at %s", thisHostPort)

		srv := CreateMemdPipeline(thisHostPort)

		logDebugf("Trying to connect")
		err := c.connectPipeline(srv)
		if err != nil {
			if isAuthError(err) {
				return err
			}
			logDebugf("Connecting failed! %v", err)
			continue
		}

		logDebugf("Attempting to request CCCP configuration")
		cccpBytes, err := doCccpRequest(srv)
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

		logDebugf("Successfully connected\n")

		// Build some fake routing data, this is used to essentially 'pass' the
		//   server connection we already have over to the config update function.
		c.routingInfo.update(nil, &routeData{
			servers: []*memdPipeline{srv},
		})

		routeCfg := buildRouteConfig(bk, c.IsSecure())
		c.numVbuckets = len(routeCfg.vbMap)
		c.applyConfig(routeCfg)

		srv.SetHandlers(c.handleServerNmv, c.handleServerDeath)

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

func (agent *Agent) CloseTest() {
	routingInfo := agent.routingInfo.get()
	for _, s := range routingInfo.servers {
		s.Close()
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

func (agent *Agent) CapiEps() []string {
	return agent.routingInfo.get().capiEpList
}

func (agent *Agent) MgmtEps() []string {
	return agent.routingInfo.get().mgmtEpList
}

func doCccpRequest(pipeline *memdPipeline) ([]byte, error) {
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
	})
	if err != nil {
		return nil, err
	}

	return resp.Value, nil
}

func doOpenDcpChannel(pipeline *memdPipeline, streamName string) error {
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
	})
	if err != nil {
		return err
	}

	return nil
}
