package couchbase

import "fmt"
import "strconv"
import "io"
import "encoding/binary"
import "encoding/hex"
import "log"
import "net"
import "strings"
import "net/http"
import "sync/atomic"
import "encoding/json"
import "math/rand"
import "time"
import "unsafe"

// The response to a memcached request
type memdResponse struct {
	Magic    commandMagic
	Opcode   commandCode
	Datatype uint8
	Status   statusCode
	Cas      uint64
	Key      []byte
	Extras   []byte
	Value    []byte

	Opaque uint32
}

// A memcached request, along with neccessary meta-information used for routing
// and response handling.
type memdRequest struct {
	Magic    commandMagic
	Opcode   commandCode
	Datatype uint8
	Opaque   uint32
	VBucket  uint16
	Cas      uint64
	Key      []byte
	Extras   []byte
	Value    []byte

	VBHash        uint32
	ReplicaIdx    int
	ResponseCount uint32
	RespChan      chan *memdResponse
}

// Represents a single server in a Couchbase cluster.
type bucketServer struct {
	addr       string
	conn       *net.TCPConn
	isDead     bool
	reqsCh     chan *memdRequest
	recvHdrBuf []byte
}

// This class represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type BaseConnection struct {
	connSpec connSpec

	httpCli *http.Client
	config  *cfgBucket
	vbMap   *cfgVBucketServerMap
	servers []*bucketServer
	capiEps unsafe.Pointer
	mgmtEps unsafe.Pointer

	opCounter uint32
	opMap     map[uint32]*memdRequest

	configCh     chan *cfgBucket
	configWaitCh chan *memdRequest
	requestCh    chan *memdRequest
	responseCh   chan *memdResponse
	timeedoutCh  chan *memdRequest
	deadServerCh chan *bucketServer

	operationTimeout time.Duration

	Stats struct {
		NumConfigUpdate  uint64
		NumServerConnect uint64
		NumServerLost    uint64
		NumServerRemoved uint64
		NumOpRelocated   uint64
		NumOp            uint64
		NumOpResp        uint64
		NumOpTimeout     uint64
	}
}

func (c *BaseConnection) KillTest() {
	for _, srv := range c.servers {
		srv.conn.Close()
	}
}

// Creates a new bucketServer object for the specified address.
func (c *BaseConnection) createServer(addr string) *bucketServer {
	return &bucketServer{
		addr:       addr,
		reqsCh:     make(chan *memdRequest),
		recvHdrBuf: make([]byte, 24),
	}
}

// Creates a new memdRequest object to perform PLAIN authentication to a server.
func makePlainAuthRequest(bucket, password string) *memdRequest {
	userBuf := []byte(bucket)
	passBuf := []byte(password)

	authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
	authData[0] = 0
	copy(authData[1:], userBuf)
	authData[1+len(userBuf)] = 0
	copy(authData[1+len(userBuf)+1:], passBuf)

	authMech := []byte("PLAIN")

	return &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdSASLAuth,
		Datatype: 0,
		VBucket:  0,
		Cas:      0,
		Extras:   nil,
		Key:      authMech,
		Value:    authData,
	}
}

// Creates a new memdRequest object to perform a configuration request.
func makeCccpRequest() *memdRequest {
	return &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdGetClusterConfig,
		Datatype: 0,
		VBucket:  0,
		Cas:      0,
		Extras:   nil,
		Key:      nil,
		Value:    nil,
	}
}

// Dials and Authenticates a bucketServer object to the cluster.
func (s *bucketServer) connect(bucket, password string) error {
	fmt.Printf("Resolvinggg %s\n", s.addr)

	tcpAddr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		return err
	}
	fmt.Printf("Resolution is %v\n", tcpAddr)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	fmt.Printf("Dial Complete\n")

	s.conn = conn

	authResp, err := s.doRequest(makePlainAuthRequest(bucket, password))
	if authResp.Status != success {
		return clientError{"Authentication failure."}
	}

	return nil
}

func (s *bucketServer) doRequest(req *memdRequest) (*memdResponse, error) {
	err := s.writeRequest(req)
	if err != nil {
		return nil, err
	}

	return s.readResponse()
}

func (s *bucketServer) readResponse() (*memdResponse, error) {
	hdrBuf := s.recvHdrBuf

	_, err := io.ReadFull(s.conn, hdrBuf)
	if err != nil {
		log.Printf("Error reading: %v", err)
		return nil, err
	}
	fmt.Println("Received Header")
	fmt.Println(hex.Dump(hdrBuf[:24]))

	bodyLen := int(binary.BigEndian.Uint32(hdrBuf[8:]))

	bodyBuf := make([]byte, bodyLen)
	_, err = io.ReadFull(s.conn, bodyBuf)
	if err != nil {
		log.Printf("Error reading: %v", err)
		return nil, err
	}
	fmt.Println("Received Body")

	keyLen := int(binary.BigEndian.Uint16(hdrBuf[2:]))
	extLen := int(hdrBuf[4])

	return &memdResponse{
		Magic:    commandMagic(hdrBuf[0]),
		Opcode:   commandCode(hdrBuf[1]),
		Datatype: hdrBuf[5],
		Status:   statusCode(binary.BigEndian.Uint16(hdrBuf[6:])),
		Opaque:   binary.BigEndian.Uint32(hdrBuf[12:]),
		Cas:      binary.BigEndian.Uint64(hdrBuf[16:]),
		Extras:   bodyBuf[:extLen],
		Key:      bodyBuf[extLen : extLen+keyLen],
		Value:    bodyBuf[extLen+keyLen:],
	}, nil
}

func (s *bucketServer) writeRequest(req *memdRequest) error {
	extLen := len(req.Extras)
	keyLen := len(req.Key)
	valLen := len(req.Value)

	buffer := make([]byte, 24+keyLen+extLen+valLen)

	buffer[0] = uint8(req.Magic)
	buffer[1] = uint8(req.Opcode)
	binary.BigEndian.PutUint16(buffer[2:], uint16(keyLen))
	buffer[4] = byte(extLen)
	buffer[5] = req.Datatype
	binary.BigEndian.PutUint16(buffer[6:], uint16(req.VBucket))
	binary.BigEndian.PutUint32(buffer[8:], uint32(len(buffer)-24))
	binary.BigEndian.PutUint32(buffer[12:], req.Opaque)
	binary.BigEndian.PutUint64(buffer[16:], req.Cas)

	copy(buffer[24:], req.Extras)
	copy(buffer[24+extLen:], req.Key)
	copy(buffer[24+extLen+keyLen:], req.Value)

	_, err := s.conn.Write(buffer)
	return err
}

func (c *BaseConnection) serverReader(s *bucketServer) {
	for {
		resp, err := s.readResponse()
		fmt.Printf("Got a response %v\n", resp)

		if err != nil {
			c.deadServerCh <- s
			return
		}

		c.responseCh <- resp
	}
}

func (c *BaseConnection) serverWriter(s *bucketServer) {
	for {
		req := <-s.reqsCh
		fmt.Printf("writing request %v\n", s, req)
		s.writeRequest(req)
	}
}

func (c *BaseConnection) serverConnectRun(s *bucketServer, bucket, password string) {
	atomic.AddUint64(&c.Stats.NumServerConnect, 1)
	err := s.connect(bucket, password)
	if err != nil {
		c.deadServerCh <- s
		return
	}

	c.serverRun(s)
}

func (c *BaseConnection) serverRun(s *bucketServer) {
	go c.serverReader(s)
	go c.serverWriter(s)
}

func (c *BaseConnection) configRunner() {
	// HTTP Polling
	//  - run http polling
	// CCCP Polling
	//  c.globalRequestCh <- &memdRequest{
	//    ReplicaIdx: -1 // No Specific Server
	//  }
}

// Routes a request to the specific server it is destined for.  It also assigns an Opaque
//  value for the request and adds it to the operation map.
// This method must only be invoked by the 'primary goroutine'.
func (c *BaseConnection) routeRequest(req *memdRequest, rerouted bool) {
	fmt.Printf("Got request %v\n", req)

	vbId := uint16(req.VBHash % uint32(len(c.vbMap.VBucketMap)))
	repIdx := req.ReplicaIdx

	if repIdx >= c.vbMap.NumReplicas {
		panic("Invalid replica index specified")
	}

	if req.ReplicaIdx >= 0 {
		req.VBucket = vbId

		// Find the server index for this
		srvIdx := c.vbMap.VBucketMap[vbId][repIdx]
		// Randomly miss-locate a server
		if rand.Uint32() < 0x7fffffff {
			fmt.Printf("Relocating for no reason\n")
			srvIdx = rand.Int() % len(c.servers)
		}

		// Grab the appropriate server
		myServer := c.servers[srvIdx]

		fmt.Printf("Found Server %v\n", myServer)

		opIdx := c.opCounter
		c.opMap[opIdx] = req
		c.opCounter++
		fmt.Printf("Assigned op index %d\n", opIdx)

		req.Opaque = opIdx
		req.ResponseCount = 1

		myServer.reqsCh <- req
	}

	// myServer := c.servers[...]
	// if (there is no server to send to) {
	//   configWaitCh <- req
	//   return
	// }

	// Assign an Opaque
	// opIdx := opCounter
	// opCounter++
	// opMap[opIdx] = req
	// req.Opaque = opIdx

	// Server specific commands
	//  - Stats - Write to all
	//  for (serverList as server) {
	//    server.reqsCh <- req
	//  }
	//  - CCCP Config Get
	//  - Observe
	//

	//if req.ReplicaIdx == -1 {
	// req.ResponseCount = serverList.count
	// req.VBucket = req.VBHash % c.VBucketCount
	// foreach{ServerList as Server) {
	//   myServer.reqsCh <- req
	// }
	//} else {
	//	if req.Opcode == GET_CLUSTER_CONFIG {
	//
	//	} else if req.Opcode == STAT {

	//	}
	//}

	// Route to correct Vbucket

	// How does the connection
}

func (c *BaseConnection) globalHandler() {
	fmt.Printf("Global Handler Running\n")
	for {
		fmt.Printf("Global Handler Loop\n")

		select {
		case req := <-c.requestCh:
			c.routeRequest(req, false)

		case resp := <-c.responseCh:
			fmt.Printf("Handling response %v\n", resp)

			req := c.opMap[resp.Opaque]
			fmt.Printf("  for request %v\n", req)

			if req != nil {
				delete(c.opMap, resp.Opaque)

				if resp.Status == notMyVBucket {
					// Maybe update config?
					atomic.AddUint64(&c.Stats.NumOpRelocated, 1)

					// Check if we can parse this NMV value as a config
					bk := new(cfgBucket)
					err := json.Unmarshal(resp.Value, bk)
					if err == nil {
						c.updateConfig(bk)
					}

					// Re-dispatch the operation for processing
					// BUG(brett19): This will potentially cause operations to loop around, while
					//  a server is consistently responding with NMV, but no new config exists yet.
					go func() {
						c.requestCh <- req
					}()
				} else {
					req.RespChan <- resp
				}
			}

		case req := <-c.timeedoutCh:
			// Maybe check the server for too many failures, and trigger config reget
			delete(c.opMap, req.Opaque)

		case deader := <-c.deadServerCh:
			// Server died, do something about it
			deader.isDead = true

			// Currently we just forward the existing configuration back through the pipe...
			// TODO(brett19): We need to actually request a new configuration when a server dies.
			c.updateConfig(c.config)

		case config := <-c.configCh:
			c.updateConfig(config)

		}
	}
}

/*
func (c *BaseConnection) Observe(key string) {
	keyBuf := []byte(key)
	keyHash := cbCrc(keyBuf)

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   OBSERVE,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      keyBuf,
		Value:    nil,
		RespChan: make(chan *memdResponse, 1),
		VBHash:   keyHash,

		ReplicaIdx: -1,
	}
	c.requestCh <- req

	responseCount := 0
	for {
		 <- req.RespChan
		responseCount++
		// Save this response for a moment

		if (req.ResponseCount >= responseCount) {
			break;
		}
	}
}
*/

// Performs a simple dispatch of a request which receives a single reply.  A timeout error will
//  occur if the operation takes longer than configured operation timeout.  This method can be
//  invoked from any goroutine.
func (c *BaseConnection) dispatchSimpleReq(req *memdRequest) (*memdResponse, Error) {
	atomic.AddUint64(&c.Stats.NumOp, 1)
	c.requestCh <- req

	select {
	case resp := <-req.RespChan:
		atomic.AddUint64(&c.Stats.NumOpResp, 1)
		return resp, nil
	case <-time.After(c.operationTimeout):
		atomic.AddUint64(&c.Stats.NumOpTimeout, 1)
		c.timeedoutCh <- req
		return nil, timeoutError{}
	}
}

// Accepts a cfgBucket object representing a cluster configuration and rebuilds the server list
//  along with any routing information for the Client.  This method must only be invoked from
//  the 'primary goroutine'.
func (c *BaseConnection) updateConfig(bk *cfgBucket) {
	atomic.AddUint64(&c.Stats.NumConfigUpdate, 1)

	c.config = bk

	var newServers []*bucketServer
	for _, hostPort := range bk.VBSMJson.ServerList {
		var newServer *bucketServer = nil

		for _, oldServer := range c.servers {
			if !oldServer.isDead && oldServer.addr == hostPort {
				newServer = oldServer
				break
			}
		}

		if newServer == nil {
			newServer = c.createServer(hostPort)
			go c.serverConnectRun(newServer, bk.Name, "")
		}

		newServers = append(newServers, newServer)
	}

	oldServers := c.servers
	c.servers = newServers

	for _, oldServer := range oldServers {
		found := false
		for _, newServer := range newServers {
			if newServer == oldServer {
				found = true
				break
			}
		}
		if !found {
			queueCleared := false
			for !queueCleared {
				select {
				case req := <-oldServer.reqsCh:
					c.routeRequest(req, false)
				default:
					queueCleared = true
					break
				}
			}
		}
	}

	var capiEps []string
	capiEps = append(capiEps, "http://test.com/")

	var mgmtEps []string
	mgmtEps = append(mgmtEps, "http://test.com/")

	c.vbMap = &bk.VBSMJson
	atomic.StorePointer(&c.capiEps, unsafe.Pointer(&capiEps))
	atomic.StorePointer(&c.mgmtEps, unsafe.Pointer(&mgmtEps))
}

func CreateBaseConnection(connSpecStr string, bucket, password string) (*BaseConnection, Error) {
	spec := parseConnSpec(connSpecStr)

	memdAddrs := spec.Hosts
	if spec.Scheme == "http" {
		memdAddrs = guessMemdHosts(spec)
	}

	fmt.Printf("ADDRs: %v\n", memdAddrs)

	c := &BaseConnection{
		httpCli:          &http.Client{},
		opMap:            map[uint32]*memdRequest{},
		configCh:         make(chan *cfgBucket, 0),
		configWaitCh:     make(chan *memdRequest, 1),
		requestCh:        make(chan *memdRequest, 1),
		responseCh:       make(chan *memdResponse, 1),
		timeedoutCh:      make(chan *memdRequest, 1),
		deadServerCh:     make(chan *bucketServer, 1),
		operationTimeout: 2500 * time.Millisecond,
	}

	var firstConfig *cfgBucket
	for _, memdAddr := range memdAddrs {
		thisHost := memdAddr.Host
		thisHostPort := fmt.Sprintf("%s:%d", thisHost, memdAddr.Port)

		srv := c.createServer(thisHostPort)

		fmt.Printf("Time to connect new server\n")
		atomic.AddUint64(&c.Stats.NumServerConnect, 1)
		err := srv.connect(bucket, password)

		fmt.Printf("MConnect: %v, %v\n", srv, err)
		if err != nil {
			fmt.Printf("Connection to %s failed, continueing\n", thisHostPort)
			continue
		}

		resp, err := srv.doRequest(makeCccpRequest())
		if err != nil {
			fmt.Printf("CCCP failed on %s, continueing\n", thisHostPort)
			continue
		}

		fmt.Printf("Request processed %v\n", resp)

		configStr := strings.Replace(string(resp.Value), "$HOST", thisHost, -1)
		bk := new(cfgBucket)
		err = json.Unmarshal([]byte(configStr), bk)
		fmt.Printf("DATA: %v %v\n", err, bk)

		go c.serverRun(srv)
		c.servers = append(c.servers, srv)

		firstConfig = bk

		break
	}

	if firstConfig == nil {
		fmt.Printf("Failed to get CCCP config, trying HTTP")

		panic("HTTP configurations not yet supported")

		//go httpConfigHandler()
		// Need to select here for timeouts
		firstConfig := <-c.configCh

		if firstConfig == nil {
			panic("Failed to retrieve first good configuration.")
		}
	}

	c.updateConfig(firstConfig)
	go c.globalHandler()

	return c, nil
}

func (c *BaseConnection) Get(key []byte) ([]byte, uint32, uint64, Error) {
	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdGet,
		Datatype:   0,
		Cas:        0,
		Extras:     nil,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return nil, 0, 0, err
	}

	flags := binary.BigEndian.Uint32(resp.Extras[0:])
	return resp.Value, flags, resp.Cas, nil
}

func (c *BaseConnection) GetAndTouch(key []byte, expiry uint32) ([]byte, uint32, uint64, Error) {
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, expiry)

	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdGAT,
		Datatype:   0,
		Cas:        0,
		Extras:     extraBuf,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return nil, 0, 0, err
	}

	flags := binary.BigEndian.Uint32(resp.Extras[0:])
	return resp.Value, flags, resp.Cas, nil
}

func (c *BaseConnection) GetAndLock(key []byte, lockTime uint32) ([]byte, uint32, uint64, Error) {
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, lockTime)

	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdGetLocked,
		Datatype:   0,
		Cas:        0,
		Extras:     extraBuf,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return nil, 0, 0, err
	}

	flags := binary.BigEndian.Uint32(resp.Extras[0:])
	return resp.Value, flags, resp.Cas, nil
}

func (c *BaseConnection) Unlock(key []byte, cas uint64) (uint64, Error) {
	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdUnlockKey,
		Datatype:   0,
		Cas:        0,
		Extras:     nil,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return 0, err
	}

	return resp.Cas, nil
}

func (c *BaseConnection) GetReplica(key []byte, replicaIdx int) ([]byte, uint32, uint64, Error) {
	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdGetReplica,
		Datatype:   0,
		Cas:        0,
		Extras:     nil,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: replicaIdx,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return nil, 0, 0, err
	}

	flags := binary.BigEndian.Uint32(resp.Extras[0:])
	return resp.Value, flags, resp.Cas, nil
}

func (c *BaseConnection) Touch(key []byte, expiry uint32) (uint64, Error) {
	// This seems odd, but this is how it's done in Node.js
	_, _, cas, err := c.GetAndTouch(key, expiry)
	return cas, err
}

func (c *BaseConnection) Remove(key []byte, cas uint64) (uint64, Error) {
	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdDelete,
		Datatype:   0,
		Cas:        cas,
		Extras:     nil,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return 0, err
	}

	return resp.Cas, nil
}

func (c *BaseConnection) store(opcode commandCode, key, value []byte, flags uint32, cas uint64, expiry uint32) (uint64, Error) {
	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf, flags)
	binary.BigEndian.PutUint32(extraBuf, expiry)
	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     opcode,
		Datatype:   0,
		Cas:        cas,
		Extras:     extraBuf,
		Key:        key,
		Value:      value,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return 0, err
	}

	return resp.Cas, nil
}

func (c *BaseConnection) Add(key, value []byte, flags uint32, expiry uint32) (uint64, Error) {
	return c.store(cmdAdd, key, value, flags, 0, expiry)
}

func (c *BaseConnection) Set(key, value []byte, flags uint32, expiry uint32) (uint64, Error) {
	return c.store(cmdSet, key, value, flags, 0, expiry)
}

func (c *BaseConnection) Replace(key, value []byte, flags uint32, cas uint64, expiry uint32) (uint64, Error) {
	return c.store(cmdReplace, key, value, flags, cas, expiry)
}

func (c *BaseConnection) adjoin(opcode commandCode, key, value []byte) (uint64, Error) {
	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     opcode,
		Datatype:   0,
		Cas:        0,
		Extras:     nil,
		Key:        key,
		Value:      value,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return 0, err
	}

	return resp.Cas, nil
}

func (c *BaseConnection) Append(key, value []byte) (uint64, Error) {
	return c.adjoin(cmdAppend, key, value)
}

func (c *BaseConnection) Prepend(key, value []byte) (uint64, Error) {
	return c.adjoin(cmdPrepend, key, value)
}

func (c *BaseConnection) counter(opcode commandCode, key []byte, delta, initial uint64, expiry uint32) (uint64, uint64, Error) {
	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], delta)
	binary.BigEndian.PutUint64(extraBuf[8:], initial)
	binary.BigEndian.PutUint32(extraBuf[16:], expiry)

	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     opcode,
		Datatype:   0,
		Cas:        0,
		Extras:     extraBuf,
		Key:        key,
		Value:      nil,
		RespChan:   make(chan *memdResponse, 1),
		VBHash:     cbCrc(key),
		ReplicaIdx: 0,
	}

	resp, err := c.dispatchSimpleReq(req)
	if err != nil {
		return 0, 0, err
	}

	intVal, perr := strconv.ParseUint(string(resp.Value), 10, 64)
	if perr != nil {
		return 0, 0, clientError{"Failed to parse returned value"}
	}

	return intVal, resp.Cas, nil
}

func (c *BaseConnection) Increment(key []byte, delta, initial uint64, expiry uint32) (uint64, uint64, Error) {
	return c.counter(cmdIncrement, key, delta, initial, expiry)
}

func (c *BaseConnection) Decrement(key []byte, delta, initial uint64, expiry uint32) (uint64, uint64, Error) {
	return c.counter(cmdDecrement, key, delta, initial, expiry)
}

func (c *BaseConnection) GetCapiEp() string {
	usCapiEps := atomic.LoadPointer(&c.capiEps)
	capiEps := *(*[]string)(usCapiEps)
	return capiEps[rand.Intn(len(capiEps))]
}

func (c *BaseConnection) GetMgmtEp() string {
	usMgmtEps := atomic.LoadPointer(&c.mgmtEps)
	mgmtEps := *(*[]string)(usMgmtEps)
	return mgmtEps[rand.Intn(len(mgmtEps))]
}

func (c *BaseConnection) SetOperationTimeout(val time.Duration) {
	c.operationTimeout = val
}
func (c *BaseConnection) GetOperationTimeout() time.Duration {
	return c.operationTimeout
}
