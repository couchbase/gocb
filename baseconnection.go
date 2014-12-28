package couchbase

import "fmt"
import "regexp"
import "strconv"
import "io"
import "errors"
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

// A single address stored within a connection string
type connSpecAddr struct {
	Host string
	Port int
}

// A parsed connection string
type connSpec struct {
	Scheme  string
	Hosts   []connSpecAddr
	Bucket  string
	Options map[string]string
}

// Parses a connection string into a structure more easily consumed by the library.
func parseConnSpec(connStr string) connSpec {
	var out connSpec
	out.Options = map[string]string{}

	partMatcher := regexp.MustCompile(`((.*):\/\/)?([^\/?]*)(\/([^\?]*))?(\?(.*))?`)
	hostMatcher := regexp.MustCompile(`([^;\,\:]+)(:([0-9]*))?(;\,)?`)
	kvMatcher := regexp.MustCompile(`([^=]*)=([^&?]*)[&?]?`)
	parts := partMatcher.FindStringSubmatch(connStr)

	if parts[2] != "" {
		out.Scheme = parts[2]
	}

	if parts[3] != "" {
		hosts := hostMatcher.FindAllStringSubmatch(parts[3], -1)
		for _, hostInfo := range hosts {
			port := 0
			if hostInfo[3] != "" {
				port, _ = strconv.Atoi(hostInfo[3])
			}

			out.Hosts = append(out.Hosts, connSpecAddr{
				Host: hostInfo[1],
				Port: port,
			})
		}
	}

	if parts[5] != "" {
		out.Bucket = parts[5]
	}

	if parts[7] != "" {
		kvs := kvMatcher.FindAllStringSubmatch(parts[7], -1)
		for _, kvInfo := range kvs {
			out.Options[kvInfo[1]] = kvInfo[2]
		}
	}

	return out
}

// Guesses a list of memcached hosts based on a connection string specification structure.
func guessMemdHosts(spec connSpec) []connSpecAddr {
	if spec.Scheme != "http" {
		panic("Should not be guessing memcached ports for non-http scheme")
	}

	var out []connSpecAddr
	for _, host := range spec.Hosts {
		memdHost := connSpecAddr{
			Host: host.Host,
			Port: 0,
		}

		if host.Port == 0 {
			memdHost.Port = 11210
		} else if host.Port == 8091 {
			memdHost.Port = 11210
		}

		if memdHost.Port != 0 {
			out = append(out, memdHost)
		}
	}
	return out
}

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
	conn       io.ReadWriteCloser
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
		Magic:    REQ_MAGIC,
		Opcode:   SASL_AUTH,
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
		Magic:    REQ_MAGIC,
		Opcode:   GET_CLUSTER_CONFIG,
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

	//authResp, err := s.doRequest(makePlainAuthRequest(bucket, password))

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
		log.Fatalf("Error reading: %v", err)
		return nil, err
	}
	fmt.Println("Received Header")
	fmt.Println(hex.Dump(hdrBuf[:24]))

	bodyLen := int(binary.BigEndian.Uint32(hdrBuf[8:]))

	bodyBuf := make([]byte, bodyLen)
	_, err = io.ReadFull(s.conn, bodyBuf)
	if err != nil {
		log.Fatalf("Error reading: %v", err)
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

func (c *BaseConnection) globalHandler() {
	fmt.Printf("Global Handler Running\n")
	for {
		fmt.Printf("Global Handler Loop\n")

		select {
		case req := <-c.requestCh:
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

		case resp := <-c.responseCh:
			fmt.Printf("Handling response %v\n", resp)

			req := c.opMap[resp.Opaque]
			fmt.Printf("  for request %v\n", req)

			if req != nil {
				delete(c.opMap, resp.Opaque)

				if resp.Status == NOT_MY_VBUCKET {
					// Maybe update config?
					atomic.AddUint64(&c.Stats.NumOpRelocated, 1)
					c.requestCh <- req
				} else {
					req.RespChan <- resp
				}
			}

			//case req := <-c.timeedoutCh:
			// Maybe check the server for too many failures, and trigger config reget
			// delete(opMap, req.Opaque)

		case deader := <-c.deadServerCh:
			// Server died, do something about it

			deader.isDead = true
			// Request new config somehow

			//case config := <-c.configCh:
			//UpdateConfig(config)

		}
	}
}

/*
func (c *BaseConnection) Observe(key string) {
	keyBuf := []byte(key)
	keyHash := cbCrc(keyBuf)

	req := &memdRequest{
		Magic:    REQ_MAGIC,
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

func (c *BaseConnection) dispatchSimpleReq(req *memdRequest) (*memdResponse, error) {
	atomic.AddUint64(&c.Stats.NumOp, 1)
	c.requestCh <- req

	select {
	case resp := <-req.RespChan:
		atomic.AddUint64(&c.Stats.NumOpResp, 1)
		return resp, nil
	case <-time.After(c.operationTimeout):
		atomic.AddUint64(&c.Stats.NumOpTimeout, 1)
		c.timeedoutCh <- req
		return nil, errors.New("Timeout")
	}
}

func (c *BaseConnection) updateConfig(bk *cfgBucket) {
	atomic.AddUint64(&c.Stats.NumConfigUpdate, 1)

	var newServers []*bucketServer
	for _, hostPort := range bk.VBSMJson.ServerList {
		var newServer *bucketServer = nil

		for _, oldServer := range c.servers {
			if oldServer.addr == hostPort {
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

	for _, oldServer := range c.servers {
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
					c.requestCh <- req
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

	c.servers = newServers
	c.vbMap = &bk.VBSMJson
	atomic.StorePointer(&c.capiEps, unsafe.Pointer(&capiEps))
	atomic.StorePointer(&c.mgmtEps, unsafe.Pointer(&mgmtEps))
}

func CreateBaseConnection(connSpecStr string, bucket, password string) (*BaseConnection, error) {
	spec := parseConnSpec(connSpecStr)

	memdAddrs := spec.Hosts
	if spec.Scheme == "http" {
		memdAddrs = guessMemdHosts(spec)
	}

	fmt.Printf("ADDRs: %v\n", memdAddrs)

	c := &BaseConnection{
		httpCli:          &http.Client{},
		opMap:            map[uint32]*memdRequest{},
		configCh:         make(chan *cfgBucket, 1),
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

func (c *BaseConnection) Get(key []byte) ([]byte, uint32, uint64, error) {
	req := &memdRequest{
		Magic:      REQ_MAGIC,
		Opcode:     GET,
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

func (c *BaseConnection) GetAndTouch(key []byte, expiry uint32) ([]byte, uint32, uint64, error) {
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, expiry)

	req := &memdRequest{
		Magic:      REQ_MAGIC,
		Opcode:     GAT,
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

func (c *BaseConnection) GetAndLock(key []byte, lockTime uint32) ([]byte, uint32, uint64, error) {
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, lockTime)

	req := &memdRequest{
		Magic:      REQ_MAGIC,
		Opcode:     GET_LOCKED,
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

func (c *BaseConnection) Unlock(key []byte, cas uint64) (uint64, error) {
	req := &memdRequest{
		Magic:      REQ_MAGIC,
		Opcode:     UNLOCK_KEY,
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

func (c *BaseConnection) GetReplica(key []byte, replicaIdx int) ([]byte, uint32, uint64, error) {
	req := &memdRequest{
		Magic:      REQ_MAGIC,
		Opcode:     GET_REPLICA,
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

func (c *BaseConnection) Touch(key []byte, expiry uint32) (uint64, error) {
	// This seems odd, but this is how it's done in Node.js
	_, _, cas, err := c.GetAndTouch(key, expiry)
	return cas, err
}

func (c *BaseConnection) Remove(key []byte, cas uint64) (uint64, error) {
	req := &memdRequest{
		Magic:      REQ_MAGIC,
		Opcode:     DELETE,
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

func (c *BaseConnection) store(opcode commandCode, key, value []byte, flags uint32, cas uint64) (uint64, error) {
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, flags)

	req := &memdRequest{
		Magic:      REQ_MAGIC,
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

func (c *BaseConnection) Add(key, value []byte, flags uint32) (uint64, error) {
	return c.store(ADD, key, value, flags, 0)
}

func (c *BaseConnection) Set(key, value []byte, flags uint32) (uint64, error) {
	return c.store(SET, key, value, flags, 0)
}

func (c *BaseConnection) Replace(key, value []byte, flags uint32, cas uint64) (uint64, error) {
	return c.store(REPLACE, key, value, flags, cas)
}

func (c *BaseConnection) adjoin(opcode commandCode, key, value []byte) (uint64, error) {
	req := &memdRequest{
		Magic:      REQ_MAGIC,
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

func (c *BaseConnection) Append(key, value []byte) (uint64, error) {
	return c.adjoin(APPEND, key, value)
}

func (c *BaseConnection) Prepend(key, value []byte) (uint64, error) {
	return c.adjoin(PREPEND, key, value)
}

func (c *BaseConnection) counter(opcode commandCode, key []byte, delta, initial uint64, expiry uint32) (uint64, uint64, error) {
	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], delta)
	binary.BigEndian.PutUint64(extraBuf[8:], initial)
	binary.BigEndian.PutUint32(extraBuf[16:], expiry)

	req := &memdRequest{
		Magic:      REQ_MAGIC,
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

	intVal, err := strconv.ParseUint(string(resp.Value), 10, 64)
	if err != nil {
		return 0, 0, errors.New("Failed to parse returned value")
	}

	return intVal, resp.Cas, nil
}

func (c *BaseConnection) Increment(key []byte, delta, initial uint64, expiry uint32) (uint64, uint64, error) {
	return c.counter(INCREMENT, key, delta, initial, expiry)
}

func (c *BaseConnection) Decrement(key []byte, delta, initial uint64, expiry uint32) (uint64, uint64, error) {
	return c.counter(DECREMENT, key, delta, initial, expiry)
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
