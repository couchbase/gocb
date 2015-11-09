package gocbcore

import (
	"strings"
	"sync"
	"time"
)

type memdInitFunc func(*memdPipeline, time.Time) error

type CloseHandler func(*memdPipeline)
type BadRouteHandler func(*memdPipeline, *memdQRequest, *memdResponse)

type Callback func(*memdResponse, error)

type memdPipeline struct {
	lock sync.RWMutex

	queue *memdQueue

	address  string
	conn     memdReadWriteCloser
	isClosed bool
	ioDoneCh chan bool

	opList  memdOpMap

	handleBadRoute BadRouteHandler
	handleDeath    CloseHandler
}

func CreateMemdPipeline(address string) *memdPipeline {
	return &memdPipeline{
		address:  address,
		queue:    createMemdQueue(),
		ioDoneCh: make(chan bool, 1),
	}
}

func (s *memdPipeline) Address() string {
	return s.address
}

func (s *memdPipeline) Hostname() string {
	return strings.Split(s.address, ":")[0]
}

func (s *memdPipeline) IsClosed() bool {
	return s.isClosed
}

func (s *memdPipeline) SetHandlers(badRouteFn BadRouteHandler, deathFn CloseHandler) {
	s.lock.Lock()

	if s.isClosed {
		// We died between authentication and here, immediately notify the deathFn
		s.lock.Unlock()
		deathFn(s)
		return
	}

	s.handleBadRoute = badRouteFn
	s.handleDeath = deathFn
	s.lock.Unlock()
}

func (pipeline *memdPipeline) ExecuteRequest(req *memdQRequest, deadline time.Time) (respOut *memdResponse, errOut error) {
	if req.Callback != nil {
		panic("Tried to synchronously dispatch an operation with an async handler.")
	}

	signal := make(chan bool)

	req.Callback = func(resp *memdResponse, err error) {
		respOut = resp
		errOut = err
		signal <- true
	}

	if !pipeline.queue.QueueRequest(req) {
		return nil, &generalError{"Failed to dispatch operation."}
	}

	timeoutTmr := AcquireTimer(deadline.Sub(time.Now()))
	select {
	case <-signal:
		ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		ReleaseTimer(timeoutTmr, true)
		req.Cancel()
		return nil, &timeoutError{}
	}
}

func (pipeline *memdPipeline) dispatchRequest(req *memdQRequest) error {
	// We do a cursory check of the server to avoid dispatching operations on the network
	//   that have already knowingly been cancelled.  This doesn't guarentee a cancelled
	//   operation from being sent, but it does reduce network IO when possible.
	if req.QueueOwner() != pipeline.queue {
		// Even though we failed to dispatch, this is not actually an error,
		//   we just consume the operation since its already been handled elsewhere
		return nil
	}

	pipeline.opList.Add(req)

	err := pipeline.conn.WritePacket(&req.memdRequest)
	if err != nil {
		logDebugf("Got write error")
		pipeline.opList.Remove(req)
		return err
	}

	return nil
}

func (s *memdPipeline) resolveRequest(resp *memdResponse) {
	opIndex := resp.Opaque

	// Find the request that goes with this response
	req := s.opList.FindAndMaybeRemove(opIndex)

	if req == nil {
		// There is no known request that goes with this response.  Ignore it.
		logDebugf("Received response with no corresponding request.")
		return
	}

	if !req.Persistent || (resp.Magic == ResMagic && resp.Status != StatusSuccess) {
		if !s.queue.UnqueueRequest(req) {
			// While we found a valid request, the request does not appear to be queued
			//   with this server anymore, this probably means that it has been cancelled.
			logDebugf("Received response for cancelled request.")
			return
		}
	}

	if resp.Magic == ResMagic && resp.Status == StatusNotMyVBucket {
		// If possible, lets backchannel our NMV back to the Agent of this memdQueueConn
		//   instance.  This is primarily meant to enhance performance, and allow the
		//   agent to be instantly notified upon a new configuration arriving.  If the
		//   backchannel isn't available, we just Callback with the NMV error.
		logDebugf("Received NMV response.")
		s.lock.RLock()
		badRouteFn := s.handleBadRoute
		s.lock.RUnlock()
		if badRouteFn != nil {
			badRouteFn(s, req, resp)
			return
		}
	}

	// Call the requests callback handler...  Ignore Status field for incoming requests.
	logDebugf("Dispatching response callback.")
	if resp.Magic == ReqMagic || resp.Status == StatusSuccess {
		req.Callback(resp, nil)
	} else {
		req.Callback(nil, &memdError{resp.Status})
	}
}

func (pipeline *memdPipeline) ioLoop() {
	killSig := make(chan bool)

	// Reading
	go func() {
		logDebugf("Reader loop starting...")
		for {
			resp := &memdResponse{}
			err := pipeline.conn.ReadPacket(resp)
			if err != nil {
				logDebugf("Server read error: %v", err)
				killSig <- true
				break
			}

			logDebugf("Got response to resolve.")
			pipeline.resolveRequest(resp)
		}
	}()

	// Writing
	logDebugf("Writer loop starting...")
	for {
		select {
		case req := <-pipeline.queue.reqsCh:
			logDebugf("Got a request to dispatch.")
			err := pipeline.dispatchRequest(req)
			if err != nil {
				// We can assume that the server is not fully drained yet, as the drainer blocks
				//   waiting for the IO goroutines to finish first.
				pipeline.queue.reqsCh <- req

				// We must wait for the receive goroutine to die as well before we can continue.
				<-killSig

				return
			}
		case <-killSig:
			return
		}
	}
}

func (pipeline *memdPipeline) Run() {
	logDebugf("Beginning pipeline runner")

	// Run the IO loop.  This will block until the connection has been closed.
	pipeline.ioLoop()

	// Now we must signal drainers that we are done!
	pipeline.ioDoneCh <- true

	// Signal the creator that we died :(
	pipeline.lock.Lock()
	pipeline.isClosed = true
	deathFn := pipeline.handleDeath
	pipeline.lock.Unlock()
	if deathFn != nil {
		deathFn(pipeline)
	} else {
		pipeline.Drain(nil)
	}
}

func (pipeline *memdPipeline) Close() {
	pipeline.Drain(nil)
}

func (pipeline *memdPipeline) Drain(reqCb drainedReqCallback) {
	// If the user does no pass a drain callback, we handle the requests
	//   by immediately failing them with a network error.
	if reqCb == nil {
		reqCb = func(req *memdQRequest) {
			req.Callback(nil, networkError{})
		}
	}

	// Make sure the connection is closed, which will signal the ioLoop
	//   to stop running and signal on ioDoneCh
	pipeline.conn.Close()

	// Drain the request queue, this will block until the io thread signals
	//   on ioDoneCh, and the queues have been completely emptied
	pipeline.queue.Drain(reqCb, pipeline.ioDoneCh)

	// As a last step, immediately notify all the requests that were
	//   on-the-wire that a network error has occurred.
	pipeline.opList.Drain(func(r *memdQRequest) {
		if pipeline.queue.UnqueueRequest(r) {
			r.Callback(nil, networkError{})
		}
	})
}
