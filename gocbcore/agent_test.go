package gocbcore

/*
import (
	"testing"
	"time"
)

func saslAuthFn(bucket, password string) func(AuthClient, time.Time) error {
	return func(srv AuthClient, deadline time.Time) error {
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
}

type Signaler struct {
	t *testing.T
	wrapped bool
	signal chan int
}

func (s *Signaler) Continue() {
	s.signal <- 0
}

func (s *Signaler) Wrap(fn func()) {
	s.wrapped = true
	defer func() {
		s.wrapped = false
		if r := recover(); r != nil {
			// Rethrow actual panics
			if r != s {
				panic(r)
			}
		}
	}()
	fn()
	s.signal <- 0
}

func (s *Signaler) Fatalf(fmt string, args ...interface{}) {
	s.t.Logf(fmt, args...)
	s.signal <- 1
	panic(s)
}

func (s *Signaler) Skipf(fmt string, args ...interface{}) {
	s.t.Logf(fmt, args...)
	s.signal <- 2
	panic(s)
}

func (s *Signaler) Wait(waitSecs int) {
	if waitSecs <= 0 {
		waitSecs = 5
	}

	select {
	case v := <-s.signal:
		if v == 1 {
			s.t.FailNow()
		} else if v == 2 {
			s.t.SkipNow()
		}
	case <-time.After(time.Duration(waitSecs) * time.Second):
		s.t.Fatalf("Wait timeout expired")
	}
}

func getSignaler(t *testing.T) *Signaler {
	signaler := &Signaler{
		t: t,
		signal: make(chan int),
	}
	return signaler
}

var globalAgent *Agent
func getAgent(t *testing.T) *Agent {
	if globalAgent != nil {
		return globalAgent
	}

	agentConfig := &AgentConfig{
		MemdAddrs:            []string{"192.168.7.26:11210"},
		HttpAddrs:            []string{"192.168.7.26:8091"},
		TlsConfig:            nil,
		BucketName:           "default",
		Password:             "",
		AuthHandler:          saslAuthFn("default", ""),
		ConnectTimeout:       5 * time.Second,
		ServerConnectTimeout: 1 * time.Second,
	}

	agent, err := CreateAgent(agentConfig)
	if err != nil {
		t.Fatalf("Failed to connect to server")
	}

	globalAgent = agent
	return agent
}

func TestHttpAgent(t *testing.T) {
	agentConfig := &AgentConfig{
		MemdAddrs:            []string{},
		HttpAddrs:            []string{"192.168.7.26:8091"},
		TlsConfig:            nil,
		BucketName:           "default",
		Password:             "",
		AuthHandler:          saslAuthFn("default", ""),
		ConnectTimeout:       5 * time.Second,
		ServerConnectTimeout: 1 * time.Second,
	}

	agent, err := CreateAgent(agentConfig)
	if err != nil {
		t.Fatalf("Failed to connect to server")
	}

	agent.CloseTest()
}

func getAgentnSignaler(t *testing.T) (*Agent, *Signaler) {
	return getAgent(t), getSignaler(t)
}

func TestBasicOps(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Set
	agent.Set([]byte("test"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)

	// Get
	agent.Get([]byte("test"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)

	// GetReplica Specific
	agent.GetReplica([]byte("test"), 1, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)

	// GetReplica Any
	agent.GetReplica([]byte("test"), 0, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)
}

func TestBasicReplace(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	oldCas := Cas(0)
	agent.Set([]byte("testx"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		oldCas = cas
		s.Continue()
	})
	s.Wait(0)

	agent.Replace([]byte("testx"), []byte("[]"), 0, oldCas, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Replace operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)
}

func TestBasicRemove(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	agent.Set([]byte("testy"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	})
	s.Wait(0)

	agent.Remove([]byte("testy"), 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Remove operation failed")
			}
		})
	})
	s.Wait(0)
}

func TestBasicInsert(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	agent.Remove([]byte("testz"), 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	})
	s.Wait(0)

	agent.Add([]byte("testz"), []byte("[]"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				t.Fatalf("Add operation failed")
			}
			if cas == Cas(0) {
				t.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)
}

func TestBasicCounters(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Counters
	agent.Remove([]byte("testCounters"), 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	})
	s.Wait(0)

	agent.Increment([]byte("testCounters"), 5, 11, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if val != 11 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	})
	s.Wait(0)

	agent.Increment([]byte("testCounters"), 5, 22, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if val != 16 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	})
	s.Wait(0)

	agent.Decrement([]byte("testCounters"), 3, 65, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if val != 13 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	})
	s.Wait(0)
}

func TestBasicAdjoins(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	agent.Set([]byte("testAdjoins"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	})
	s.Wait(0)

	agent.Append([]byte("testAdjoins"), []byte(" Frank!"), func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Append operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)

	agent.Prepend([]byte("testAdjoins"), []byte("Hello "), func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Prepend operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	})
	s.Wait(0)

	agent.Get([]byte("testAdjoins"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}

			if string(value) != "Hello there Frank!" {
				s.Fatalf("Adjoin operations did not behave")
			}
		})
	})
	s.Wait(0)
}

func isKeyNotFoundError(err error) bool {
	te, ok := err.(interface { KeyNotFound() bool })
	return ok && te.KeyNotFound()
}

func TestExpiry(t *testing.T) {
	agent := getAgent(t)
	s := getSignaler(t)

	agent.Set([]byte("testExpiry"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed")
			}
		})
	})
	s.Wait(0)

	time.Sleep(1500 * time.Millisecond)

	agent.Get([]byte("testExpiry"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if !isKeyNotFoundError(err) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	})
	s.Wait(0)
}

func TestTouch(t *testing.T) {
	agent := getAgent(t)
	s := getSignaler(t)

	agent.Set([]byte("testTouch"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed")
			}
		})
	})
	s.Wait(0)

	agent.Touch([]byte("testTouch"), 0, 3, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed")
			}
		})
	});
	s.Wait(0)

	time.Sleep(1500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	})
	s.Wait(0)

	time.Sleep(2500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if !isKeyNotFoundError(err) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	})
	s.Wait(0)
}

func TestGetAndTouch(t *testing.T) {
	agent := getAgent(t)
	s := getSignaler(t)

	agent.Set([]byte("testTouch"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed")
			}
		})
	})
	s.Wait(0)

	agent.GetAndTouch([]byte("testTouch"), 3, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed")
			}
		})
	});
	s.Wait(0)

	time.Sleep(1500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	})
	s.Wait(0)

	time.Sleep(2500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if !isKeyNotFoundError(err) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	})
	s.Wait(0)
}

func TestObserve(t *testing.T) {
	agent := getAgent(t)
	s := getSignaler(t)

	agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	})
	s.Wait(0)

	agent.Observe([]byte("testObserve"), 1, func(ks KeyState, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed")
			}
		})
	})
	s.Wait(0)
}

func TestObserveSeqNo(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	origMt := MutationToken{}
	agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if (err != nil) {
				s.Fatalf("Initial set operation failed")
			}

			if mt.VbUuid == 0 && mt.SeqNo == 0 {
				s.Skipf("ObserveSeqNo not supported by server")
			}

			origMt = mt
		})
	})
	s.Wait(0)

	origCurSeqNo := SeqNo(0)
	agent.ObserveSeqNo([]byte("testObserve"), origMt.VbUuid, 1, func(curSeqNo, persistSeqNo SeqNo, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ObserveSeqNo operation failed")
			}

			origCurSeqNo = curSeqNo
		})
	})
	s.Wait(0)

	newMt := MutationToken{}
	agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Second set operation failed")
			}

			newMt = mt
		})
	})
	s.Wait(0)

	agent.ObserveSeqNo([]byte("testObserve"), newMt.VbUuid, 1, func(curSeqNo, persistSeqNo SeqNo, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ObserveSeqNo operation failed")
			}
			if curSeqNo < origCurSeqNo {
				s.Fatalf("SeqNo does not appear to be working")
			}
		})
	})
	s.Wait(0)
}

func TestRandomGet(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	agent.GetRandom(func(key, value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed")
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(key) == 0 {
				s.Fatalf("Invalid key returned")
			}
			if len(key) == 0 {
				s.Fatalf("No value returned")
			}
		})
	})
	s.Wait(0)
}

func TestGetHttpEps(t *testing.T) {
	agent := getAgent(t)

	/* Relies on a 3.0.0+ server
	n1qlEpList := agent.N1qlEps()
	if len(n1qlEpList) == 0 {
		t.Fatalf("Failed to retreive N1QL endpoint list")
	}
	/

	mgmtEpList := agent.MgmtEps()
	if len(mgmtEpList) == 0 {
		t.Fatalf("Failed to retreive N1QL endpoint list")
	}

	capiEpList := agent.CapiEps()
	if len(capiEpList) == 0 {
		t.Fatalf("Failed to retreive N1QL endpoint list")
	}
}
*/
