package gocbcore

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

func TestBasicOps(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	// Set
	agent.Set([]byte("test"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		if err != nil {
			t.Fatalf("Set operation failed")
		}
		if cas == Cas(0) {
			t.Fatalf("Invalid cas received")
		}
		signal <- true
	})
	<-signal

	// Get
	agent.Get([]byte("test"), func(value []byte, flags uint32, cas Cas, err error) {
		if err != nil {
			t.Fatalf("Get operation failed")
		}
		if cas == Cas(0) {
			t.Fatalf("Invalid cas received")
		}

		signal <- true
	})
	<-signal

	// GetReplica Specific
	agent.GetReplica([]byte("test"), 1, func(value []byte, flags uint32, cas Cas, err error) {
		if err != nil {
			t.Fatalf("Get operation failed")
		}
		if cas == Cas(0) {
			t.Fatalf("Invalid cas received")
		}

		signal <- true
	})
	<-signal

	// GetReplica Any
	agent.GetReplica([]byte("test"), 0, func(value []byte, flags uint32, cas Cas, err error) {
		if err != nil {
			t.Fatalf("Get operation failed")
		}
		if cas == Cas(0) {
			t.Fatalf("Invalid cas received")
		}

		signal <- true
	})
	<-signal

	// Replace
	agent.Set([]byte("testx"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		agent.Replace([]byte("testx"), []byte("[]"), 0, cas, 0, func(cas Cas, mt MutationToken, err error) {
			if err != nil {
				t.Fatalf("Replace operation failed")
			}
			if cas == Cas(0) {
				t.Fatalf("Invalid cas received")
			}
			signal <- true
		})
	})
	<-signal

	// Set / Remove
	agent.Set([]byte("testy"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		agent.Remove([]byte("testy"), 0, func(cas Cas, mt MutationToken, err error) {
			if err != nil {
				t.Fatalf("Remove operation failed")
			}
			signal <- true
		})
	})
	<-signal

	// Insert
	agent.Remove([]byte("testz"), 0, func(cas Cas, mt MutationToken, err error) {
		agent.Add([]byte("testz"), []byte("[]"), 0, 0, func(cas Cas, mt MutationToken, err error) {
			if err != nil {
				t.Fatalf("Add operation failed")
			}
			if cas == Cas(0) {
				t.Fatalf("Invalid cas received")
			}
			signal <- true
		})
	})
	<-signal

	// Counters
	agent.Remove([]byte("testCounters"), 0, func(cas Cas, mt MutationToken, err error) {

		agent.Increment([]byte("testCounters"), 5, 11, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
			if err != nil {
				t.Fatalf("Increment operation failed")
			}
			if cas == Cas(0) {
				t.Fatalf("Invalid cas received")
			}
			if val != 11 {
				t.Fatalf("Increment did not operate properly")
			}

			agent.Increment([]byte("testCounters"), 5, 22, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
				if err != nil {
					t.Fatalf("Increment operation failed")
				}
				if cas == Cas(0) {
					t.Fatalf("Invalid cas received")
				}
				if val != 16 {
					t.Fatalf("Increment did not operate properly")
				}

				agent.Decrement([]byte("testCounters"), 3, 65, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
					if err != nil {
						t.Fatalf("Increment operation failed")
					}
					if cas == Cas(0) {
						t.Fatalf("Invalid cas received")
					}
					if val != 13 {
						t.Fatalf("Increment did not operate properly")
					}

					signal <- true
				})
			})
		})
	})
	<-signal

	// Adjoins
	agent.Set([]byte("testAdjoins"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {

		agent.Append([]byte("testAdjoins"), []byte(" Frank!"), func(cas Cas, mt MutationToken, err error) {
			if err != nil {
				t.Fatalf("Append operation failed")
			}
			if cas == Cas(0) {
				t.Fatalf("Invalid cas received")
			}

			agent.Prepend([]byte("testAdjoins"), []byte("Hello "), func(cas Cas, mt MutationToken, err error) {
				if err != nil {
					t.Fatalf("Prepend operation failed")
				}
				if cas == Cas(0) {
					t.Fatalf("Invalid cas received")
				}

				agent.Get([]byte("testAdjoins"), func(value []byte, flags uint32, cas Cas, err error) {
					if err != nil {
						t.Fatalf("Get operation failed")
					}
					if cas == Cas(0) {
						t.Fatalf("Invalid cas received")
					}

					if string(value) != "Hello there Frank!" {
						t.Fatalf("Adjoin operations did not behave")
					}

					signal <- true
				})
			})
		})
	})
	<-signal
}

func isKeyNotFoundError(err error) bool {
	te, ok := err.(interface { KeyNotFound() bool })
	return ok && te.KeyNotFound()
}

func TestExpiry(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	agent.Set([]byte("testExpiry"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		if err != nil {
			t.Fatalf("Set operation failed")
		}

		signal <- true
	})
	<-signal

	time.Sleep(1500 * time.Millisecond)

	agent.Get([]byte("testExpiry"), func(value []byte, flags uint32, cas Cas, err error) {
		if !isKeyNotFoundError(err) {
			t.Fatalf("Get should have returned key not found")
		}

		signal <- true
	})
	<-signal
}

func TestTouch(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	agent.Set([]byte("testTouch"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		if err != nil {
			t.Fatalf("Set operation failed")
		}

		agent.Touch([]byte("testTouch"), 0, 3, func(cas Cas, mt MutationToken, err error) {
			if err != nil {
				t.Fatalf("Touch operation failed")
			}

			signal <- true
		});
	})
	<-signal

	time.Sleep(1500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		if err != nil {
			t.Fatalf("Get should have been successful")
		}

		signal <- true
	})
	<-signal

	time.Sleep(2500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		if !isKeyNotFoundError(err) {
			t.Fatalf("Get should have returned key not found")
		}

		signal <- true
	})
	<-signal
}

func TestGetAndTouch(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	agent.Set([]byte("testTouch"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		if err != nil {
			t.Fatalf("Set operation failed")
		}

		agent.GetAndTouch([]byte("testTouch"), 3, func(value []byte, flags uint32, cas Cas, err error) {
			if err != nil {
				t.Fatalf("Touch operation failed")
			}

			signal <- true
		});
	})
	<-signal

	time.Sleep(1500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		if err != nil {
			t.Fatalf("Get should have been successful")
		}

		signal <- true
	})
	<-signal

	time.Sleep(2500 * time.Millisecond)

	agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		if !isKeyNotFoundError(err) {
			t.Fatalf("Get should have returned key not found")
		}

		signal <- true
	})
	<-signal
}

func TestObserve(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		agent.Observe([]byte("testObserve"), 1, func(ks KeyState, cas Cas, err error) {
			if err != nil {
				t.Fatalf("Get operation failed")
			}

			signal <- true
		})
	})
	<-signal
}

/* Dependant on SeqNo support...
func TestObserveSeqNo(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, origMt MutationToken, err error) {

		agent.ObserveSeqNo([]byte("testObserve"), origMt.VbUuid, 1, func(origCurSeqNo, origPersistSeqNo SeqNo, err error) {
			if err != nil {
				t.Fatalf("ObserveSeqNo operation failed")
			}

			agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {

				agent.ObserveSeqNo([]byte("testObserve"), mt.VbUuid, 1, func(curSeqNo, persistSeqNo SeqNo, err error) {
					if err != nil {
						t.Fatalf("ObserveSeqNo operation failed")
					}
					if curSeqNo < origCurSeqNo {
						t.Fatalf("SeqNo does not appear to be working")
					}

					signal <- true
				})
			})
		})
	})
	<-signal
}
*/

func TestRandomGet(t *testing.T) {
	agent := getAgent(t)
	signal := make(chan bool)

	agent.GetRandom(func(key, value []byte, flags uint32, cas Cas, err error) {
		if err != nil {
			t.Fatalf("Get operation failed")
		}
		if cas == Cas(0) {
			t.Fatalf("Invalid cas received")
		}
		if len(key) == 0 {
			t.Fatalf("Invalid key returned")
		}
		if len(key) == 0 {
			t.Fatalf("No value returned")
		}

		signal <- true
	})
	<-signal
}
