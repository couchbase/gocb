package gocbcore

import (
	"sync"
	"testing"
	"time"
)

func TestQueueOverflow(t *testing.T) {
	queue := createMemdQueue()
	queueMax := cap(queue.reqsCh)

	for i := 0; i < queueMax; i++ {
		queue.QueueRequest(&memdQRequest{
			Callback: func(resp *memdResponse, err error) {
				t.Fatalf("Should not have been tripped any callbacks")
			},
		})
	}

	queue.QueueRequest(&memdQRequest{
		Callback: func(resp *memdResponse, err error) {
			if resp != nil || err == nil {
				t.Fatalf("Should have overflowed")
			}
		},
	})
}

func TestQueueDouble(t *testing.T) {
	queue1 := createMemdQueue()
	queue2 := createMemdQueue()

	op := &memdQRequest{
		Callback: func(resp *memdResponse, err error) {
			t.Fatalf("Should not have been tripped any callbacks")
		},
	}

	queue1.QueueRequest(op)

	defer func() {
		if recover() == nil {
			t.Fatalf("Panic should have occured")
		}
	}()
	queue2.QueueRequest(op)
}

func TestQueueDrain(t *testing.T) {
	queue := createMemdQueue()
	queueMax := cap(queue.reqsCh)

	for i := 0; i < queueMax; i++ {
		queue.QueueRequest(&memdQRequest{
			Callback: func(resp *memdResponse, err error) {
				t.Fatalf("Should not have been tripped any callbacks")
			},
		})
	}

	done := make(chan bool)
	go func() {
		var wg sync.WaitGroup
		wg.Add(queueMax)
		queue.Drain(func(*memdQRequest) {
			wg.Done()
		}, nil)
		wg.Wait()
		done <- true
	}()
	select {
	case <-done:
		if queue.QueueRequest(&memdQRequest{}) {
			t.Fatalf("Queueing operations after calling drain should fail")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Failed to drain queued requests in 3 seconds")
	}
}

func TestQueueDrainTimed(t *testing.T) {
	queue := createMemdQueue()
	queueMax := cap(queue.reqsCh)

	for i := 0; i < queueMax; i++ {
		queue.QueueRequest(&memdQRequest{
			Callback: func(resp *memdResponse, err error) {
				t.Fatalf("Should not have been tripped any callbacks")
			},
		})
	}

	done := make(chan bool)
	go func() {
		var wg sync.WaitGroup
		wg.Add(queueMax)

		signal := make(chan bool)
		go func() {
			time.Sleep(1 * time.Millisecond)
			signal <- true
		}()

		queue.Drain(func(req *memdQRequest) {
			wg.Done()
			if req.QueueOwner() != nil {
				t.Fatalf("Drained requests should not have an owner")
			}
		}, signal)
		wg.Wait()
		done <- true
	}()
	select {
	case <-done:
		if queue.QueueRequest(&memdQRequest{}) {
			t.Fatalf("Queueing operations after calling drain should fail")
		}
	case <-time.After(4 * time.Second):
		t.Fatalf("Failed to drain queued requests in 4 seconds")
	}

	if queue.QueueRequest(&memdQRequest{}) {
		t.Fatalf("Queueing operations after calling drain should fail")
	}
}

func TestQueueCancel(t *testing.T) {
	queue := createMemdQueue()

	op1 := &memdQRequest{
		Callback: func(resp *memdResponse, err error) {
			t.Fatalf("Should not have been tripped any callbacks")
		},
	}
	op2 := &memdQRequest{
		Callback: func(resp *memdResponse, err error) {
			t.Fatalf("Should not have been tripped any callbacks")
		},
	}
	queue.QueueRequest(op1)
	queue.QueueRequest(op2)

	if op1.Cancel() != true {
		t.Fatalf("Should have cancelled successfully")
	}
	if op1.Cancel() != false {
		t.Fatalf("Should not have been able to cancel twice")
	}

	queue.Drain(func(req *memdQRequest) {
		if req != op2 {
			t.Fatalf("Only op2 should have been in the queue")
		}
	}, nil)

	if op2.Cancel() != false {
		t.Fatalf("Op 2 should have been cancelled by Drain")
	}
}
