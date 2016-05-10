package gocbcore

import (
	"testing"
	"time"
)

func TestQueueOverflow(t *testing.T) {
	queue := createMemdQueue()
	queueMax := cap(queue.reqsCh)

	for i := 0; i < queueMax; i++ {
		queue.QueueRequest(&memdQRequest{
			Callback: func(resp *memdResponse, _ *memdRequest, err error) {
				t.Fatalf("Should not have been tripped any callbacks")
			},
		})
	}

	queue.QueueRequest(&memdQRequest{
		Callback: func(resp *memdResponse, _ *memdRequest, err error) {
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
		Callback: func(resp *memdResponse, _ *memdRequest, err error) {
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
			Callback: func(resp *memdResponse, _ *memdRequest, err error) {
				t.Fatalf("Should not have been tripped any callbacks")
			},
		})
	}

	drainCount := 0
	queue.Drain(func(*memdQRequest) {
		drainCount++
	}, nil)
	if drainCount != queueMax {
		t.Fatalf("Drain did not return all of the queued requests")
	}

	if queue.QueueRequest(&memdQRequest{}) {
		t.Fatalf("Queueing operations after calling drain should fail")
	}
}

func TestQueueDrainTimed(t *testing.T) {
	queue := createMemdQueue()
	queueMax := cap(queue.reqsCh)

	for i := 0; i < queueMax; i++ {
		queue.QueueRequest(&memdQRequest{
			Callback: func(resp *memdResponse, _ *memdRequest, err error) {
				t.Fatalf("Should not have been tripped any callbacks")
			},
		})
	}

	signal := make(chan bool)
	go func() {
		time.Sleep(1 * time.Millisecond)
		signal <- true
	}()

	drainCount := 0
	queue.Drain(func(req *memdQRequest) {
		drainCount++
		if req.QueueOwner() != nil {
			t.Fatalf("Drained requests should not have an owner")
		}
	}, signal)
	if drainCount != queueMax {
		t.Fatalf("Drain did not return all of the queued requests")
	}

	if queue.QueueRequest(&memdQRequest{}) {
		t.Fatalf("Queueing operations after calling drain should fail")
	}
}

func TestQueueCancel(t *testing.T) {
	queue := createMemdQueue()

	op1 := &memdQRequest{
		Callback: func(resp *memdResponse, _ *memdRequest, err error) {
			t.Fatalf("Should not have been tripped any callbacks")
		},
	}
	op2 := &memdQRequest{
		Callback: func(resp *memdResponse, _ *memdRequest, err error) {
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
