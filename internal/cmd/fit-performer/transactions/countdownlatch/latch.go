package countdownlatch

import "sync/atomic"

type CountdownLatch struct {
	count int32
	done  chan struct{}
}

func New(startCount int32) *CountdownLatch {
	return &CountdownLatch{
		count: startCount,
		done:  make(chan struct{}),
	}
}

func (cl *CountdownLatch) CountDown() {
	count := atomic.AddInt32(&cl.count, -1)
	if count == 0 {
		close(cl.done)
	}
}

func (cl *CountdownLatch) Await() {
	<-cl.done
}

func (cl *CountdownLatch) Count() int32 {
	return atomic.LoadInt32(&cl.count)
}
