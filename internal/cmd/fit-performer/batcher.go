package main

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
)

type BatchSender interface {
	Send(*run.Result) error
}

type Batcher struct {
	writeQLock sync.Mutex
	writeQ     []*run.Result
	sender     BatchSender
	logger     *logrus.Logger

	stopCh    chan struct{}
	stoppedCh chan struct{}
	onError   chan<- error

	batchSize int32
}

func NewBatcher(sender BatchSender, logger *logrus.Logger, onError chan<- error, batchSize int32) *Batcher {
	return &Batcher{
		sender:    sender,
		logger:    logger,
		batchSize: batchSize,

		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
		onError:   onError,
	}
}

func (b *Batcher) Send(result *run.Result) {
	b.writeQLock.Lock()
	b.writeQ = append(b.writeQ, result)
	b.writeQLock.Unlock()
}

func (b *Batcher) Stop() <-chan struct{} {
	close(b.stopCh)
	return b.stoppedCh
}

func (b *Batcher) Run() {
	go func() {
		if err := b.batchLoop(); err != nil {
			b.logger.Logf(logrus.ErrorLevel, "Error sending data to driver: %v", err)
			b.onError <- err
			return
		}

		b.logger.Logf(logrus.InfoLevel, "Batch loop ended, flushing %d items", len(b.writeQ))

		// Flush any remaining items in the write queue
		if err := b.flush(); err != nil {
			b.logger.Logf(logrus.ErrorLevel, "Error sending data to driver during final flush: %v", err)
			b.onError <- err
			return
		}

		close(b.stoppedCh)
	}()
}

func (b *Batcher) batchLoop() error {
	for {
		// All items are flushed every 10 milliseconds. They might be separated into multiple batches if they
		// exceed the batch size
		err := b.flush()
		if err != nil {
			return err
		}

		select {
		case <-b.stopCh:
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (b *Batcher) flush() error {
	if b.batchSize == 0 {
		return b.flushIndividual()
	}

	return b.flushBatch()
}

func (b *Batcher) batchItems(items []*run.Result) [][]*run.Result {
	batchCount := int32(len(items)) / b.batchSize
	if (int32(len(items)) - batchCount*b.batchSize) > 0 {
		batchCount++
	}
	batches := make([][]*run.Result, 0, batchCount)
	for len(items) > 0 {
		size := b.batchSize
		if b.batchSize > int32(len(items)) {
			size = int32(len(items))
		}
		batch := make([]*run.Result, size)
		copy(batch, items[:size])
		batches = append(batches, batch)
		items = items[size:]
	}
	return batches
}

func (b *Batcher) itemsToFlush() []*run.Result {
	b.writeQLock.Lock()
	size := int32(len(b.writeQ))
	if size == 0 {
		b.writeQLock.Unlock()
		return nil
	}

	batch := make([]*run.Result, size)
	copy(batch, b.writeQ)
	b.writeQ = nil // Clear the queue
	b.writeQLock.Unlock()

	return batch
}

func (b *Batcher) flushIndividual() error {
	items := b.itemsToFlush()
	if len(items) == 0 {
		return nil
	}

	b.logger.Debugf("Flushing %d items", len(items))

	for _, item := range items {
		err := b.sender.Send(item)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Batcher) flushBatch() error {
	items := b.itemsToFlush()
	if len(items) == 0 {
		return nil
	}
	batches := b.batchItems(items)

	b.logger.Debugf("Flushing %d items in %d batches", len(items), len(batches))

	for _, batch := range batches {
		batchResult := &run.Result{
			Result: &run.Result_Batched{
				Batched: &run.BatchedResult{
					Result: batch,
				},
			},
		}
		err := b.sender.Send(batchResult)
		if err != nil {
			return err
		}
	}

	return nil
}
