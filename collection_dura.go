package gocb

import (
	"context"

	"github.com/opentracing/opentracing-go"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

func (c *Collection) observeOnceSeqNo(ctx context.Context, tracectx opentracing.SpanContext, mt MutationToken,
	replicaIdx int, commCh chan uint) (pendingOp, error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	span := c.startKvOpTrace("ObserveOnce", tracectx)

	return agent.ObserveVbEx(gocbcore.ObserveVbOptions{
		VbId:         mt.token.VbId,
		VbUuid:       mt.token.VbUuid,
		ReplicaIdx:   replicaIdx,
		TraceContext: span.Context(),
	}, func(res *gocbcore.ObserveVbResult, err error) {
		if err != nil || res == nil {
			commCh <- 0
			return
		}

		didReplicate := res.CurrentSeqNo >= mt.token.SeqNo
		didPersist := res.PersistSeqNo >= mt.token.SeqNo

		var out uint
		if didReplicate {
			out |= 1
		}
		if didPersist {
			out |= 2
		}
		commCh <- out
	})
}

func (c *Collection) observeOne(ctx context.Context, tracectx opentracing.SpanContext, key []byte, mt MutationToken,
	cas Cas, forDelete bool, replicaIdx int, replicaCh, persistCh chan bool, scopeName, collectionName string) {

	sentReplicated := false
	sentPersisted := false

	failMe := func() {
		if !sentReplicated {
			replicaCh <- false
			sentReplicated = true
		}
		if !sentPersisted {
			persistCh <- false
			sentPersisted = true
		}
	}

	// Doing this will set the context deadline to whichever is shorter, what is already set or the timeout
	// value
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, c.sb.DuraTimeout)
	defer cancel()

	commCh := make(chan uint)
	for {
		op, err := c.observeOnceSeqNo(ctx, tracectx, mt, replicaIdx, commCh)
		if err != nil {
			failMe()
			return
		}

		select {
		case val := <-commCh:
			// Got Value
			if (val&1) != 0 && !sentReplicated {
				replicaCh <- true
				sentReplicated = true
			}
			if (val&2) != 0 && !sentPersisted {
				persistCh <- true
				sentPersisted = true
			}

			if sentReplicated && sentPersisted {
				return
			}

			waitTmr := gocbcore.AcquireTimer(c.sb.DuraPollTimeout)
			select {
			case <-waitTmr.C:
				gocbcore.ReleaseTimer(waitTmr, true)
				// Fall through to outside for loop
			case <-ctx.Done():
				op.Cancel()
				gocbcore.ReleaseTimer(waitTmr, false)
				failMe()
				return
			}

		case <-ctx.Done():
			// Timed out
			op.Cancel()
			failMe()
			return
		}
	}
}

type durabilitySettings struct {
	ctx            context.Context
	key            string
	cas            Cas
	mt             MutationToken
	replicaTo      uint
	persistTo      uint
	forDelete      bool
	collectionName string
	scopeName      string
	tracectx       opentracing.SpanContext
}

func (c *Collection) durability(settings durabilitySettings) error {
	if settings.ctx == nil {
		settings.ctx = context.Background()
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return err
	}

	numServers := agent.NumReplicas() + 1

	if settings.replicaTo > uint(numServers-1) || settings.persistTo > uint(numServers) {
		return durabilityError{reason: "Not enough replicas to match durability requirements."}
	}

	keyBytes := []byte(settings.key)

	replicaCh := make(chan bool, numServers)
	persistCh := make(chan bool, numServers)

	for replicaIdx := 0; replicaIdx < numServers; replicaIdx++ {
		go c.observeOne(settings.ctx, settings.tracectx, keyBytes, settings.mt, settings.cas, settings.forDelete,
			replicaIdx, replicaCh, persistCh, settings.scopeName, settings.collectionName)
	}

	results := int(0)
	replicas := uint(0)
	persists := uint(0)

	for {
		select {
		case rV := <-replicaCh:
			if rV {
				replicas++
			}
			results++
		case pV := <-persistCh:
			if pV {
				persists++
			}
			results++
		}

		if replicas >= settings.replicaTo && persists >= settings.persistTo {
			return nil
		} else if results == (numServers * 2) {
			return durabilityError{reason: "Failed to meet durability requirements in time."}
		}
	}
}
