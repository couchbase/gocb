package gocb

import (
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

func (c *Collection) observeOnceSeqNo(
	tracectx requestSpanContext,
	docID string,
	mt gocbcore.MutationToken,
	replicaIdx int,
	cancelCh chan struct{},
) (didReplicate, didPersist bool, errOut error) {
	opm := c.newKvOpManager("observeOnceSeqNo", tracectx)
	defer opm.Finish()

	opm.SetDocumentID(docID)
	opm.SetCancelCh(cancelCh)

	agent, err := c.getKvProvider()
	if err != nil {
		return false, false, err
	}
	err = opm.Wait(agent.ObserveVbEx(gocbcore.ObserveVbOptions{
		VbID:         mt.VbID,
		VbUUID:       mt.VbUUID,
		ReplicaIdx:   replicaIdx,
		TraceContext: opm.TraceSpan(),
	}, func(res *gocbcore.ObserveVbResult, err error) {
		if err != nil || res == nil {
			errOut = opm.EnhanceErr(err)
			return
		}

		didReplicate = res.CurrentSeqNo >= mt.SeqNo
		didPersist = res.PersistSeqNo >= mt.SeqNo

		opm.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}
	return
}

func (c *Collection) observeOne(
	tracectx requestSpanContext,
	docID string,
	mt gocbcore.MutationToken,
	replicaIdx int,
	replicaCh, persistCh chan struct{},
	cancelCh chan struct{},
) {
	sentReplicated := false
	sentPersisted := false

ObserveLoop:
	for {
		select {
		case <-cancelCh:
			break ObserveLoop
		default:
			// not cancelled yet
		}

		didReplicate, didPersist, err := c.observeOnceSeqNo(tracectx, docID, mt, replicaIdx, cancelCh)
		if err != nil {
			logDebugf("ObserveOnce failed unexpected: %s", err)
			return
		}

		if didReplicate && !sentReplicated {
			replicaCh <- struct{}{}
			sentReplicated = true
		}

		if didPersist && !sentPersisted {
			persistCh <- struct{}{}
			sentPersisted = true
		}

		// If we've got persisted and replicated, we can just stop
		if sentPersisted && sentReplicated {
			break ObserveLoop
		}

		waitTmr := gocbcore.AcquireTimer(c.sb.DuraPollTimeout)
		select {
		case <-waitTmr.C:
			gocbcore.ReleaseTimer(waitTmr, true)
		case <-cancelCh:
			gocbcore.ReleaseTimer(waitTmr, false)
		}
	}
}

func (c *Collection) waitForDurability(
	tracectx requestSpanContext,
	docID string,
	mt gocbcore.MutationToken,
	replicateTo uint,
	persistTo uint,
	deadline time.Time,
	cancelCh chan struct{},
) error {
	opm := c.newKvOpManager("waitForDurability", tracectx)
	defer opm.Finish()

	opm.SetDocumentID(docID)

	agent, err := c.getKvProvider()
	if err != nil {
		return err
	}

	numServers := agent.NumReplicas() + 1
	if replicateTo > uint(numServers-1) || persistTo > uint(numServers) {
		return opm.EnhanceErr(ErrDurabilityImpossible)
	}

	subOpCancelCh := make(chan struct{}, 1)
	replicaCh := make(chan struct{}, numServers)
	persistCh := make(chan struct{}, numServers)

	for replicaIdx := 0; replicaIdx < numServers; replicaIdx++ {
		go c.observeOne(opm.TraceSpan(), docID, mt, replicaIdx, replicaCh, persistCh, subOpCancelCh)
	}

	numReplicated := uint(0)
	numPersisted := uint(0)

	for {
		select {
		case <-replicaCh:
			numReplicated++
		case <-persistCh:
			numPersisted++
		case <-time.After(deadline.Sub(time.Now())):
			// deadline exceeded
			close(subOpCancelCh)
			return opm.EnhanceErr(ErrAmbiguousTimeout)
		case <-cancelCh:
			// parent asked for cancellation
			close(subOpCancelCh)
			return opm.EnhanceErr(ErrRequestCanceled)
		}

		if numReplicated >= replicateTo && numPersisted >= persistTo {
			close(subOpCancelCh)
			return nil
		}
	}
}
