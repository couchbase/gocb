package gocb

import (
	"github.com/couchbase/gocb/gocbcore"
)

type bucketInternal struct {
	b *Bucket
}

// Retrieves a document from the bucket
func (bi *bucketInternal) GetRandom(valuePtr interface{}) (string, Cas, error) {
	return bi.b.getRandom(valuePtr)
}

// Inserts or replaces (with meta) a document in a bucket.
func (bi *bucketInternal) UpsertMeta(key string, value, extra []byte, flags, expiry uint32, cas, revseqno uint64) (Cas, error) {
	outcas, _, err := bi.b.upsertMeta(key, value, extra, flags, expiry, cas, revseqno)
	return outcas, err
}

// Removes a document (with meta) from the bucket.
func (bi *bucketInternal) RemoveMeta(key string, extra []byte, flags, expiry uint32, cas, revseqno uint64) (Cas, error) {
	outcas, _, err := bi.b.removeMeta(key, extra, flags, expiry, cas, revseqno)
	return outcas, err
}

func (b *Bucket) getRandom(valuePtr interface{}) (keyOut string, casOut Cas, errOut error) {
	signal := make(chan bool, 1)
	op, err := b.client.GetRandom(func(keyBytes, bytes []byte, flags uint32, cas gocbcore.Cas, err error) {
		errOut = err
		if errOut == nil {
			errOut = b.transcoder.Decode(bytes, flags, valuePtr)
			if errOut == nil {
				casOut = Cas(cas)
				keyOut = string(keyBytes)
			}
		}
		signal <- true
	})
	if err != nil {
		return "", 0, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.opTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			<-signal
			return
		}
		return "", 0, ErrTimeout
	}
}

func (b *Bucket) upsertMeta(key string, value, extra []byte, flags uint32, expiry uint32, cas, revseqno uint64) (Cas, MutationToken, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.SetMeta([]byte(key), value, extra, flags, expiry, cas, revseqno, gocbcore.StoreCallback(cb))
		return op, err
	})
}

func (b *Bucket) removeMeta(key string, extra []byte, flags uint32, expiry uint32, cas, revseqno uint64) (Cas, MutationToken, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.DeleteMeta([]byte(key), extra, flags, expiry, cas, revseqno, gocbcore.RemoveCallback(cb))
		return op, err
	})
}
