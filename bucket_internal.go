package gocb

import (
	"github.com/couchbase/gocb/gocbcore"
)

type bucketInternal struct {
	b *Bucket;
}

// Retrieves a document from the bucket
func (bi *bucketInternal) GetRandom(valuePtr interface{}) (string, Cas, error) {
	return bi.b.getRandom(valuePtr)
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
		op.Cancel()
		return "", 0, timeoutError{}
	}
}
