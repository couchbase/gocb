package gocb

import (
	"github.com/couchbaselabs/gocb/gocbcore"
	"math/rand"
	"sync"
	"time"
)

// An interface representing a single bucket within a cluster.
type Bucket struct {
	name     string
	password string
	client   *gocbcore.Agent

	transcoder      Transcoder
	opTimeout       time.Duration
	duraTimeout     time.Duration
	duraPollTimeout time.Duration
}

func createBucket(config *gocbcore.AgentConfig) (*Bucket, error) {
	cli, err := gocbcore.CreateAgent(config)
	if err != nil {
		return nil, err
	}

	return &Bucket{
		name:       config.BucketName,
		password:   config.Password,
		client:     cli,
		transcoder: &DefaultTranscoder{},

		opTimeout:       2500 * time.Millisecond,
		duraTimeout:     40000 * time.Millisecond,
		duraPollTimeout: 100 * time.Millisecond,
	}, nil
}

func (b *Bucket) OperationTimeout() time.Duration {
	return b.opTimeout
}
func (b *Bucket) SetOperationTimeout(timeout time.Duration) {
	b.opTimeout = timeout
}
func (b *Bucket) DurabilityTimeout() time.Duration {
	return b.duraTimeout
}
func (b *Bucket) SetDurabilityTimeout(timeout time.Duration) {
	b.duraTimeout = timeout
}
func (b *Bucket) DurabilityPollTimeout() time.Duration {
	return b.duraPollTimeout
}
func (b *Bucket) SetDurabilityPollTimeout(timeout time.Duration) {
	b.duraPollTimeout = timeout
}

func (b *Bucket) SetTranscoder(transcoder Transcoder) {
	b.transcoder = transcoder
}

var timerPool = sync.Pool{}

func acquireTimer(d time.Duration) *time.Timer {
	tmrMaybe := timerPool.Get()
	if tmrMaybe == nil {
		return time.NewTimer(d)
	}
	tmr := tmrMaybe.(*time.Timer)
	tmr.Reset(d)
	return tmr
}
func releaseTimer(t *time.Timer, wasRead bool) {
	stopped := t.Stop()
	if !wasRead && !stopped {
		<-t.C
	}
	timerPool.Put(t)
}

type Cas gocbcore.Cas
type pendingOp gocbcore.PendingOp

// Returns a CAPI endpoint.  Guarenteed to return something for now...
func (b *Bucket) getViewEp() (string, error) {
	capiEps := b.client.CapiEps()
	if len(capiEps) == 0 {
		return "", &clientError{"No available view nodes."}
	}
	return capiEps[rand.Intn(len(capiEps))], nil
}

func (b *Bucket) getMgmtEp() (string, error) {
	mgmtEps := b.client.MgmtEps()
	if len(mgmtEps) == 0 {
		return "", &clientError{"No available management nodes."}
	}
	return mgmtEps[rand.Intn(len(mgmtEps))], nil
}

func (b *Bucket) getN1qlEp() (string, error) {
	n1qlEps := b.client.N1qlEps()
	if len(n1qlEps) == 0 {
		return "", &clientError{"No available N1QL nodes."}
	}
	return n1qlEps[rand.Intn(len(n1qlEps))], nil
}

func (b *Bucket) IoRouter() *gocbcore.Agent {
	return b.client
}

func (b *Bucket) Manager(username, password string) *BucketManager {
	return &BucketManager{
		bucket:   b,
		username: username,
		password: password,
	}
}
