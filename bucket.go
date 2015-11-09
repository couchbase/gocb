package gocb

import (
	"github.com/couchbase/gocb/gocbcore"
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

	queryCacheLock  sync.RWMutex
	queryCache      map[string]*n1qlCache

	internal        *bucketInternal
}

func createBucket(config *gocbcore.AgentConfig) (*Bucket, error) {
	cli, err := gocbcore.CreateAgent(config)
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{
		name:       config.BucketName,
		password:   config.Password,
		client:     cli,
		transcoder: &DefaultTranscoder{},
		queryCache: make(map[string]*n1qlCache),

		opTimeout:       2500 * time.Millisecond,
		duraTimeout:     40000 * time.Millisecond,
		duraPollTimeout: 100 * time.Millisecond,
	}
	bucket.internal = &bucketInternal{
		b: bucket,
	}
	return bucket, nil
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

func (b *Bucket) InvalidateQueryCache() {
	b.queryCacheLock.Lock()
	b.queryCache = make(map[string]*n1qlCache)
	b.queryCacheLock.Unlock()
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

func (b *Bucket) Close() {
	b.client.Close()
}

func (b *Bucket) IoRouter() *gocbcore.Agent {
	return b.client
}

// Internal methods, not safe to be consumed by third parties.
func (b *Bucket) Internal() *bucketInternal {
	return b.internal
}

func (b *Bucket) Manager(username, password string) *BucketManager {
	return &BucketManager{
		bucket:   b,
		username: username,
		password: password,
	}
}
