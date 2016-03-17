package gocb

import (
	"github.com/couchbase/gocb/gocbcore"
	"math/rand"
	"time"
)

// An interface representing a single bucket within a cluster.
type Bucket struct {
	cluster  *Cluster
	name     string
	password string
	client   *gocbcore.Agent

	transcoder      Transcoder
	opTimeout       time.Duration
	duraTimeout     time.Duration
	duraPollTimeout time.Duration
	viewTimeout     time.Duration
	n1qlTimeout     time.Duration

	internal *bucketInternal
}

func createBucket(cluster *Cluster, config *gocbcore.AgentConfig) (*Bucket, error) {
	cli, err := gocbcore.CreateAgent(config)
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{
		cluster:    cluster,
		name:       config.BucketName,
		password:   config.Password,
		client:     cli,
		transcoder: &DefaultTranscoder{},

		opTimeout:       2500 * time.Millisecond,
		duraTimeout:     40000 * time.Millisecond,
		duraPollTimeout: 100 * time.Millisecond,
		viewTimeout:     75 * time.Second,
		n1qlTimeout:     75 * time.Second,
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
func (b *Bucket) ViewTimeout() time.Duration {
	return b.viewTimeout
}
func (b *Bucket) SetViewTimeout(timeout time.Duration) {
	b.viewTimeout = timeout
}
func (b *Bucket) N1qlTimeout() time.Duration {
	return b.n1qlTimeout
}
func (b *Bucket) SetN1qlTimeout(timeout time.Duration) {
	b.n1qlTimeout = timeout
}

func (b *Bucket) SetTranscoder(transcoder Transcoder) {
	b.transcoder = transcoder
}

func (b *Bucket) InvalidateQueryCache() {
	b.cluster.InvalidateQueryCache()
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
	b.cluster.closeBucket(b)
	b.client.Close()
}

func (b *Bucket) IoRouter() *gocbcore.Agent {
	return b.client
}

// *INTERNAL*
// Internal methods, not safe to be consumed by third parties.
func (b *Bucket) Internal() *bucketInternal {
	return b.internal
}

func (b *Bucket) Manager(username, password string) *BucketManager {
	userPass := userPassPair{username, password}
	if username == "" || password == "" {
		if b.cluster.auth != nil {
			userPass = b.cluster.auth.bucketMgmt(b.name)
		}
	}

	return &BucketManager{
		bucket:   b,
		username: userPass.Username,
		password: userPass.Password,
	}
}
