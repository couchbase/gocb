package gocb

import (
	"github.com/couchbase/gocb/gocbcore"
	"math/rand"
	"time"
)

// Bucket is an interface representing a single bucket within a cluster.
type Bucket struct {
	cluster   *Cluster
	name      string
	password  string
	client    *gocbcore.Agent
	mtEnabled bool

	transcoder      Transcoder
	opTimeout       time.Duration
	duraTimeout     time.Duration
	duraPollTimeout time.Duration
	viewTimeout     time.Duration
	n1qlTimeout     time.Duration
	ftsTimeout      time.Duration

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
		mtEnabled:  config.UseMutationTokens,
		transcoder: &DefaultTranscoder{},

		opTimeout:       2500 * time.Millisecond,
		duraTimeout:     40000 * time.Millisecond,
		duraPollTimeout: 100 * time.Millisecond,
		viewTimeout:     75 * time.Second,
		n1qlTimeout:     75 * time.Second,
		ftsTimeout:      75 * time.Second,
	}
	bucket.internal = &bucketInternal{
		b: bucket,
	}
	return bucket, nil
}

// OperationTimeout returns the int64 Duration encoded timeout for accessing a Bucket
func (b *Bucket) OperationTimeout() time.Duration {
	return b.opTimeout
}

// SetOperationTimeout allows for setting a Bucket timeout with an int64 time.Duration
func (b *Bucket) SetOperationTimeout(timeout time.Duration) {
	b.opTimeout = timeout
}

// DurabilityTimeout returns the specified Bucket durability timeout in int64 time.Duration
func (b *Bucket) DurabilityTimeout() time.Duration {
	return b.duraTimeout
}

// SetDurabilityTimeout specifies the Bucket durability timeout via an int64 time.Duration
func (b *Bucket) SetDurabilityTimeout(timeout time.Duration) {
	b.duraTimeout = timeout
}

// DurabilityPollTimeout returns the specified Bucket durability poll timeout in int64 time.Duration
func (b *Bucket) DurabilityPollTimeout() time.Duration {
	return b.duraPollTimeout
}

// SetDurabilityPollTimeout sets the specified Bucket's durability poll timeout via an int64 time.Duration
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

// SetTranscoder sets the Bucket's transcoder
func (b *Bucket) SetTranscoder(transcoder Transcoder) {
	b.transcoder = transcoder
}

// InvalidateQueryCache Invalidates and clears the query cache. This method can be used to explicitly clear the internal N1QL query cache. This cache will be filled with non-adhoc query statements (query plans) to speed up those subsequent executions. Triggering this method will wipe out the complete cache, which will not cause an interruption but rather all queries need to be re-prepared internally. This method is likely to be deprecated in the future once the server side query engine distributes its state throughout the cluster.
func (b *Bucket) InvalidateQueryCache() {
	b.cluster.InvalidateQueryCache()
}

// Cas is acronym for "Check and Set" and is useful for ensuring that a mutation of a document by one user or thread does not override another near simultaneous mutation by another user or thread. The CAS value is returned by the server with the result when you perform a read on a document using Get or when you perform a mutation on a document using Insert, Upsert, Replace or Remove.
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

func (b *Bucket) getFtsEp() (string, error) {
	ftsEps := b.client.FtsEps()
	if len(ftsEps) == 0 {
		return "", &clientError{"No available FTS nodes."}
	}
	return ftsEps[rand.Intn(len(ftsEps))], nil
}

// Close the instance’s underlying socket resources.  Note that operations pending on the connection may fail.
func (b *Bucket) Close() {
	b.cluster.closeBucket(b)
	b.client.Close()
}

// IoRouter returns a pointer to the Bucket's client
func (b *Bucket) IoRouter() *gocbcore.Agent {
	return b.client
}

// *INTERNAL*
// Internal methods, not safe to be consumed by third parties.
func (b *Bucket) Internal() *bucketInternal {
	return b.internal
}

// Manager returns a pointer to a BucketManager
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
