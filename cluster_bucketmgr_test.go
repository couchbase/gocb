package gocb

import (
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

func TestBucketMgrOps(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if globalCluster.NotSupportsFeature(BucketMgrFeature) {
		t.Skip("Skipping test as bucket manager not supported.")
	}

	mgr := globalCluster.Buckets()

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: BucketSettings{
			Name:                 "test22",
			RAMQuotaMB:           100,
			NumReplicas:          0,
			BucketType:           MemcachedBucketType,
			EvictionPolicy:       EvictionPolicyTypeValueOnly,
			FlushEnabled:         true,
			MaxTTL:               10,
			CompressionMode:      CompressionModeActive,
			ReplicaIndexDisabled: true,
		},
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create bucket manager %v", err)
	}

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	var bucket *BucketSettings
	bucketSignal := make(chan struct{})
	go func() {
		for {
			// Check that we can get and re-upsert a bucket.
			bucket, err = mgr.GetBucket("test22", nil)
			if err != nil {
				t.Logf("Failed to get bucket %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			bucketSignal <- struct{}{}
			break
		}
	}()

	maxWaitTime := 2 * time.Second
	timer := gocbcore.AcquireTimer(maxWaitTime)
	select {
	case <-timer.C:
		gocbcore.ReleaseTimer(timer, true)
		t.Fatalf("Wait timeout expired for bucket to become available")
	case <-bucketSignal:
	}

	err = mgr.UpdateBucket(*bucket, nil)
	if err != nil {
		t.Fatalf("Failed to upsert bucket after get %v", err)
	}

	buckets, err := mgr.GetAllBuckets(nil)
	if err != nil {
		t.Fatalf("Failed to get all buckets %v", err)
	}

	if len(buckets) == 0 {
		t.Fatalf("Bucket settings list was length 0")
	}

	var b *BucketSettings
	for _, bucket := range buckets {
		if bucket.Name == "test22" {
			b = &bucket
		}
	}

	if b == nil {
		t.Fatalf("Test bucket was not found in list of bucket settings, %v", buckets)
	}

	err = mgr.FlushBucket("test22", nil)
	if err != nil {
		t.Fatalf("Failed to flush bucket manager %v", err)
	}

	err = mgr.DropBucket("test22", nil)
	if err != nil {
		t.Fatalf("Failed to drop bucket manager %v", err)
	}
}
