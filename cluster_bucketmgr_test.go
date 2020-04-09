package gocb

import (
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
)

func (suite *IntegrationTestSuite) TestBucketMgrOps() {
	suite.skipIfUnsupported(BucketMgrFeature)

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
		suite.T().Fatalf("Failed to create bucket %v", err)
	}

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	var bucket *BucketSettings
	bucketSignal := make(chan struct{})
	go func() {
		for {
			// Check that we can get and re-upsert a bucket.
			bucket, err = mgr.GetBucket("test22", nil)
			if err != nil {
				suite.T().Logf("Failed to get bucket %v", err)
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
		suite.T().Fatalf("Wait timeout expired for bucket to become available")
	case <-bucketSignal:
	}

	err = mgr.UpdateBucket(*bucket, nil)
	if err != nil {
		suite.T().Fatalf("Failed to upsert bucket after get %v", err)
	}

	buckets, err := mgr.GetAllBuckets(nil)
	if err != nil {
		suite.T().Fatalf("Failed to get all buckets %v", err)
	}

	if len(buckets) == 0 {
		suite.T().Fatalf("Bucket settings list was length 0")
	}

	var b *BucketSettings
	for _, bucket := range buckets {
		if bucket.Name == "test22" {
			b = &bucket
		}
	}

	if b == nil {
		suite.T().Fatalf("Test bucket was not found in list of bucket settings, %v", buckets)
	}

	success := suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		err = mgr.FlushBucket("test22", nil)
		if err != nil {
			suite.T().Logf("Flush bucket failed with %s", err)
			return false
		}
		return true
	})

	if !success {
		suite.T().Fatal("Wait time for bucket flush expired")
	}

	success = suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		err = mgr.DropBucket("test22", nil)
		if err != nil {
			suite.T().Logf("Drop bucket failed with %s", err)
			return false
		}
		return true
	})

	if !success {
		suite.T().Fatal("Wait time for drop bucket expired")
	}
}

func (suite *IntegrationTestSuite) TestBucketMgrFlushDisabled() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: BucketSettings{
			Name:                 "testFlush",
			RAMQuotaMB:           100,
			NumReplicas:          0,
			BucketType:           CouchbaseBucketType,
			EvictionPolicy:       EvictionPolicyTypeValueOnly,
			FlushEnabled:         false,
			MaxTTL:               10,
			CompressionMode:      CompressionModeActive,
			ReplicaIndexDisabled: true,
		},
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create bucket %v", err)
	}
	defer mgr.DropBucket("testFlush", nil)

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	bucketSignal := make(chan struct{})
	go func() {
		for {
			// Check that we can get and re-upsert a bucket.
			_, err := mgr.GetBucket("testFlush", nil)
			if err != nil {
				suite.T().Logf("Failed to get bucket %v", err)
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
		suite.T().Fatalf("Wait timeout expired for bucket to become available")
	case <-bucketSignal:
	}

	err = mgr.FlushBucket("testFlush", nil)
	if err == nil {
		suite.T().Fatalf("Expected to fail to flush bucket")
	}

	if !errors.Is(err, ErrBucketNotFlushable) {
		suite.T().Fatalf("Expected error to be bucket not flushable but was %v", err)
	}
}
func (suite *IntegrationTestSuite) TestBucketMgrBucketNotExist() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	_, err := mgr.GetBucket("testBucketThatDoesNotExist", nil)
	if err == nil {
		suite.T().Fatalf("Expected to fail to get bucket")
	}

	if !errors.Is(err, ErrBucketNotFound) {
		suite.T().Fatalf("Expected error to be bucket not found but was %v", err)
	}
}
