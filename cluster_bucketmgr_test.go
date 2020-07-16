package gocb

import (
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
)

func (suite *IntegrationTestSuite) TestBucketMgrOps() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	settings := BucketSettings{
		Name:                 "test22",
		RAMQuotaMB:           100,
		NumReplicas:          1,
		BucketType:           CouchbaseBucketType,
		EvictionPolicy:       EvictionPolicyTypeValueOnly,
		FlushEnabled:         true,
		MaxTTL:               10 * time.Second,
		CompressionMode:      CompressionModeActive,
		ReplicaIndexDisabled: true,
	}
	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings:         settings,
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create bucket %v", err)
	}

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	var bucket *BucketSettings
	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		bucket, err = mgr.GetBucket("test22", nil)
		if err != nil {
			suite.T().Logf("Failed to get bucket %v", err)
			return false
		}

		return true
	})

	suite.Assert().True(success, "GetBucket failed to execute within the required time")

	suite.Assert().Equal(settings.BucketType, bucket.BucketType)
	suite.Assert().Equal(settings.Name, bucket.Name)
	suite.Assert().Equal(settings.RAMQuotaMB, bucket.RAMQuotaMB)
	suite.Assert().Equal(settings.NumReplicas, bucket.NumReplicas)
	suite.Assert().Equal(settings.FlushEnabled, bucket.FlushEnabled)
	suite.Assert().Equal(settings.MaxTTL, bucket.MaxTTL)
	suite.Assert().Equal(settings.EvictionPolicy, bucket.EvictionPolicy)
	suite.Assert().Equal(settings.CompressionMode, bucket.CompressionMode)
	suite.Assert().True(bucket.ReplicaIndexDisabled)

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

	success = suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
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

func (suite *IntegrationTestSuite) TestBucketMgrMemcached() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	settings := BucketSettings{
		Name:                 "testmemd",
		RAMQuotaMB:           100,
		NumReplicas:          0,
		BucketType:           MemcachedBucketType,
		FlushEnabled:         true,
		ReplicaIndexDisabled: true,
	}
	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create bucket %v", err)
	}
	defer mgr.DropBucket("testmemd", nil)

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	var bucket *BucketSettings
	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		bucket, err = mgr.GetBucket("testmemd", nil)
		if err != nil {
			suite.T().Logf("Failed to get bucket %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "GetBucket failed to execute within the required time")

	suite.Assert().Equal(settings.BucketType, bucket.BucketType)
	suite.Assert().Equal(settings.Name, bucket.Name)
	suite.Assert().Equal(settings.RAMQuotaMB, bucket.RAMQuotaMB)
	suite.Assert().Equal(settings.NumReplicas, bucket.NumReplicas)
	suite.Assert().Equal(settings.FlushEnabled, bucket.FlushEnabled)
	suite.Assert().Equal(time.Duration(0), bucket.MaxTTL)
	suite.Assert().Equal(CompressionModeOff, bucket.CompressionMode)
	suite.Assert().True(bucket.ReplicaIndexDisabled)

	settings.MaxTTL = 10 * time.Second
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)

	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected invalid argument error but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestBucketMgrEphemeral() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	settings := BucketSettings{
		Name:           "testeph",
		RAMQuotaMB:     100,
		NumReplicas:    1,
		BucketType:     EphemeralBucketType,
		FlushEnabled:   true,
		MaxTTL:         10 * time.Second,
		EvictionPolicy: EvictionPolicyTypeNoEviction,
	}

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create bucket %v", err)
	}
	defer mgr.DropBucket("testeph", nil)

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	var bucket *BucketSettings
	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		bucket, err = mgr.GetBucket("testeph", nil)
		if err != nil {
			suite.T().Logf("Failed to get bucket %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "GetBucket failed to execute within the required time")

	suite.Assert().Equal(settings.BucketType, bucket.BucketType)
	suite.Assert().Equal(settings.Name, bucket.Name)
	suite.Assert().Equal(settings.RAMQuotaMB, bucket.RAMQuotaMB)
	suite.Assert().Equal(settings.NumReplicas, bucket.NumReplicas)
	suite.Assert().Equal(settings.FlushEnabled, bucket.FlushEnabled)
	suite.Assert().Equal(settings.MaxTTL, bucket.MaxTTL)
	suite.Assert().Equal(CompressionModePassive, bucket.CompressionMode)
	suite.Assert().Equal(settings.EvictionPolicy, bucket.EvictionPolicy)
	suite.Assert().True(bucket.ReplicaIndexDisabled)

	settings = BucketSettings{
		Name:           "testephNRU",
		RAMQuotaMB:     100,
		NumReplicas:    1,
		BucketType:     EphemeralBucketType,
		FlushEnabled:   true,
		MaxTTL:         10 * time.Second,
		EvictionPolicy: EvictionPolicyTypeNotRecentlyUsed,
	}

	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create bucket %v", err)
	}
	defer mgr.DropBucket("testephNRU", nil)

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	success = suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		bucket, err = mgr.GetBucket("testephNRU", nil)
		if err != nil {
			suite.T().Logf("Failed to get bucket %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "GetBucket failed to execute within the required time")

	suite.Assert().Equal(settings.BucketType, bucket.BucketType)
	suite.Assert().Equal(settings.Name, bucket.Name)
	suite.Assert().Equal(settings.RAMQuotaMB, bucket.RAMQuotaMB)
	suite.Assert().Equal(settings.NumReplicas, bucket.NumReplicas)
	suite.Assert().Equal(settings.FlushEnabled, bucket.FlushEnabled)
	suite.Assert().Equal(settings.MaxTTL, bucket.MaxTTL)
	suite.Assert().Equal(CompressionModePassive, bucket.CompressionMode)
	suite.Assert().Equal(settings.EvictionPolicy, bucket.EvictionPolicy)
	suite.Assert().True(bucket.ReplicaIndexDisabled)
}

func (suite *IntegrationTestSuite) TestBucketMgrInvalidEviction() {
	mgr := globalCluster.Buckets()

	settings := BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     MemcachedBucketType,
		EvictionPolicy: EvictionPolicyTypeValueOnly,
	}
	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     MemcachedBucketType,
		EvictionPolicy: EvictionPolicyTypeFull,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     MemcachedBucketType,
		EvictionPolicy: EvictionPolicyTypeNotRecentlyUsed,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     MemcachedBucketType,
		EvictionPolicy: EvictionPolicyTypeNoEviction,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     CouchbaseBucketType,
		EvictionPolicy: EvictionPolicyTypeNoEviction,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     CouchbaseBucketType,
		EvictionPolicy: EvictionPolicyTypeNotRecentlyUsed,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     EphemeralBucketType,
		EvictionPolicy: EvictionPolicyTypeValueOnly,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}

	settings = BucketSettings{
		Name:           "test",
		RAMQuotaMB:     100,
		NumReplicas:    0,
		BucketType:     EphemeralBucketType,
		EvictionPolicy: EvictionPolicyTypeFull,
	}
	err = mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Error should have been invalid argument, was %v", err)
	}
}
