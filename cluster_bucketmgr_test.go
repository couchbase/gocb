package gocb

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
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
		MaxExpiry:            10 * time.Second,
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
	suite.Assert().Equal(settings.MaxExpiry, bucket.MaxExpiry)
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

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_create_bucket"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_update_bucket"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_get_bucket"), 1, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_get_all_buckets"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_flush_bucket"), 1, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_drop_bucket"), 1, true)
}

func (suite *IntegrationTestSuite) TestBucketMgrFlushDisabled() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	deadline := time.Now().Add(10 * time.Second)
	for {
		err := mgr.CreateBucket(CreateBucketSettings{
			BucketSettings: BucketSettings{
				Name:                 "testFlush",
				RAMQuotaMB:           100,
				NumReplicas:          0,
				BucketType:           CouchbaseBucketType,
				EvictionPolicy:       EvictionPolicyTypeValueOnly,
				FlushEnabled:         false,
				MaxExpiry:            10,
				CompressionMode:      CompressionModeActive,
				ReplicaIndexDisabled: true,
			},
			ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
		}, nil)
		if err == nil {
			break
		}

		suite.T().Logf("Failed to create bucket %v", err)

		if time.Now().After(deadline) {
			suite.T().Fatalf("Deadline exceeded for create bucket, failing test")
		}
	}
	defer mgr.DropBucket("testFlush", nil)

	suite.Assert().True(suite.tryUntil(time.Now().Add(10*time.Second), 500*time.Millisecond, func() bool {
		err := mgr.FlushBucket("testFlush", nil)
		if err == nil {
			suite.T().Fatalf("Expected to fail to flush bucket")
		}

		if errors.Is(err, ErrBucketNotFlushable) {
			return true
		}

		suite.T().Logf("Expected error to be bucket not flushable but was %v", err)
		return false
	}))
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
	suite.Assert().Equal(time.Duration(0), bucket.MaxExpiry)
	suite.Assert().Equal(CompressionModeOff, bucket.CompressionMode)
	suite.Assert().True(bucket.ReplicaIndexDisabled)

	settings.MaxExpiry = 10 * time.Second
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
		MaxExpiry:      10 * time.Second,
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
	suite.Assert().Equal(settings.MaxExpiry, bucket.MaxExpiry)
	suite.Assert().Equal(CompressionModePassive, bucket.CompressionMode)
	suite.Assert().Equal(settings.EvictionPolicy, bucket.EvictionPolicy)
	suite.Assert().True(bucket.ReplicaIndexDisabled)

	settings = BucketSettings{
		Name:           "testephNRU",
		RAMQuotaMB:     100,
		NumReplicas:    1,
		BucketType:     EphemeralBucketType,
		FlushEnabled:   true,
		MaxExpiry:      10 * time.Second,
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
	suite.Assert().Equal(settings.MaxExpiry, bucket.MaxExpiry)
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

func (suite *IntegrationTestSuite) TestBucketMgrMinDurability() {
	suite.skipIfUnsupported(BucketMgrFeature)
	suite.skipIfUnsupported(BucketMgrDurabilityFeature)

	mgr := globalCluster.Buckets()

	type tCase struct {
		name     string
		settings BucketSettings
	}

	testCases := []tCase{
		{
			name: "none",
			settings: BucketSettings{
				Name:                   "testnone",
				RAMQuotaMB:             100,
				BucketType:             CouchbaseBucketType,
				MinimumDurabilityLevel: DurabilityLevelNone,
			},
		},
		{
			name: "majority",
			settings: BucketSettings{
				Name:                   "testmajority",
				RAMQuotaMB:             100,
				BucketType:             CouchbaseBucketType,
				MinimumDurabilityLevel: DurabilityLevelMajority,
			},
		},
		{
			name: "majorityPersistToMaster",
			settings: BucketSettings{
				Name:                   "testmajorityPersistToMaster",
				RAMQuotaMB:             100,
				BucketType:             CouchbaseBucketType,
				MinimumDurabilityLevel: DurabilityLevelMajorityAndPersistOnMaster,
			},
		},
		{
			name: "persistToMajority",
			settings: BucketSettings{
				Name:                   "testpersistToMajority",
				RAMQuotaMB:             100,
				BucketType:             CouchbaseBucketType,
				MinimumDurabilityLevel: DurabilityLevelPersistToMajority,
			},
		},
	}
	for _, tCase := range testCases {
		suite.T().Run(tCase.name, func(te *testing.T) {
			err := mgr.CreateBucket(CreateBucketSettings{
				BucketSettings:         tCase.settings,
				ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
			}, nil)
			if err != nil {
				te.Fatalf("Failed to create bucket %v", err)
			}

			defer mgr.DropBucket(tCase.settings.Name, nil)

			// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
			var bucket *BucketSettings
			success := suite.tryUntil(time.Now().Add(5*time.Second), 250*time.Millisecond, func() bool {
				bucket, err = mgr.GetBucket(tCase.settings.Name, nil)
				if err != nil {
					te.Logf("Failed to get bucket %v", err)
					return false
				}

				return true
			})

			if !success {
				te.Fatalf("GetBucket failed to execute within the required time")
			}

			if bucket.MinimumDurabilityLevel != tCase.settings.MinimumDurabilityLevel {
				te.Fatalf("Expected minimum durability level to be %d but was %d", tCase.settings.MinimumDurabilityLevel,
					bucket.MinimumDurabilityLevel)
			}
		})
	}
}

func (suite *IntegrationTestSuite) TestBucketMgrFlushBucketNotFound() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	err := mgr.FlushBucket("testFlushBucketNotFound", nil)
	if err == nil {
		suite.T().Fatalf("Expected to fail to flush bucket")
	}

	if !errors.Is(err, ErrBucketNotFound) {
		suite.T().Fatalf("Expected error to be bucket not found but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestBucketMgrStorageBackendCouchstore() {
	suite.skipIfUnsupported(BucketMgrFeature)
	suite.skipIfUnsupported(StorageBackendFeature)

	mgr := globalCluster.Buckets()

	bName := "testcouchbasestorage"
	settings := BucketSettings{
		Name:           bName,
		RAMQuotaMB:     100,
		NumReplicas:    1,
		BucketType:     CouchbaseBucketType,
		StorageBackend: StorageBackendCouchstore,
	}

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	suite.Require().Nil(err, err)
	defer mgr.DropBucket(bName, nil)

	b, err := mgr.GetBucket(bName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Equal(StorageBackendCouchstore, b.StorageBackend)
}

func (suite *IntegrationTestSuite) TestBucketMgrStorageBackendMagma() {
	suite.skipIfUnsupported(BucketMgrFeature)
	suite.skipIfUnsupported(StorageBackendFeature)

	mgr := globalCluster.Buckets()

	bName := "magma"
	settings := BucketSettings{
		Name:           bName,
		RAMQuotaMB:     1024,
		NumReplicas:    1,
		BucketType:     CouchbaseBucketType,
		StorageBackend: StorageBackendMagma,
	}

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings: settings,
	}, nil)
	suite.Require().Nil(err, err)
	defer mgr.DropBucket(bName, nil)

	var bucket *BucketSettings
	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		bucket, err = mgr.GetBucket(bName, nil)
		if err != nil {
			suite.T().Logf("Failed to get bucket %v", err)
			return false
		}

		return true
	})
	suite.Assert().True(success, "GetBucket failed to execute within the required time")

	suite.Assert().Equal(StorageBackendMagma, bucket.StorageBackend)
	suite.Assert().Equal(1024, int(bucket.RAMQuotaMB))

	bucket.RAMQuotaMB = 1124
	err = mgr.UpdateBucket(*bucket, nil)
	suite.Require().Nil(err, err)

	bucket, err = mgr.GetBucket(bName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Equal(StorageBackendMagma, bucket.StorageBackend)
	suite.Assert().Equal(1124, int(bucket.RAMQuotaMB))
}

func (suite *IntegrationTestSuite) TestBucketMgrCustomConflictResolution() {
	suite.skipIfUnsupported(BucketMgrFeature)
	suite.skipIfUnsupported(CustomConflictResolutionFeature)

	mgr := globalCluster.Buckets()

	bName := "testcouchbaseccr"
	settings := BucketSettings{
		Name:           bName,
		RAMQuotaMB:     100,
		NumReplicas:    1,
		BucketType:     CouchbaseBucketType,
		StorageBackend: StorageBackendCouchstore,
	}

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings:         settings,
		ConflictResolutionType: ConflictResolutionTypeCustom,
	}, nil)
	suite.Require().Nil(err, err)
	defer mgr.DropBucket(bName, nil)

	// Can't check Conflict resolution of bucket
	b, err := mgr.GetBucket(bName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(bName, b.Name)
}

func (suite *IntegrationTestSuite) TestBucketMgrHistoryRetention() {
	suite.skipIfUnsupported(BucketMgrFeature)
	suite.skipIfUnsupported(HistoryRetentionFeature)

	mgr := globalCluster.Buckets()

	bName := "a" + uuid.NewString()[:6]
	settings := BucketSettings{
		Name:           bName,
		RAMQuotaMB:     1024,
		NumReplicas:    1,
		BucketType:     CouchbaseBucketType,
		StorageBackend: StorageBackendMagma,
		HistoryRetentionCollectionDefault: &HistoryRetentionCollectionDefaultSettings{
			Enabled: true,
		},
		HistoryRetentionBytes:    2147483648,
		HistoryRetentionDuration: 13000 * time.Second,
	}

	err := mgr.CreateBucket(CreateBucketSettings{
		BucketSettings:         settings,
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}, nil)
	suite.Require().Nil(err, err)
	defer mgr.DropBucket(bName, nil)

	b, err := mgr.GetBucket(bName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(bName, b.Name)

	if suite.Assert().NotNil(b.HistoryRetentionCollectionDefault) {
		suite.Assert().True(b.HistoryRetentionCollectionDefault.Enabled)
	}
	suite.Assert().Equal(settings.HistoryRetentionDuration, b.HistoryRetentionDuration)
	suite.Assert().Equal(settings.HistoryRetentionBytes, b.HistoryRetentionBytes)

	b.HistoryRetentionCollectionDefault = &HistoryRetentionCollectionDefaultSettings{
		Enabled: false,
	}

	err = mgr.UpdateBucket(*b, nil)
	suite.Require().Nil(err, err)

	b, err = mgr.GetBucket(bName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(bName, b.Name)

	if suite.Assert().NotNil(b.HistoryRetentionCollectionDefault) {
		suite.Assert().False(b.HistoryRetentionCollectionDefault.Enabled)
	}
}
