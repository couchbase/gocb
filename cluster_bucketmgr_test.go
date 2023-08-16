package gocb

import (
	"errors"
	"testing"
	"time"
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
	suite.Require().NoError(err, "Failed to create bucket")

	// Buckets don't become available immediately so we need to do a bit of polling to see if it comes online.
	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), "test22", nil)

	bucket, err := mgr.GetBucket("test22", nil)
	suite.Require().NoError(err, "Failed to get bucket")

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
	suite.Require().NoError(err, "Failed to update bucket")

	buckets, err := mgr.GetAllBuckets(nil)
	suite.Require().NoError(err, "Failed to get all buckets")

	suite.Assert().NotZero(len(buckets), "GetAllBuckets returned no buckets")

	var b *BucketSettings
	for _, bucket := range buckets {
		if bucket.Name == "test22" {
			b = &bucket
		}
	}

	suite.Assert().NotNilf(b, "Test bucket was not found in list of bucket settings, %v", buckets)

	suite.Assert().True(suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		err = mgr.FlushBucket("test22", nil)
		if err != nil {
			suite.T().Logf("Flush bucket failed with %s", err)
			return false
		}
		return true
	}), "Flush bucket did not succeed in time")

	suite.Assert().True(suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		err = mgr.DropBucket("test22", nil)
		if err != nil {
			suite.T().Logf("Drop bucket failed with %s", err)
			return false
		}
		return true
	}), "Drop bucket did not succeed in time")

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
	suite.Require().NoError(err, "Failed to create bucket")
	defer mgr.DropBucket("testFlush", nil)

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), "testFlush", nil)

	err = mgr.FlushBucket("testFlush", nil)
	suite.Require().ErrorIs(err, ErrBucketNotFlushable)
}

func (suite *IntegrationTestSuite) TestBucketMgrBucketNotExist() {
	suite.skipIfUnsupported(BucketMgrFeature)

	mgr := globalCluster.Buckets()

	_, err := mgr.GetBucket("testBucketThatDoesNotExist", nil)
	suite.Assert().ErrorIs(err, ErrBucketNotFound)
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
	suite.Require().NoError(err, "Failed to create bucket")
	defer mgr.DropBucket("testmemd", nil)

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), "testmemd", nil)

	bucket, err := mgr.GetBucket("testmemd", nil)
	suite.Require().NoError(err, "Failed to get bucket")

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
	suite.Require().ErrorIs(err, ErrInvalidArgument)
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
	suite.Require().NoError(err, "Failed to create bucket")

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), "testeph", nil)

	bucket, err := mgr.GetBucket("testeph", nil)
	suite.Require().NoError(err, "Failed to get bucket")

	mgr.DropBucket("testeph", nil)

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
	suite.Require().NoError(err, "Failed to create bucket")
	defer mgr.DropBucket("testephNRU", nil)

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), "testephNRU", nil)

	bucket, err = mgr.GetBucket("testephNRU", nil)
	suite.Require().NoError(err, "Failed to get bucket")

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)

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
	suite.Assert().ErrorIs(err, ErrInvalidArgument)
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

			suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), tCase.settings.Name, nil)

			bucket, err := mgr.GetBucket(tCase.settings.Name, nil)
			if err != nil {
				te.Fatalf("Failed to get bucket %v", err)
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
	suite.Require().ErrorIs(err, ErrBucketNotFound)
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

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), bName, nil)

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

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), bName, nil)

	bucket, err := mgr.GetBucket(bName, nil)
	suite.Require().NoError(err, "Failed to get bucket")

	suite.Assert().Equal(StorageBackendMagma, bucket.StorageBackend)
	suite.Assert().Equal(1024, int(bucket.RAMQuotaMB))

	bucket.RAMQuotaMB = 1124
	err = mgr.UpdateBucket(*bucket, nil)
	suite.Require().Nil(err, err)

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), bName, func(bucket *BucketSettings) bool {
		return bucket.RAMQuotaMB == 1124
	})

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

	suite.EnsureBucketOnAllNodes(time.Now().Add(30*time.Second), bName, nil)

	// Can't check Conflict resolution of bucket
	b, err := mgr.GetBucket(bName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(bName, b.Name)
}
