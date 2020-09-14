package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestQueryIndexesCrud() {
	suite.skipIfUnsupported(QueryIndexFeature)

	bucketMgr := globalCluster.Buckets()
	bucketName := "testIndexes"

	err := bucketMgr.CreateBucket(CreateBucketSettings{
		BucketSettings: BucketSettings{
			Name:        bucketName,
			RAMQuotaMB:  100,
			NumReplicas: 0,
			BucketType:  CouchbaseBucketType,
		},
	}, nil)
	suite.Require().Nil(err, err)
	defer bucketMgr.DropBucket(bucketName, nil)

	mgr := globalCluster.QueryIndexes()

	deadline := time.Now().Add(5 * time.Second)
	for {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			IgnoreIfExists: true,
		})
		if err == nil {
			break
		}

		suite.T().Logf("Failed to create primary index: %s", err)

		sleepDeadline := time.Now().Add(500 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			suite.T().Errorf("timed out waiting for create index to succeed")
			return
		}
	}

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	// We create this first to give it a chance to be created by the time we need it.
	err = mgr.CreateIndex(bucketName, "testIndexDeferred", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		Deferred:       true,
	})
	suite.Require().Nil(err, err)

	indexNames, err := mgr.BuildDeferredIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes(bucketName, []string{"testIndexDeferred"}, 10*time.Second, nil)
	suite.Require().Nil(err, err)

	indexes, err := mgr.GetAllIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexes, 3)

	err = mgr.DropIndex(bucketName, "testIndex", nil)
	suite.Require().Nil(err, err)

	err = mgr.DropIndex(bucketName, "testIndex", nil)
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	err = mgr.DropPrimaryIndex(bucketName, nil)
	suite.Require().Nil(err, err)

	err = mgr.DropPrimaryIndex(bucketName, nil)
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}
}
