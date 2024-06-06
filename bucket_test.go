package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestBucketWaitUntilReady() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator:  globalConfig.Auth,
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	b := c.Bucket(globalConfig.Bucket)

	err = b.WaitUntilReady(globalCluster.waitUntilReadyTimeout(), nil)
	suite.Require().Nil(err, err)

	// Just test that we can use the bucket.
	_, err = b.DefaultCollection().Upsert(generateDocId("TestBucketWaitUntilReady"), "test", nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) TestBucketWaitUntilReadyInvalidAuth() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyAuthFailFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password + "nopethisshouldntwork",
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	b := c.Bucket(globalConfig.Bucket)

	start := time.Now()
	err = b.WaitUntilReady(globalCluster.waitUntilReadyTimeout(), nil)
	if !errors.Is(err, ErrUnambiguousTimeout) {
		suite.T().Fatalf("Expected unambiguous timeout error but was %v", err)
	}

	elapsed := time.Since(start)
	suite.Assert().GreaterOrEqual(int64(elapsed), int64(globalCluster.waitUntilReadyTimeout()))
	suite.Assert().LessOrEqual(int64(elapsed), int64(globalCluster.waitUntilReadyTimeout()+time.Second))
}

func (suite *IntegrationTestSuite) TestBucketWaitUntilReadyFastFailAuth() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyFastFailFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: "thisisaprettyunlikelypasswordtobeused",
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	b := c.Bucket(globalConfig.Bucket)

	err = b.WaitUntilReady(globalCluster.waitUntilReadyTimeout(), &WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if !errors.Is(err, ErrAuthenticationFailure) {
		suite.T().Fatalf("Expected authentication error but was: %v", err)
	}
}

func (suite *IntegrationTestSuite) TestBucketWaitUntilReadyFastFailConnStr() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)
	suite.skipIfUnsupported(WaitUntilReadyFastFailFeature)

	c, err := Connect("10.10.10.10", ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	b := c.Bucket(globalConfig.Bucket)

	err = b.WaitUntilReady(globalCluster.waitUntilReadyTimeout(), &WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if !errors.Is(err, ErrTimeout) {
		suite.T().Fatalf("Expected timeout error but was: %v", err)
	}
}

func (suite *IntegrationTestSuite) TestBucketOpsAfterClusterClose() {
	_, b := suite.CreateAndCloseNewClusterAndBucket()
	suite.Run("Ping", func() {
		_, err := b.Ping(nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("WaitUntilReady", func() {
		err := b.WaitUntilReady(5*time.Second, nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("Internal.IORouter", func() {
		_, err := b.Internal().IORouter()
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("ViewQuery", func() {
		_, b := suite.CreateAndCloseNewClusterAndBucket()
		_, err := b.ViewQuery("test", "test", nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
}
