package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestBucketWaitUntilReady() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}})
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

	c, err := Connect(globalConfig.connstr, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password + "nopethisshouldntwork",
	}})
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

	c, err := Connect(globalConfig.connstr, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: "thisisaprettyunlikelypasswordtobeused",
	}})
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

	c, err := Connect("10.10.10.10", ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}})
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
