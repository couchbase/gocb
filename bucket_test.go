package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestBucketWaitUntilReady() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	b := c.Bucket(globalConfig.Bucket)

	err = b.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	// Just test that we can use the bucket.
	_, err = b.DefaultCollection().Upsert("TestBucketWaitUntilReady", "test", nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) TestBucketWaitUntilReadyInvalidAuth() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password + "nopethisshouldntwork",
	}})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	b := c.Bucket(globalConfig.Bucket)

	start := time.Now()
	err = b.WaitUntilReady(7*time.Second, nil)
	if !errors.Is(err, ErrUnambiguousTimeout) {
		suite.T().Fatalf("Expected unambiguous timeout error but was %v", err)
	}

	elapsed := time.Since(start)
	suite.Assert().GreaterOrEqual(int64(elapsed), int64(7*time.Second))
	suite.Assert().LessOrEqual(int64(elapsed), int64(8*time.Second))
}
