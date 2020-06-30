package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestClusterWaitUntilReady() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	err = c.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	// Just test that we can use the cluster.
	buckets, err := c.Buckets().GetAllBuckets(nil)
	suite.Require().Nil(err, err)

	suite.Assert().GreaterOrEqual(len(buckets), 1)
}

func (suite *IntegrationTestSuite) TestClusterWaitUntilReadyInvalidAuth() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password + "nopethisshouldntwork",
	}})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	start := time.Now()
	err = c.WaitUntilReady(7*time.Second, nil)
	if !errors.Is(err, ErrUnambiguousTimeout) {
		suite.T().Fatalf("Expected unambiguous timeout error but was %v", err)
	}

	elapsed := time.Since(start)
	suite.Assert().GreaterOrEqual(int64(elapsed), int64(7*time.Second))
	suite.Assert().LessOrEqual(int64(elapsed), int64(8*time.Second))
}
