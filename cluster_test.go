package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestClusterWaitUntilReady() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator:  globalConfig.Auth,
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err)
	defer c.Close(nil)

	err = c.WaitUntilReady(7*time.Second, nil)
	suite.Require().NoError(err)

	// Just test that we can use the cluster.
	_, err = c.Bucket(globalBucket.Name()).DefaultCollection().Upsert("test", "test", nil)
	suite.Require().NoError(err)
}

func (suite *IntegrationTestSuite) TestClusterWaitUntilReadyInvalidAuth() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)
	suite.skipIfUnsupported(WaitUntilReadyAuthFailFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password + "nopethisshouldntwork",
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err)
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

func (suite *IntegrationTestSuite) TestClusterWaitUntilReadyFastFailAuth() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)
	suite.skipIfUnsupported(WaitUntilReadyFastFailFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: "thisisaprettyunlikelypasswordtobeused",
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err)
	defer c.Close(nil)

	err = c.WaitUntilReady(7*time.Second, &WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if !errors.Is(err, ErrAuthenticationFailure) {
		suite.T().Fatalf("Expected authentication error but was: %v", err)
	}
}

func (suite *IntegrationTestSuite) TestClusterWaitUntilReadyFastFailConnStr() {
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
	suite.Require().NoError(err)
	defer c.Close(nil)

	err = c.WaitUntilReady(7*time.Second, &WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if !errors.Is(err, ErrTimeout) {
		suite.T().Fatalf("Expected timeout error but was: %v", err)
	}
}

func (suite *IntegrationTestSuite) TestClusterWaitUntilReadyKeyValueService() {
	suite.skipIfUnsupported(WaitUntilReadyFeature)
	suite.skipIfUnsupported(WaitUntilReadyClusterFeature)

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err)
	defer c.Close(nil)

	err = c.WaitUntilReady(7*time.Second, &WaitUntilReadyOptions{
		ServiceTypes: []ServiceType{ServiceTypeKeyValue},
	})
	if globalCluster.IsProtostellar() {
		// couchbase2 ignores ServiceTypes, as the health check has no per-service granularity.
		suite.Require().NoError(err)
		return
	}
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected error to be invalid argument but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestClusterWaitUntilReadyCouchbase2Unreachable() {
	if !globalCluster.IsProtostellar() {
		suite.T().Skip("Skipping test as it only applies to couchbase2")
	}

	testCases := []struct {
		name    string
		connStr string
	}{
		{name: "InvalidHostname", connStr: "couchbase2://nonexistent.invalid"},
		{name: "NothingListening", connStr: "couchbase2://127.0.0.1:1"},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			c, err := Connect(tc.connStr, ClusterOptions{
				Authenticator:  globalConfig.Auth,
				SecurityConfig: globalConfig.SecurityConfig,
			})
			suite.Require().NoError(err)
			defer c.Close(nil)

			err = c.WaitUntilReady(2*time.Second, nil)
			suite.Require().ErrorIs(err, ErrUnambiguousTimeout)

			var tErr *TimeoutError
			suite.Require().ErrorAs(err, &tErr)
			suite.Assert().Contains(tErr.RetryReasons, ServiceNotAvailableRetryReason)
		})
	}
}

func (suite *IntegrationTestSuite) CreateAndCloseNewCluster() *Cluster {
	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err, err)
	err = c.Close(nil)
	suite.Require().NoError(err, err)
	return c
}

func (suite *IntegrationTestSuite) CreateAndCloseNewClusterAndBucket() (*Cluster, *Bucket) {
	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		},
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err, err)
	// We need to open bucket before closing cluster otherwise we'll always just surface the error assigned on the call
	// to Bucket().
	b := c.Bucket(globalBucket.Name())
	err = c.Close(nil)
	suite.Require().NoError(err, err)
	return c, b
}

func (suite *IntegrationTestSuite) TestClusterCloseTwice() {
	c := suite.CreateAndCloseNewCluster()
	err := c.Close(nil)
	suite.Require().ErrorIs(err, ErrShutdown)
}

func (suite *IntegrationTestSuite) TestClusterOpsAfterClusterClose() {
	c := suite.CreateAndCloseNewCluster()
	suite.Run("Diagnostics", func() {
		_, err := c.Diagnostics(nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("Ping", func() {
		_, err := c.Ping(nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("WaitUntilReady", func() {
		err := c.WaitUntilReady(5*time.Second, nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("Bucket", func() {
		b := c.Bucket("test")
		suite.Require().ErrorIs(b.bootstrapError, ErrShutdown)
	})
	suite.Run("Query", func() {
		_, err := c.Query("SELECT 1", nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("AnalyticsQuery", func() {
		_, err := c.AnalyticsQuery("SELECT 1", nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
	suite.Run("GetAllBuckets", func() {
		_, err := c.Buckets().GetAllBuckets(nil)
		suite.Require().ErrorIs(err, ErrShutdown)
	})
}
