package gocb

import (
	"fmt"
	"time"
)

func (suite *IntegrationTestSuite) TestScopeAnalyticsQuery() {
	suite.skipIfUnsupported(CollectionsAnalyticsFeature)

	n := suite.setupScopeAnalytics()
	query := fmt.Sprintf("SELECT * FROM %s.%s.%s WHERE service=? LIMIT %d;",
		globalBucket.Name(),
		globalScope.Name(),
		globalCollection.Name(), n)
	suite.runAnalyticsTest(n, query, globalScope)
}

func (suite *IntegrationTestSuite) setupScopeAnalytics() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "analytics", "", globalCollection.Name())
	suite.Require().Nil(err, "Failed to create dataset %v", err)

	_, err = globalCluster.AnalyticsQuery(
		fmt.Sprintf("ALTER COLLECTION %s.%s.%s ENABLE ANALYTICS",
			globalBucket.Name(),
			globalScope.Name(),
			globalCollection.Name(),
		),
		&AnalyticsOptions{
			Timeout: 10 * time.Second,
		},
	)
	suite.Require().Nil(err, "Failed to create analytics collection %v", err)

	return n
}
