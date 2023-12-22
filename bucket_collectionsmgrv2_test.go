package gocb

func (suite *IntegrationTestSuite) TestCollectionManagerCrudV2() {
	suite.runCollectionManagerCrudTest(true)
}

func (suite *IntegrationTestSuite) TestDropNotExistentScopeV2() {
	suite.runDropNonExistentScopeTest(true)
}

func (suite *IntegrationTestSuite) TestDropNonExistentCollectionV2() {
	suite.runTestDropNonExistentCollectionTest(true)
}

func (suite *IntegrationTestSuite) TestCollectionsAreNotPresentV2() {
	suite.runCollectionsAreNotPresentTest(true)
}

func (suite *IntegrationTestSuite) TestDropScopesAreNotExistV2() {
	suite.runDropScopesAreNotExistTest(true)
}

func (suite *IntegrationTestSuite) TestGetAllScopesV2() {
	suite.runGetAllScopesTest(true)
}

func (suite *IntegrationTestSuite) TestCollectionsInBucketV2() {
	suite.runCollectionsInBucketTest(true)
}

func (suite *IntegrationTestSuite) TestNumberOfCollectionsInScopeV2() {
	suite.runNumberOfCollectionInScopeTest(true)
}

func (suite *IntegrationTestSuite) TestMaxNumberOfCollectionsInScopeV2() {
	suite.runMaxNumberOfCollectionsInScopeTest(true)
}

func (suite *IntegrationTestSuite) TestCollectionHistoryRetentionTestV2() {
	suite.runCollectionHistoryRetentionTest(true)
}

func (suite *IntegrationTestSuite) TestCollectionHistoryRetentionUnsupportedV2() {
	suite.runCollectionHistoryRetentionUnsupportedTest(true)
}

func (suite *IntegrationTestSuite) TestCreateCollectionWithMaxExpiryAsNoExpiryV2() {
	suite.runCreateCollectionWithMaxExpiryAsNoExpiryTest(true)
}

func (suite *IntegrationTestSuite) TestUpdateCollectionWithMaxExpiryAsNoExpiryV2() {
	suite.runUpdateCollectionWithMaxExpiryAsNoExpiryTest(true)
}

func (suite *UnitTestSuite) TestGetAllScopesMgmtRequestFailsV2() {
	suite.runGetAllScopesMgmtRequestFailsTest(true)
}
