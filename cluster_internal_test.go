package gocb

func (suite *IntegrationTestSuite) TestInternalClusterGetNodesMetadata() {
	ic := globalCluster.Internal()

	nodes, err := ic.GetNodesMetadata(nil)
	suite.Require().Nil(err, err)

	suite.Assert().GreaterOrEqual(len(nodes), 1)
}
