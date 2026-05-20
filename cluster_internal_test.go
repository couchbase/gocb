package gocb

func (suite *IntegrationTestSuite) TestInternalClusterGetNodesMetadata() {
	suite.skipIfUnsupported(NodesMetadataFeature)

	ic := globalCluster.Internal()

	nodes, err := ic.GetNodesMetadata(nil)
	suite.Require().NoError(err)

	suite.Assert().GreaterOrEqual(len(nodes), 1)
}
