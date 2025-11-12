package gocb

func (suite *UnitTestSuite) TestCollectionName() {
	bName := "bucket"
	sName := "scope"
	cName := "collection"

	b := suite.bucket(bName, nil)
	s := b.Scope(sName)
	c := s.Collection(cName)

	suite.Assert().Equal(bName, c.bucketName())
	suite.Assert().Equal(sName, c.ScopeName())
	suite.Assert().Equal(cName, c.Name())
}

func (suite *UnitTestSuite) TestDefaultScopeCollectionName() {
	bName := "bucket"
	cName := "collection"

	b := suite.bucket(bName, nil)
	c := b.Collection(cName)

	suite.Assert().Equal(bName, c.bucketName())
	suite.Assert().Equal("_default", c.ScopeName())
	suite.Assert().Equal(cName, c.Name())
}

func (suite *UnitTestSuite) TestDefaultScopeDefaultCollectionName() {
	bName := "bucket"

	b := suite.bucket(bName, nil)
	c := b.DefaultCollection()

	suite.Assert().Equal(bName, c.bucketName())
	suite.Assert().Equal("_default", c.ScopeName())
	suite.Assert().Equal("_default", c.Name())
}
