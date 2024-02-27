package gocb

import "github.com/couchbase/gocbcore/v10"

func (suite *UnitTestSuite) TestCollectionInternal_BucketCapabilityStatus_Supported() {
	provider := new(mockKvCapabilityVerifier)
	provider.On(
		"BucketCapabilityStatus",
		gocbcore.BucketCapabilityDurableWrites,
	).Return(gocbcore.CapabilityStatusSupported)

	cli := new(mockConnectionManager)
	cli.On("getKvCapabilitiesProvider", "mock").Return(provider, nil)

	b := suite.bucket("mock", suite.defaultTimeoutConfig(), cli)

	ib := b.Internal()
	status, err := ib.CapabilityStatus(CapabilityDurableWrites)
	suite.Require().Nil(err)
	suite.Assert().Equal(CapabilityStatusSupported, status)
}

func (suite *UnitTestSuite) TestCollectionInternal_BucketCapabilityStatus_Unsupported() {
	provider := new(mockKvCapabilityVerifier)
	provider.On(
		"BucketCapabilityStatus",
		gocbcore.BucketCapabilityDurableWrites,
	).Return(gocbcore.CapabilityStatusUnsupported)

	cli := new(mockConnectionManager)
	cli.On("getKvCapabilitiesProvider", "mock").Return(provider, nil)

	b := suite.bucket("mock", suite.defaultTimeoutConfig(), cli)

	ib := b.Internal()
	status, err := ib.CapabilityStatus(CapabilityDurableWrites)
	suite.Require().Nil(err)
	suite.Assert().Equal(CapabilityStatusUnsupported, status)
}
