package gocb

import (
	"context"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestGetReplica() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(GetReplicaFeature)

	doc := map[string]string{"test": "value"}

	docId := generateDocId("getReplicaDoc")
	suite.mustUpsertToAllReplicas(docId, doc)

	strategy, err := NewGetReplicaStrategyFromIndex(ReplicaIndexFirst, nil)
	suite.Require().NoError(err)

	res, err := globalCollection.GetReplica(docId, strategy, nil)
	suite.Require().NoError(err)
	suite.Require().True(res.IsReplica())

	var resContent map[string]string
	err = res.Content(&resContent)
	suite.Require().NoError(err)
	suite.Assert().Equal(doc, resContent)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(2, len(nilParents))
	suite.AssertKvSpan(nilParents[1], "get_replica", DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "get_replica", 1, false)
}

func (suite *IntegrationTestSuite) TestGetReplicaDocNotFound() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(GetReplicaFeature)

	strategy, err := NewGetReplicaStrategyFromIndex(ReplicaIndexFirst, nil)
	suite.Require().NoError(err)

	res, err := globalCollection.GetReplica("getReplicaMissingDoc", strategy, nil)
	suite.Assert().ErrorIs(err, ErrDocumentNotFound)
	suite.Assert().ErrorIs(err, ErrDocumentNotFoundOnReplica)
	suite.Assert().Nil(res)
}

func (suite *IntegrationTestSuite) TestGetReplicaIndexOutOfBounds() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(GetReplicaFeature)

	idx, ok := suite.outOfBoundsReplicaIndex()
	if !ok {
		suite.T().Skip("Cluster has the maximum number of replicas, cannot build an out of bounds replica index")
	}

	doc := map[string]string{"test": "value"}

	docId := generateDocId("getReplicaOutOfBoundsDoc")
	suite.mustUpsertToAllReplicas(docId, doc)

	strategy, err := NewGetReplicaStrategyFromIndex(idx, &GetReplicaStrategyFromIndexOptions{Wrap: false})
	suite.Require().NoError(err)

	res, err := globalCollection.GetReplica(docId, strategy, nil)
	suite.Require().ErrorIs(err, ErrReplicaIndexOutOfBounds)
	suite.Require().Nil(res)
}

func (suite *IntegrationTestSuite) TestGetReplicaIndexOutOfBoundsWithWrap() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(GetReplicaFeature)

	idx, ok := suite.outOfBoundsReplicaIndex()
	if !ok {
		suite.T().Skip("Cluster has the maximum number of replicas, cannot build an out of bounds replica index")
	}

	doc := map[string]string{"test": "value"}

	docId := generateDocId("getReplicaWrapDoc")
	suite.mustUpsertToAllReplicas(docId, doc)

	// With wrap enabled the index is wrapped around the replicas that are available, so a read using an index that
	// would otherwise be out of bounds succeeds.
	strategy, err := NewGetReplicaStrategyFromIndex(idx, &GetReplicaStrategyFromIndexOptions{Wrap: true})
	suite.Require().NoError(err)

	res, err := globalCollection.GetReplica(docId, strategy, nil)
	suite.Require().NoError(err)
	suite.Require().True(res.IsReplica())

	var resContent map[string]string
	err = res.Content(&resContent)
	suite.Require().NoError(err)
	suite.Assert().Equal(doc, resContent)
}

func (suite *IntegrationTestSuite) mustUpsertToAllReplicas(docId string, doc interface{}) {
	mutRes, err := globalCollection.Upsert(docId, doc, &UpsertOptions{
		PersistTo: uint(suite.numReplicas() + 1),
		Timeout:   10 * time.Second,
	})
	suite.Require().NoError(err)
	suite.Require().NotZero(mutRes.Cas())
}

func (suite *IntegrationTestSuite) numReplicas() int {
	prov, err := globalCollection.getKvProvider()
	suite.Require().NoError(err)

	agent, ok := prov.(*kvProviderCore)
	suite.Require().True(ok, "Expected kv provider to be a core provider")

	snapshot, err := agent.snapshotProvider.WaitForConfigSnapshot(context.Background(), time.Now().Add(5*time.Second))
	suite.Require().NoError(err)

	numReplicas, err := snapshot.NumReplicas()
	suite.Require().NoError(err)

	return numReplicas
}

// outOfBoundsReplicaIndex returns the lowest replica index that is out of bounds for this cluster, if one can be
// expressed using the ReplicaIndex values that the API allows.
func (suite *IntegrationTestSuite) outOfBoundsReplicaIndex() (ReplicaIndex, bool) {
	numReplicas := suite.numReplicas()
	if numReplicas >= int(ReplicaIndexThird) {
		return 0, false
	}

	return ReplicaIndex(numReplicas + 1), true
}

func (suite *UnitTestSuite) TestGetReplicaEmptyStrategy() {
	provider := new(mockKvProviderCoreProvider)

	col := suite.collection("mock", "", "", suite.kvProviderCore(provider, nil))

	res, err := col.GetReplica("someid", GetReplicaStrategy{}, nil)
	suite.Require().ErrorIs(err, ErrInvalidArgument)
	suite.Require().Nil(res)

	provider.AssertNotCalled(suite.T(), "GetOneReplica", mock.Anything, mock.Anything)
}

func (suite *UnitTestSuite) TestGetReplicaFromIndex() {
	type testCase struct {
		name string
		idx  ReplicaIndex
		wrap bool
	}

	testCases := []testCase{
		{name: "NoWrap", idx: ReplicaIndexSecond, wrap: false},
		{name: "Wrap", idx: ReplicaIndexSecond, wrap: true},
	}

	for _, tCase := range testCases {
		suite.Run(tCase.name, func() {
			pendingOp := new(mockPendingOp)
			pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

			provider := new(mockKvProviderCoreProvider)
			provider.
				On("GetOneReplica", mock.AnythingOfType("gocbcore.GetOneReplicaOptions"), mock.AnythingOfType("gocbcore.GetReplicaCallback")).
				Run(func(args mock.Arguments) {
					opts := args.Get(0).(gocbcore.GetOneReplicaOptions)
					cb := args.Get(1).(gocbcore.GetReplicaCallback)

					suite.Assert().Zero(opts.ReplicaIdx)
					suite.Assert().Equal(gocbcore.IndexReplicaSelector{ReplicaIdx: int(tCase.idx), Wrap: tCase.wrap}, opts.ReplicaSelector)

					cb(&gocbcore.GetReplicaResult{
						Value: []byte(`{"test":"value"}`),
						Flags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
						Cas:   gocbcore.Cas(123),
					}, nil)
				}).
				Return(pendingOp, nil)

			col := suite.collection("mock", "", "", suite.kvProviderCore(provider, nil))

			strategy, err := NewGetReplicaStrategyFromIndex(tCase.idx, &GetReplicaStrategyFromIndexOptions{Wrap: tCase.wrap})
			suite.Require().NoError(err)

			res, err := col.GetReplica("someid", strategy, nil)
			suite.Require().NoError(err)

			suite.Assert().Equal(Cas(123), res.Cas())
			suite.Assert().True(res.IsReplica())

			var content map[string]string
			err = res.Content(&content)
			suite.Require().NoError(err)
			suite.Assert().Equal(map[string]string{"test": "value"}, content)
		})
	}
}

func (suite *UnitTestSuite) TestGetReplicaErrors() {
	type testCase struct {
		name        string
		coreErr     error
		expectedErr error
	}

	testCases := []testCase{
		{
			name:        "DocNotFound",
			coreErr:     gocbcore.ErrDocumentNotFound,
			expectedErr: ErrDocumentNotFoundOnReplica,
		},
		{
			name:        "IndexOutOfBounds",
			coreErr:     gocbcore.ErrInvalidReplica,
			expectedErr: ErrReplicaIndexOutOfBounds,
		},
		{
			name:        "IndexCurrentlyUnavailable",
			coreErr:     gocbcore.ErrReplicaCurrentlyUnavailable,
			expectedErr: ErrReplicaIndexCurrentlyUnavailable,
		},
	}

	for _, tCase := range testCases {
		suite.Run(tCase.name, func() {
			pendingOp := new(mockPendingOp)
			pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

			provider := new(mockKvProviderCoreProvider)
			provider.
				On("GetOneReplica", mock.AnythingOfType("gocbcore.GetOneReplicaOptions"), mock.AnythingOfType("gocbcore.GetReplicaCallback")).
				Run(func(args mock.Arguments) {
					cb := args.Get(1).(gocbcore.GetReplicaCallback)
					cb(nil, &gocbcore.KeyValueError{
						InnerError: tCase.coreErr,
						// The other fields of gocbcore.KeyValueError shouldn't matter here.
					})
				}).
				Return(pendingOp, nil)

			col := suite.collection("mock", "", "", suite.kvProviderCore(provider, nil))

			strategy, err := NewGetReplicaStrategyFromIndex(ReplicaIndexFirst, nil)
			suite.Require().NoError(err)

			res, err := col.GetReplica("someid", strategy, nil)
			suite.Require().ErrorIs(err, tCase.expectedErr)
			suite.Require().Nil(res)
		})
	}
}
