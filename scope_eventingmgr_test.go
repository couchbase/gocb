package gocb

import (
	"fmt"
	"github.com/google/uuid"
	"time"
)

func (suite *IntegrationTestSuite) TestScopeEventingManagerUpsertGetDrop() {
	suite.runEventingManagerUpsertGetDropTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerUnknownBucket() {
	suite.runEventingManagerUnknownBucketTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerUnknownFunction() {
	suite.runEventingManagerUnknownFunctionTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerInvalidCode() {
	suite.runEventingManagerInvalidCodeTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerCollectionNotFound() {
	suite.runEventingManagerCollectionNotFoundTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerSameSourceAndMetaKeyspace() {
	suite.runEventingManagerSameSourceAndMetaKeyspaceTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerDeploysAndUndeploys() {
	suite.runEventingManagerDeploysAndUndeploysTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerPausesAndResumes() {
	suite.runEventingManagerPausesAndResumesTest(globalScope)
}

func (suite *IntegrationTestSuite) TestScopeEventingManagerGetAllFunctionsFiltering() {
	suite.skipIfUnsupported(EventingFunctionManagerFeature)
	suite.skipIfUnsupported(ScopeEventingFunctionManagerFeature)
	suite.skipIfUnsupported(EventingFunctionManagerMB52649Feature)

	scopeMgr := globalScope.EventingFunctions()
	clusterMgr := globalCluster.Cluster.EventingFunctions()

	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.EnsureScopeOnAllNodes(scopeName)

	suite.mustCreateCollection(scopeName, "source")
	suite.mustCreateCollection(scopeName, "meta")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source", "meta"})

	scopeFnName := uuid.New().String()
	clusterFnName := uuid.New().String()

	expectedScopeFn := EventingFunction{
		Name: scopeFnName,
		Code: `function OnUpdate(doc, meta) {
}`,
		MetadataKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "meta",
		},
		SourceKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "source",
		},
	}
	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		err := scopeMgr.UpsertFunction(expectedScopeFn, nil)
		if err != nil {
			suite.T().Logf("Expected UpsertFunction to succeed: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not succeed in time")

	suite.EnsureEveningFunctionOnAllNodes(time.Now().Add(30*time.Second), scopeFnName, globalScope.BucketName(), globalScope.Name())

	expectedClusterFn := EventingFunction{
		Name: clusterFnName,
		Code: `function OnUpdate(doc, meta) {
}`,
		MetadataKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "meta",
		},
		SourceKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "source",
		},
	}
	success = suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		err := clusterMgr.UpsertFunction(expectedClusterFn, nil)
		if err != nil {
			suite.T().Logf("Expected UpsertFunction to succeed: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not succeed in time")

	suite.EnsureEveningFunctionOnAllNodes(time.Now().Add(30*time.Second), clusterFnName, "", "")

	scopeFunctions, err := scopeMgr.GetAllFunctions(nil)
	suite.Require().Nil(err, err)

	var found bool
	for _, fn := range scopeFunctions {
		// Scope-level GetAllFunctions should not return the cluster-level function
		suite.Assert().NotEqual(clusterFnName, fn.Name)

		if fn.Name == scopeFnName {
			found = true
			suite.Assert().Equal(expectedScopeFn.Code, fn.Code)
		}
	}
	suite.Assert().True(found, fmt.Sprintf("Eventing function %s not found in GetAllFunctions", scopeFnName))

	clusterFunctions, err := clusterMgr.GetAllFunctions(nil)
	suite.Require().Nil(err, err)

	found = false
	for _, fn := range clusterFunctions {
		// Cluster-level GetAllFunctions should not return the scope-level function
		suite.Assert().NotEqual(scopeFnName, fn.Name)

		if fn.Name == clusterFnName {
			found = true
			suite.Assert().Equal(expectedClusterFn.Code, fn.Code)
		}
	}
	suite.Assert().True(found, fmt.Sprintf("Eventing function %s not found in GetAllFunctions", scopeFnName))
}
