package gocb

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (suite *IntegrationTestSuite) TestEventingManagerUpsertGetDrop() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")
	suite.mustCreateCollection(scopeName, "meta")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source", "meta"})

	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
		Code: `function OnUpdate(doc, meta) {
}`,
		BucketBindings: []EventingFunctionBucketBinding{
			{
				Name: EventingFunctionKeyspace{
					Bucket:     globalBucket.Name(),
					Scope:      globalScope.Name(),
					Collection: globalCollection.Name(),
				},
				Alias:  "bucketbinding1",
				Access: EventingFunctionBucketAccessReadWrite,
			},
		},
		UrlBindings: []EventingFunctionUrlBinding{
			{
				Hostname: "http://127.0.0.1",
				Alias:    "urlbinding1",
				Auth: EventingFunctionUrlAuthBasic{
					User: "dave",
					Pass: "password",
				},
				AllowCookies:           false,
				ValidateSSLCertificate: false,
			},
		},
		ConstantBindings: []EventingFunctionConstantBinding{
			{
				Alias:   "someconstant",
				Literal: "someliteral",
			},
		},
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
		err := mgr.UpsertFunction(expectedFn, nil)
		if err != nil {
			suite.T().Logf("Upsert function failed: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not succeed in time")

	suite.EnsureEveningFunctionOnAllNodes(time.Now().Add(30*time.Second), fnName)

	functions, err := mgr.GetAllFunctions(nil)
	suite.Require().Nil(err, err)

	var found bool
	for _, fn := range functions {
		if fn.Name == fnName {
			found = true
			suite.Assert().Equal(expectedFn.Code, fn.Code)
		}
	}
	suite.Assert().True(found, fmt.Sprintf("Eventing function %s not found in GetAllFunctions", fnName))

	funcsStatus, err := mgr.FunctionsStatus(nil)
	suite.Require().Nil(err)

	var foundStatus *EventingFunctionState
	for _, fn := range funcsStatus.Functions {
		if fn.Name == fnName {
			foundStatus = &fn
		}
	}
	suite.Require().NotNil(foundStatus, "Evening function status not found")
	suite.Assert().NotEmpty(foundStatus.Status)

	actualFn, err := mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(expectedFn.Code, actualFn.Code)

	err = mgr.DropFunction(fnName, nil)
	suite.Require().Nil(err)
}

func (suite *IntegrationTestSuite) TestEventingManagerUnknownBucket() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)
	suite.skipIfUnsupported(EventingFunctionManagerMB52572Feature)

	mgr := globalCluster.Cluster.EventingFunctions()
	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
		Code: `feefifofum`,
		MetadataKeyspace: EventingFunctionKeyspace{
			Bucket:     "immadeup",
			Scope:      "idontexist",
			Collection: "meeither",
		},
		SourceKeyspace: EventingFunctionKeyspace{
			Bucket:     "immadeup",
			Scope:      "idontexist2",
			Collection: "meeither2",
		},
	}
	err := mgr.UpsertFunction(expectedFn, nil)
	if !errors.Is(err, ErrBucketNotFound) {
		suite.T().Logf("Expected ResumeFunction to fail with bucket not found but was %v", err)
		suite.T().Fail()
	}
}

func (suite *IntegrationTestSuite) TestEventingManagerUnknownFunction() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")
	suite.mustCreateCollection(scopeName, "meta")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source", "meta"})

	fnName := uuid.New().String()
	fn, err := mgr.GetFunction(fnName, nil)
	suite.Assert().Nil(fn)
	if !errors.Is(err, ErrEventingFunctionNotFound) {
		suite.T().Logf("Expected GetFunction to fail with not found but was %v", err)
		suite.T().Fail()
	}
	err = mgr.DeployFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotFound) {
		suite.T().Logf("Expected DeployFunction to fail with not found but was %v", err)
		suite.T().Fail()
	}
	err = mgr.PauseFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotFound) {
		suite.T().Logf("Expected PauseFunction to fail with not found but was %v", err)
		suite.T().Fail()
	}
	// see MB-47840 on why those are not only ErrEventingFunctionNotFound
	err = mgr.DropFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotDeployed) && !errors.Is(err, ErrEventingFunctionNotFound) {
		suite.T().Logf("Expected DropFunction to fail with not deployed but was %v", err)
		suite.T().Fail()
	}
	err = mgr.UndeployFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotDeployed) && !errors.Is(err, ErrEventingFunctionNotFound) {
		suite.T().Logf("Expected UndeployFunction to fail with not deployed but was %v", err)
		suite.T().Fail()
	}
	err = mgr.ResumeFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotDeployed) && !errors.Is(err, ErrEventingFunctionNotFound) {
		suite.T().Logf("Expected ResumeFunction to fail with not deployed but was %v", err)
		suite.T().Fail()
	}
}

func (suite *IntegrationTestSuite) TestEventingManagerInvalidCode() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")
	suite.mustCreateCollection(scopeName, "meta")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source", "meta"})

	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
		Code: `feefifofum`,
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
		err := mgr.UpsertFunction(expectedFn, nil)
		if !errors.Is(err, ErrEventingFunctionCompilationFailure) {
			suite.T().Logf("Expected ResumeFunction to fail with compilation failure but was %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not fail in the expected way in time")
}

func (suite *IntegrationTestSuite) TestEventingManagerCollectionNotFound() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source"})

	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
		Code: `feefifofum`,
		MetadataKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "idefinitelydontexist",
		},
		SourceKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "source",
		},
	}
	err := mgr.UpsertFunction(expectedFn, nil)
	if !errors.Is(err, ErrCollectionNotFound) {
		suite.T().Logf("Expected ResumeFunction to fail with collection not found but was %v", err)
		suite.T().Fail()
	}
}

func (suite *IntegrationTestSuite) TestEventingManagerSameSourceAndMetaKeyspace() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source"})

	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
		Code: `feefifofum`,
		MetadataKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "source",
		},
		SourceKeyspace: EventingFunctionKeyspace{
			Bucket:     globalBucket.Name(),
			Scope:      scopeName,
			Collection: "source",
		},
	}
	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		err := mgr.UpsertFunction(expectedFn, nil)
		if !errors.Is(err, ErrEventingFunctionIdenticalKeyspace) {
			suite.T().Logf("Expected ResumeFunction to fail with identical keyspace but was %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not fail in the expected way in time")
}

func (suite *IntegrationTestSuite) TestEventingManagerDeploysAndUndeploys() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)
	suite.skipIfUnsupported(EventingFunctionManagerMB52649Feature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")
	suite.mustCreateCollection(scopeName, "meta")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source", "meta"})

	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
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
		err := mgr.UpsertFunction(expectedFn, nil)
		if err != nil {
			suite.T().Logf("Expected UpsertFunction to succeed: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not succeed in time")

	suite.EnsureEveningFunctionOnAllNodes(time.Now().Add(30*time.Second), fnName)

	actualFn, err := mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Require().Equal(EventingFunctionDeploymentStatusUndeployed, actualFn.Settings.DeploymentStatus)

	err = mgr.UndeployFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotDeployed) {
		suite.T().Fatalf("Expected UndeployFunction to fail with not deployed but was %v", err)
	}

	err = mgr.DeployFunction(fnName, nil)
	suite.Require().Nil(err, err)

	actualFn, err = mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Require().Equal(EventingFunctionDeploymentStatusDeployed, actualFn.Settings.DeploymentStatus)

	success = suite.tryUntil(time.Now().Add(60*time.Second), 500*time.Millisecond, func() bool {
		funcsStatus, err := mgr.FunctionsStatus(nil)
		suite.Require().Nil(err)

		for _, fn := range funcsStatus.Functions {
			if fn.Name == fnName {
				if fn.Status != EventingFunctionStateDeployed {
					suite.T().Logf("FunctionsStatus reports function not deployed: %s", fn.Status)
				}
				return fn.Status == EventingFunctionStateDeployed
			}
		}

		suite.T().Fatalf("Function not found from FunctionsStatus")
		return false
	})
	suite.Require().True(success, "FunctionsStatus never reported function deployed")

	err = mgr.UndeployFunction(fnName, nil)
	suite.Require().Nil(err, err)

	actualFn, err = mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(EventingFunctionDeploymentStatusUndeployed, actualFn.Settings.DeploymentStatus)

	success = suite.tryUntil(time.Now().Add(60*time.Second), 500*time.Millisecond, func() bool {
		funcsStatus, err := mgr.FunctionsStatus(nil)
		suite.Require().Nil(err)

		for _, fn := range funcsStatus.Functions {
			if fn.Name == fnName {
				if fn.Status != EventingFunctionStateUndeployed {
					suite.T().Logf("FunctionsStatus reports function not undeployed: %s", fn.Status)
				}
				return fn.Status == EventingFunctionStateUndeployed
			}
		}

		suite.T().Fatalf("Function not found from FunctionsStatus")
		return false
	})
	suite.Require().True(success, "FunctionsStatus never reported function undeployed")

	err = mgr.DropFunction(fnName, nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) TestEventingManagerPausesAndResumes() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(EventingFunctionManagerFeature)
	suite.skipIfUnsupported(EventingFunctionManagerMB52649Feature)

	mgr := globalCluster.Cluster.EventingFunctions()
	scopeName := uuid.NewString()
	suite.mustCreateScope(scopeName)
	defer suite.dropScope(scopeName)
	suite.mustCreateCollection(scopeName, "source")
	suite.mustCreateCollection(scopeName, "meta")

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{"source", "meta"})

	fnName := uuid.New().String()
	expectedFn := EventingFunction{
		Name: fnName,
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
		err := mgr.UpsertFunction(expectedFn, nil)
		if err != nil {
			suite.T().Logf("Expected UpsertFunction to succeed: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Upsert function did not succeed in time")

	suite.EnsureEveningFunctionOnAllNodes(time.Now().Add(30*time.Second), fnName)

	actualFn, err := mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Require().Equal(EventingFunctionProcessingStatusPaused, actualFn.Settings.ProcessingStatus)

	err = mgr.PauseFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotBootstrapped) {
		suite.T().Fatalf("Expected UndeployFunction to fail with not bootstrapped but was %v", err)
	}

	err = mgr.ResumeFunction(fnName, nil)
	if !errors.Is(err, ErrEventingFunctionNotDeployed) {
		suite.T().Fatalf("Expected UndeployFunction to fail with not deployed but was %v", err)
	}

	err = mgr.DeployFunction(fnName, nil)
	suite.Require().Nil(err, err)

	actualFn, err = mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Require().Equal(EventingFunctionProcessingStatusRunning, actualFn.Settings.ProcessingStatus)

	success = suite.tryUntil(time.Now().Add(60*time.Second), 500*time.Millisecond, func() bool {
		funcsStatus, err := mgr.FunctionsStatus(nil)
		suite.Require().Nil(err)

		for _, fn := range funcsStatus.Functions {
			if fn.Name == fnName {
				if fn.Status != EventingFunctionStateDeployed {
					suite.T().Logf("FunctionsStatus reports function not deployed: %s", fn.Status)
				}
				return fn.Status == EventingFunctionStateDeployed
			}
		}

		suite.T().Fatalf("Function not found from FunctionsStatus")
		return false
	})
	suite.Require().True(success, "FunctionsStatus never reported function deployed")

	err = mgr.PauseFunction(fnName, nil)
	suite.Require().Nil(err, err)

	actualFn, err = mgr.GetFunction(fnName, nil)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(EventingFunctionProcessingStatusPaused, actualFn.Settings.ProcessingStatus)

	success = suite.tryUntil(time.Now().Add(60*time.Second), 500*time.Millisecond, func() bool {
		funcsStatus, err := mgr.FunctionsStatus(nil)
		suite.Require().Nil(err)

		for _, fn := range funcsStatus.Functions {
			if fn.Name == fnName {
				if fn.Status != EventingFunctionStatePaused {
					suite.T().Logf("FunctionsStatus reports function not paused: %s", fn.Status)
				}
				return fn.Status == EventingFunctionStatePaused
			}
		}

		suite.T().Fatalf("Function not found from FunctionsStatus")
		return false
	})
	suite.Require().True(success, "FunctionsStatus never reported function paused")

	err = mgr.UndeployFunction(fnName, nil)
	suite.Require().Nil(err, err)

	success = suite.tryUntil(time.Now().Add(30*time.Second), 500*time.Millisecond, func() bool {
		funcsStatus, err := mgr.FunctionsStatus(nil)
		suite.Require().Nil(err)

		for _, fn := range funcsStatus.Functions {
			if fn.Name == fnName {
				if fn.Status != EventingFunctionStateUndeployed {
					suite.T().Logf("FunctionsStatus reports function not undeployed: %s", fn.Status)
				}
				return fn.Status == EventingFunctionStateUndeployed
			}
		}

		suite.T().Fatalf("Function not found from FunctionsStatus")
		return false
	})
	suite.Require().True(success, "FunctionsStatus never reported function undeployed")

	err = mgr.DropFunction(fnName, nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) mustCreateScope(scope string) {
	cmgr := globalBucket.Collections()
	err := cmgr.CreateScope(scope, nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) dropScope(scope string) {
	cmgr := globalBucket.Collections()
	err := cmgr.DropScope(scope, nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) mustCreateCollection(scope, collection string) {
	cmgr := globalBucket.Collections()
	err := cmgr.CreateCollection(CollectionSpec{
		Name:      collection,
		ScopeName: scope,
	}, nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) dropCollection(scope, collection string) {
	cmgr := globalBucket.Collections()
	err := cmgr.DropCollection(CollectionSpec{
		Name:      collection,
		ScopeName: scope,
	}, nil)
	suite.Require().Nil(err, err)
}
