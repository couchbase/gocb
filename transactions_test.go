package gocb

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/google/uuid"
)

func (suite *IntegrationTestSuite) verifyDocument(key string, val interface{}) {
	res, err := globalCollection.Get(key, nil)
	suite.Require().Nil(err, err)

	var actualVal interface{}
	err = res.Content(&actualVal)
	suite.Require().Nil(err, err)

	suite.Assert().Equal(actualVal, val)
}

func (suite *IntegrationTestSuite) verifyDocumentNotFound(key string) {
	_, err := globalCollection.Get(key, nil)
	suite.Require().ErrorIs(err, ErrDocumentNotFound)
}

func (suite *IntegrationTestSuite) TestTransactionsDoubleInsert() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txninsert"
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		_, err = ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Assert().Nil(txnRes)
	suite.Assert().ErrorIs(err, ErrDocumentExists)

	var txnErr *TransactionFailedError
	if suite.Assert().ErrorAs(err, &txnErr) {
		suite.Assert().NotNil(txnErr.Result())
	}
}

func (suite *IntegrationTestSuite) TestTransactionsInsert() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := generateDocId("txninsert")
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue)
}

func (suite *IntegrationTestSuite) TestTransactionsCustomMetadata() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfServerVersionEquals(srvVer750)

	metaCollectionName := "txnsCustomMetadata"
	collections := globalBucket.Collections()
	err := collections.CreateCollection(CollectionSpec{
		Name:      metaCollectionName,
		ScopeName: globalScope.Name(),
	}, nil)
	suite.Require().Nil(err, err)
	defer collections.DropCollection(CollectionSpec{
		Name:      metaCollectionName,
		ScopeName: globalScope.Name(),
	}, nil)
	suite.mustWaitForCollections(globalScope.Name(), []string{metaCollectionName})

	tConfig := globalCluster.transactionsConfig
	tConfig.MetadataCollection = &TransactionKeyspace{
		BucketName:     globalBucket.Name(),
		ScopeName:      globalScope.Name(),
		CollectionName: metaCollectionName,
	}

	c, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator: PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		},
		TransactionsConfig: tConfig,
	})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	docID := "txnsCustomMetadata"
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := c.Transactions()

	var atr string
	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		atr = string(ctx.txn.Attempt().AtrID)

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	res, err := globalScope.Collection(metaCollectionName).LookupIn(atr,
		[]LookupInSpec{
			GetSpec("", nil),
		}, &LookupInOptions{
			Internal: struct {
				DocFlags SubdocDocFlag
				User     string
			}{DocFlags: SubdocDocFlagAccessDeleted},
		})
	suite.Require().Nil(err, err)

	suite.Assert().True(res.Exists(0))

	suite.verifyDocument(docID, docValue)
}

func (suite *IntegrationTestSuite) TestTransactionsCustomMetadataTransactionOption() {
	suite.skipIfUnsupported(TransactionsFeature)

	metaCollectionName := generateDocId("txnsCustomMetadataTxnOption")
	collections := globalBucket.Collections()
	err := collections.CreateCollection(CollectionSpec{
		Name:      metaCollectionName,
		ScopeName: globalScope.Name(),
	}, nil)
	suite.Require().Nil(err, err)
	defer collections.DropCollection(CollectionSpec{
		Name:      metaCollectionName,
		ScopeName: globalScope.Name(),
	}, nil)
	suite.mustWaitForCollections(globalScope.Name(), []string{metaCollectionName})

	perConfig := &TransactionOptions{
		MetadataCollection: globalBucket.Collection(metaCollectionName),
	}

	docID := generateDocId("txnsCustomMetadataTxnOption")
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Transactions()

	var atr string
	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		atr = string(ctx.txn.Attempt().AtrID)

		return nil
	}, perConfig)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	res, err := globalScope.Collection(metaCollectionName).LookupIn(atr,
		[]LookupInSpec{
			GetSpec("", nil),
		}, &LookupInOptions{
			Internal: struct {
				DocFlags SubdocDocFlag
				User     string
			}{DocFlags: SubdocDocFlagAccessDeleted},
		})
	suite.Require().Nil(err, err)

	suite.Assert().True(res.Exists(0))

	suite.verifyDocument(docID, docValue)
}

func (suite *IntegrationTestSuite) TestTransactionsCustomMetadataLocationRemoved() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsRemoveLocationFeature)

	metaCollectionName := uuid.NewString()
	collections := globalBucket.Collections()
	err := collections.CreateCollection(CollectionSpec{
		Name:      metaCollectionName,
		ScopeName: globalScope.Name(),
	}, nil)
	suite.Require().Nil(err, err)
	suite.mustWaitForCollections(globalScope.Name(), []string{metaCollectionName})

	perConfig := &TransactionOptions{
		MetadataCollection: globalBucket.Collection(metaCollectionName),
	}

	docID := uuid.NewString()
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		return nil
	}, perConfig)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	location := gocbcore.TransactionLostATRLocation{
		BucketName:     globalBucket.Name(),
		ScopeName:      globalScope.Name(),
		CollectionName: metaCollectionName,
	}

	suite.Require().Contains(txns.Internal().CleanupLocations(), location)

	err = collections.DropCollection(CollectionSpec{
		Name:      metaCollectionName,
		ScopeName: globalScope.Name(),
	}, nil)
	suite.Require().Nil(err, err)

	suite.Eventually(func() bool {
		locations := txns.Internal().CleanupLocations()
		for _, loc := range locations {
			if loc == location {
				return false
			}
		}
		return true
	}, globalCluster.txnCleanupTimeout(), 100*time.Millisecond)
}

func (suite *IntegrationTestSuite) TestTransactionsRollback() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txnreplace"
	docValue := map[string]interface{}{
		"test": "test",
	}
	docValue2 := map[string]interface{}{
		"test": "test2",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Insert(globalCollection, docID, docValue2)
		if err != nil {
			return err
		}

		getRes2, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue2 map[string]interface{}
		err = getRes2.Content(&actualDocValue2)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue2, actualDocValue2)

		return errors.New("bail me out")
	}, nil)
	suite.Require().NotNil(err)
	suite.Require().Nil(txnRes)

	suite.verifyDocument(docID, docValue)
}

func (suite *IntegrationTestSuite) TestTransactionsReadExternalToTxn() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txnreplace"
	docValue := map[string]interface{}{
		"test": "test",
	}
	docValue2 := map[string]interface{}{
		"test": "test2",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	interceptCh := make(chan struct{})
	go func() {
		txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
			getRes, err := ctx.Get(globalCollection, docID)
			if err != nil {
				return err
			}

			var actualDocValue map[string]interface{}
			err = getRes.Content(&actualDocValue)
			if err != nil {
				return err
			}

			suite.Assert().Equal(docValue, actualDocValue)

			_, err = ctx.Replace(getRes, docValue2)
			if err != nil {
				return err
			}

			interceptCh <- struct{}{}
			<-interceptCh

			getRes2, err := ctx.Get(globalCollection, docID)
			if err != nil {
				return err
			}

			var actualDocValue2 map[string]interface{}
			err = getRes2.Content(&actualDocValue2)
			if err != nil {
				return err
			}

			suite.Assert().Equal(docValue2, actualDocValue2)

			return nil
		}, nil)
		suite.Assert().Nil(err, err)
		suite.Assert().NotNil(txnRes)

		interceptCh <- struct{}{}
	}()

	<-interceptCh
	suite.verifyDocument(docID, docValue)
	interceptCh <- struct{}{}
	<-interceptCh
	suite.verifyDocument(docID, docValue2)
}

func (suite *IntegrationTestSuite) TestTransactionsReplace() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txnreplace"
	docValue := map[string]interface{}{
		"test": "test",
	}
	docValue2 := map[string]interface{}{
		"test": "test2",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		_, err = ctx.Replace(getRes, docValue2)
		if err != nil {
			return err
		}

		getRes2, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue2 map[string]interface{}
		err = getRes2.Content(&actualDocValue2)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue2, actualDocValue2)

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue2)
}

func (suite *IntegrationTestSuite) TestTransactionsRemove() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txnremove"
	docValue := map[string]interface{}{
		"test": "test",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		err = ctx.Remove(getRes)
		if err != nil {
			return err
		}

		_, err = ctx.Get(globalCollection, docID)
		if !errors.Is(err, ErrDocumentNotFound) {
			return errors.New("get should have returned a doc not found")
		}

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)
}

func (suite *IntegrationTestSuite) TestTransactionsInsertReplace() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := generateDocId("txninsertreplace")
	docValue := map[string]interface{}{
		"test": "test",
	}
	docValue2 := map[string]interface{}{
		"test": "test2",
	}

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		res, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		_, err = ctx.Replace(res, docValue2)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue2, actualDocValue)

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue2)
}

func (suite *IntegrationTestSuite) TestTransactionsInsertRemove() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txninsertremove"
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		res, err := ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		err = ctx.Remove(res)
		if err != nil {
			return err
		}

		_, err = ctx.Get(globalCollection, docID)
		if !errors.Is(err, ErrDocumentNotFound) {
			return errors.New(fmt.Sprintf("error should have been doc not found, was %#v", err))
		}

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocumentNotFound(docID)
}

func (suite *IntegrationTestSuite) TestTransactionsUserError() {
	suite.skipIfUnsupported(TransactionsFeature)

	var ErrOopsieDoodle = errors.New("im an error")

	txns := globalCluster.Cluster.Transactions()

	_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		return ErrOopsieDoodle
	}, nil)
	suite.Require().ErrorIs(err, ErrOopsieDoodle)
}

func (suite *IntegrationTestSuite) TestTransactionsGetDocNotFoundAllowsContinue() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := generateDocId("txndocnotfoundallowscontinue")
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		getRes, err := ctx.Get(globalCollection, docID)
		if !errors.Is(err, ErrDocumentNotFound) {
			return fmt.Errorf("get should have returned document not found but was %v", err)
		}

		_, err = ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		getRes, err = ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = getRes.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		return nil
	}, nil)
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue)
}

func (suite *IntegrationTestSuite) TestTransactionsGetOnly() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "getOnly"
	docValue := map[string]interface{}{
		"test": "test",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	_, err = txns.Run(func(ctx *TransactionAttemptContext) error {
		res, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = res.Content(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		return nil
	}, nil)
	suite.Require().Nil(err, err)
}

func (suite *UnitTestSuite) TestMultipleTransactionObjects() {
	cli := new(mockConnectionManager)
	cli.On("close").Return(nil)

	tConfig := TransactionsConfig{}
	tConfig.CleanupConfig.DisableLostAttemptCleanup = true

	c := clusterFromOptions(ClusterOptions{
		Tracer:             &NoopTracer{},
		Meter:              &NoopMeter{},
		TransactionsConfig: tConfig,
	})
	defer c.Close(nil)
	c.connectionManager = cli

	txns := c.Transactions()

	txns2 := c.Transactions()

	suite.Assert().Equal(&txns, &txns2)
}

func (suite *UnitTestSuite) TestTransactionsCustomMetadataAddedToCleanupLocs() {
	metaCollectionName := "TestTransactionsCustomMetadataAddedToCleanupLocs"

	cli := new(mockConnectionManager)
	cli.On("openBucket", "default").Return(nil)
	cli.On("openBucket", "connect").Return(nil)
	cli.On("connection", "default").Return(&gocbcore.Agent{}, nil)
	cli.On("close").Return(nil)

	tConfig := TransactionsConfig{}
	tConfig.MetadataCollection = &TransactionKeyspace{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: metaCollectionName,
	}
	tConfig.CleanupConfig.DisableLostAttemptCleanup = true
	tConfig.CleanupConfig.DisableClientAttemptCleanup = true
	c := clusterFromOptions(ClusterOptions{
		Tracer:             &NoopTracer{},
		Meter:              &NoopMeter{},
		TransactionsConfig: tConfig,
	})
	defer c.Close(nil)
	c.connectionManager = cli

	txns, err := c.initTransactions(tConfig)
	suite.Require().Nil(err, err)

	locs, err := txns.atrLocationsProvider()
	suite.Require().Nil(err)

	suite.Require().Len(locs, 1)
	suite.Require().Contains(locs, gocbcore.TransactionLostATRLocation{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: metaCollectionName,
	})
}

func (suite *UnitTestSuite) TestTransactionsCustomMetadataAlreadyInCleanupCollections() {
	metaCollectionName := "TestTransactionsCustomMetadataAlreadyInCleanupCollections"

	cli := new(mockConnectionManager)
	cli.On("openBucket", "default").Return(nil)
	cli.On("connection", "default").Return(&gocbcore.Agent{}, nil)
	cli.On("close").Return(nil)

	tConfig := TransactionsConfig{}
	tConfig.MetadataCollection = &TransactionKeyspace{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: metaCollectionName,
	}
	tConfig.CleanupConfig.CleanupCollections = []TransactionKeyspace{
		{
			BucketName:     "default",
			ScopeName:      "_default",
			CollectionName: metaCollectionName,
		},
	}
	tConfig.CleanupConfig.DisableLostAttemptCleanup = true
	tConfig.CleanupConfig.DisableClientAttemptCleanup = true
	c := clusterFromOptions(ClusterOptions{
		Tracer:             &NoopTracer{},
		Meter:              &NoopMeter{},
		TransactionsConfig: tConfig,
	})
	defer c.Close(nil)
	c.connectionManager = cli

	txns, err := c.initTransactions(tConfig)
	suite.Require().Nil(err, err)

	locs, err := txns.atrLocationsProvider()
	suite.Require().Nil(err)

	suite.Require().Len(locs, 1)
	suite.Require().Contains(locs, gocbcore.TransactionLostATRLocation{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: metaCollectionName,
	})
}

func (suite *IntegrationTestSuite) TestTransactionsNoContentionSingleThreadPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsNoContentionMedTxnSingleThreadPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k000tok099", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsNoContentionBigTxnSingleThreadPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k000tok499", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowLateContentionTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19andkC", true, 50},
		{"k20tok39andkC", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowLateContentionTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19andkC", false, 50},
		{"k20tok39andkC", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowEarlyContentionTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"kCandk00tok19", true, 50},
		{"kCandk20tok39", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowEarlyContentionTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"kCandk00tok19", false, 50},
		{"kCandk20tok39", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsHighContentionThreeThreadsTwoPessimisticOneOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsHighContentionThreeThreadsTwoOptimisticOnePessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", false, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsHighContentionTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsHighContentionTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsVHighContentionTenThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
		{"k00tok19", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsVHighContentionTenThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
		{"k00tok19", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowLateContentionMedTxnTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k000tok099andkC", true, 50},
		{"k100tok199andkC", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowLateContentionMedTxnTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k000tok099andkC", false, 50},
		{"k100tok199andkC", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowEarlyContentionMedTxnTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"kCandk000tok099", true, 50},
		{"kCandk100tok199", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsLowEarlyContentionMedTxnTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"kCandk000tok099", false, 50},
		{"kCandk100tok199", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsOneSidedEarlyContentionMedTxnTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"kCON", true, 50 * 101},
		{"kCandk100tok199", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsOneSidedEarlyContentionMedTxnTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"kCON", false, 50 * 101},
		{"kCandk100tok199", false, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsHighContentionBigTxnTwoThreadsOptimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok199", true, 50},
		{"k00tok199", true, 50},
	})
}

func (suite *IntegrationTestSuite) TestTransactionsHighContentionBigTxnTwoThreadsPessimistic() {
	suite.skipIfUnsupported(TransactionsBulkFeature)
	suite.runTranasctionLoadTest([]transactionTestGroup{
		{"k00tok199", false, 50},
		{"k00tok199", false, 50},
	})
}

type transactionTestGroup struct {
	name     string
	isOptim  bool
	numIters int
}

type tranactionTestResult struct {
	NumSuccess int
	NumError   int
	NumIters   int
	Keys       []string
	MinTime    time.Duration
	MaxTime    time.Duration
	AvgTime    time.Duration
	SumTime    time.Duration
}

func (suite *IntegrationTestSuite) doTransactionOps(name string, keys []string, numIters int, useOptim bool) *tranactionTestResult {

	transactions := globalCluster.Transactions()

	// log.Printf("  %s testing (%+v)", name, keys)

	var minTime time.Duration
	var maxTime time.Duration
	var sumTime time.Duration

	numSuccess := 0
	numError := 0
	numToRun := numIters

	for runIdx := 0; runIdx < numToRun; runIdx++ {
		numIters = runIdx + 1

		tstime := time.Now()

		_, err := transactions.Run(func(ctx *TransactionAttemptContext) error {
			if useOptim {
				resObjs := make([]*TransactionGetResult, len(keys))
				valDatas := make([]map[string]int, len(keys))

				for kIdx, k := range keys {
					resObj, err := ctx.Get(globalCollection, k)
					if err != nil {
						return err
					}

					resObjs[kIdx] = resObj
				}

				for kIdx := range keys {
					var valData map[string]int
					err := resObjs[kIdx].Content(&valData)
					if err != nil {
						return err
					}

					valData["i"]++

					valDatas[kIdx] = valData
				}

				for kIdx := range keys {
					_, err := ctx.Replace(resObjs[kIdx], valDatas[kIdx])
					if err != nil {
						return err
					}
				}
			} else {
				for _, k := range keys {
					resObj, err := ctx.Get(globalCollection, k)
					if err != nil {
						return err
					}

					var valData map[string]int
					err = resObj.Content(&valData)
					if err != nil {
						return err
					}

					valData["i"]++

					_, err = ctx.Replace(resObj, valData)
					if err != nil {
						return err
					}

				}
			}

			return nil
		}, nil)
		if err != nil {
			log.Printf("run failed: %s", err)
			numError++
		} else {
			numSuccess++

			tetime := time.Now()
			tdtime := tetime.Sub(tstime)

			if minTime == 0 || tdtime < minTime {
				minTime = tdtime
			}
			if maxTime == 0 || tdtime > maxTime {
				maxTime = tdtime
			}
			sumTime += tdtime
		}
	}

	var avgTime time.Duration
	if suite.Assert().GreaterOrEqual(numSuccess, 1) {
		avgTime = sumTime / time.Duration(numSuccess)
	}

	log.Printf("  %s testing took %s, (%d iters, %d keys, %.2f success rate, min:%dms max:%dms avg:%dms)",
		name,
		sumTime.String(),
		numIters,
		len(keys),
		float64(numSuccess)/float64(numIters)*100,
		minTime/time.Millisecond, maxTime/time.Millisecond, avgTime/time.Millisecond)

	return &tranactionTestResult{
		NumSuccess: numSuccess,
		NumError:   numError,
		NumIters:   numIters,
		Keys:       keys,
		MinTime:    minTime,
		MaxTime:    maxTime,
		SumTime:    sumTime,
		AvgTime:    avgTime,
	}
}

func (suite *IntegrationTestSuite) transactionPrepDocs(allKeys []string) {
	testDummy := map[string]int{"i": 1}

	// Flush and wait for it to finish...
	_, err := globalCollection.Upsert("flush-watch", nil, nil)
	suite.Require().Nil(err, err)
	err = globalCluster.Buckets().FlushBucket("default", nil)
	suite.Require().Nil(err, err)

	suite.tryTimes(512, 100*time.Millisecond, func() bool {
		_, err := globalCollection.Get("flush-watch", nil)
		return errors.Is(err, ErrDocumentNotFound)
	})

	for _, k := range allKeys {
		_, err := globalCollection.Insert(k, testDummy, nil)
		suite.Require().Nil(err, err)
	}
}

func (suite *IntegrationTestSuite) runTranasctionLoadTest(grps []transactionTestGroup) {
	allKeysMap := make(map[string]int)
	for _, grp := range grps {
		for _, key := range transactionsTestKeys[grp.name] {
			allKeysMap[key]++
		}
	}

	allKeys := make([]string, 0, len(allKeysMap))
	for key := range allKeysMap {
		allKeys = append(allKeys, key)
	}

	suite.transactionPrepDocs(allKeys)

	resultCh := make(chan *tranactionTestResult, 100)

	for grpIdx, grp := range grps {
		var gname string
		if grp.isOptim {
			gname = fmt.Sprintf("%d-%s-opti", grpIdx+1, grp.name)
		} else {
			gname = fmt.Sprintf("%d-%s-pess", grpIdx+1, grp.name)
		}

		go func(grp transactionTestGroup) {
			res := suite.doTransactionOps(gname, transactionsTestKeys[grp.name], grp.numIters, grp.isOptim)
			resultCh <- res
		}(grp)
	}

	var ttlSuccess int
	var ttlError int
	var ttlIters int
	var ttlWrites int
	var minTime time.Duration
	var maxTime time.Duration
	var sumTime time.Duration
	var ttlTime time.Duration

	groupVals := make(map[string]int)
	for range grps {
		tRes := <-resultCh

		if minTime == 0 || tRes.MinTime < minTime {
			minTime = tRes.MinTime
		}

		if maxTime == 0 || tRes.MaxTime > maxTime {
			maxTime = tRes.MaxTime
		}

		ttlWrites += tRes.NumSuccess * len(tRes.Keys)
		ttlIters += tRes.NumIters
		ttlSuccess += tRes.NumSuccess
		ttlError += tRes.NumError
		sumTime += tRes.SumTime

		if ttlTime == 0 || tRes.SumTime > ttlTime {
			ttlTime = tRes.SumTime
		}

		for _, key := range tRes.Keys {
			groupVals[key] += tRes.NumSuccess
		}
	}

	avgTime := sumTime / time.Duration(ttlSuccess)
	wps := float64(ttlWrites) / (float64(sumTime) / float64(time.Second))

	log.Printf("  overall took %s, %.2f success rate, min:%dms max:%dms avg:%dms, %.2f wps",
		ttlTime.String(),
		float64(ttlSuccess)/float64(ttlIters)*100,
		minTime/time.Millisecond, maxTime/time.Millisecond, avgTime/time.Millisecond,
		wps)

	suite.Assert().Zero(ttlError)

	// VALIDATE
	for key, val := range groupVals {
		doc, err := globalCollection.Get(key, nil)
		if err != nil {
			panic(err)
		}

		var docContent map[string]int
		if suite.Assert().Nil(doc.Content(&docContent)) {
			suite.Assert().Equalf(val+1, docContent["i"], "%s - expected map[i:%d] does not match actual %+v", key, val+1, docContent)
		}
	}
}
