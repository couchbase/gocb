package gocb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestTransactionsQueryModeInsert() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryinsert"
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		_, err = ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryModeReplace() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryreplace"
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
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		_, err = ctx.Replace(getRes, docValue2)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue2)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryModeRemove() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryremove"
	docValue := map[string]interface{}{
		"test": "test",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		err = ctx.Remove(getRes)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocumentNotFound(docID)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryModeDocNotFound() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "querydocnotfound"

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		_, err = ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Assert().ErrorIs(err, ErrDocumentNotFound)
	suite.Assert().Nil(txnRes)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryModeDocFound() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "querydocfound"
	docValue := map[string]interface{}{
		"test": "test",
	}

	_, err := globalCollection.Upsert(docID, docValue, nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocContent map[string]interface{}
		err = getRes.Content(&actualDocContent)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocContent)

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryUpdateStatement() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryupdatestatement"
	docValue := map[string]interface{}{
		"test": "test",
	}
	resultDocValue := map[string]interface{}{
		"test": "test",
		"foo":  float64(2),
	}

	err := globalCluster.QueryIndexes().CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		CollectionName: globalCollection.Name(),
		ScopeName:      globalScope.Name(),
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	_, err = globalCollection.Upsert(docID, docValue, &UpsertOptions{
		DurabilityLevel: DurabilityLevelMajority,
	})
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		queryRes, err := ctx.Query(
			fmt.Sprintf("UPDATE `%s` SET foo = 2 WHERE META().id = \"%s\"",
				globalCollection.Name(),
				docID,
			),
			&TransactionQueryOptions{
				Scope: globalScope,
			},
		)
		if err != nil {
			return err
		}
		meta, err := queryRes.MetaData()
		if err != nil {
			return err
		}

		suite.Assert().Equal(uint64(1), meta.Metrics.MutationCount)

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		var actualDocContent map[string]interface{}
		err = getRes.Content(&actualDocContent)
		if err != nil {
			return err
		}

		suite.Assert().Equal(resultDocValue, actualDocContent)

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, resultDocValue)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryUpdateStatementKVReplace() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryupdatestatementkvreplace"
	docValue := map[string]interface{}{
		"test": "test",
	}
	docValue2 := map[string]interface{}{
		"test": "test",
		"foo":  float64(4),
	}
	resultDocValue := map[string]interface{}{
		"test": "test",
		"foo":  float64(4),
	}

	err := globalCluster.QueryIndexes().CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		CollectionName: globalCollection.Name(),
		ScopeName:      globalScope.Name(),
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	_, err = globalCollection.Upsert(docID, docValue, &UpsertOptions{
		DurabilityLevel: DurabilityLevelMajority,
	})
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query(
			fmt.Sprintf("UPDATE `%s` SET foo = 2 WHERE META().id = \"%s\"",
				globalCollection.Name(),
				docID,
			),
			&TransactionQueryOptions{
				Scope: globalScope,
			},
		)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		_, err = ctx.Replace(getRes, docValue2)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, resultDocValue)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryUpdateStatementKVRemove() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryupdatestatementkvremove"
	docValue := map[string]interface{}{
		"test": "test",
	}

	err := globalCluster.QueryIndexes().CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		CollectionName: globalCollection.Name(),
		ScopeName:      globalScope.Name(),
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	_, err = globalCollection.Upsert(docID, docValue, &UpsertOptions{
		DurabilityLevel: DurabilityLevelMajority,
	})
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query(
			fmt.Sprintf("UPDATE `%s` SET foo = 2 WHERE META().id = \"%s\"",
				globalCollection.Name(),
				docID,
			),
			&TransactionQueryOptions{
				Scope: globalScope,
			},
		)
		if err != nil {
			return err
		}

		getRes, err := ctx.Get(globalCollection, docID)
		if err != nil {
			return err
		}

		err = ctx.Remove(getRes)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Require().NoError(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocumentNotFound(docID)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryDoubleInsertStatement() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "querydoubleinsert"

	err := globalCluster.QueryIndexes().CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		CollectionName: globalCollection.Name(),
		ScopeName:      globalScope.Name(),
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query(
			fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {\"foo\": 2})",
				globalBucket.Name(),
				docID,
			), nil)
		if err != nil {
			return err
		}

		_, err = ctx.Query(
			fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {\"foo\": 2})",
				globalBucket.Name(),
				docID,
			), nil)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Assert().ErrorIs(err, ErrDocumentExists)
	suite.Assert().Nil(txnRes)

}

func (suite *IntegrationTestSuite) TestTransactionsInsertReadByQuery() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "insertreadbyquery"
	docValue := map[string]interface{}{
		"test": "test",
	}

	err := globalCluster.QueryIndexes().CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		CollectionName: globalCollection.Name(),
		ScopeName:      globalScope.Name(),
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err = ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		queryRes, err := ctx.Query(fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE META().id = '%s'", globalCollection.Name(), globalCollection.Name(), docID), &TransactionQueryOptions{
			ScanConsistency: QueryScanConsistencyRequestPlus,
			Scope:           globalScope,
		})
		if err != nil {
			return err
		}

		var actualDocValue map[string]interface{}
		err = queryRes.One(&actualDocValue)
		if err != nil {
			return err
		}

		suite.Assert().Equal(docValue, actualDocValue)

		queryRes, err = ctx.Query(fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE META().id = 'insertreadbyquery'", globalCollection.Name(), globalCollection.Name()), &TransactionQueryOptions{
			Scope: globalScope,
		})
		if err != nil {
			return err
		}

		var vals []map[string]interface{}
		for queryRes.Next() {
			var actualDocValue map[string]interface{}
			err = queryRes.Row(&actualDocValue)
			if err != nil {
				return err
			}

			vals = append(vals, actualDocValue)
		}

		if suite.Assert().Len(vals, 1) {
			suite.Assert().Equal(docValue, vals[0])
		}

		return nil
	}, &TransactionOptions{
		Timeout: 30 * time.Second,
	})
	suite.Require().Nil(err, err)

	suite.Assert().True(txnRes.UnstagingComplete)
	suite.Assert().NotEmpty(txnRes.TransactionID)

	suite.verifyDocument(docID, docValue)

}

func (suite *IntegrationTestSuite) TestTransactionsQueryInsertDocExists() {
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(TransactionsQueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	docID := "queryinsertdocexists"
	docValue := map[string]interface{}{
		"test": "test",
	}

	_, err := globalCollection.Upsert(docID, "{}", nil)
	suite.Require().Nil(err, err)

	txns := globalCluster.Cluster.Transactions()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		_, err = ctx.Insert(globalCollection, docID, docValue)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	suite.Assert().ErrorIs(err, ErrDocumentExists)
	suite.Assert().Nil(txnRes)

}

type transactionMockQueryRowReader struct {
	Dataset []testBreweryDocument
	mockQueryRowReaderBase
}

func (suite *UnitTestSuite) TestTransactionsQueryGocbcoreCauseError() {
	var dataset struct {
		jsonQueryResponse
		Errors json.RawMessage
	}
	err := loadJSONTestDataset("transaction_gocbcore_cause_error", &dataset)
	suite.Require().Nil(err, err)

	var beginWorkDataset struct {
		jsonQueryResponse
	}
	err = loadJSONTestDataset("transaction_begin_work_response", &beginWorkDataset)
	suite.Require().Nil(err, err)

	reader := &mockQueryRowReader{
		Dataset: []testBreweryDocument{},
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			Meta:  suite.mustConvertToBytes(dataset.jsonQueryResponse),
			Suite: suite,
			RowsErr: &QueryError{
				ErrorText:      string(dataset.Errors),
				HTTPStatusCode: 200,
				Statement:      "somethiung",
				InnerError:     errors.New("query error"),
				Errors: []QueryErrorDesc{
					{
						Code:    1234,
						Message: "1234",
					},
				},
			},
		},
	}

	beginWorkReader := &mockQueryRowReader{
		Dataset: []testBreweryDocument{},
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			Meta: suite.mustConvertToBytes(beginWorkDataset.jsonQueryResponse),
		},
	}

	provider := new(mockQueryProviderCoreProvider)
	// BEGIN WORK
	provider.
		On("N1QLQuery", nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(beginWorkReader, nil).
		Once()
	// QUERY
	provider.
		On("N1QLQuery", nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(reader, nil).
		Once()
	// ROLLBACK
	provider.
		On("N1QLQuery", nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(beginWorkReader, nil).
		Once()

	queryProvider := &queryProviderCore{
		provider: provider,
	}

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("close").Return(nil)

	cluster := suite.newCluster(cli)
	defer cluster.Close(nil)

	queryProvider.meter = cluster.meter
	queryProvider.tracer = cluster.tracer
	queryProvider.retryStrategyWrapper = cluster.retryStrategyWrapper
	queryProvider.timeouts = cluster.timeoutsConfig

	txns := &transactionsProviderCore{}
	err = txns.Init(TransactionsConfig{
		CleanupConfig: TransactionsCleanupConfig{
			DisableLostAttemptCleanup: true,
		},
	}, cluster)
	suite.Require().Nil(err, err)
	defer txns.close()

	txnRes, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("SELECT 1=1", nil)
		if err != nil {
			return err
		}

		return nil
	}, nil, false)
	suite.Require().ErrorIs(err, ErrAttemptExpired)
	var finalErr *TransactionExpiredError
	suite.Assert().True(errors.As(err, &finalErr))
	suite.Assert().Nil(txnRes)
}
