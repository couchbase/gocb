package gocb

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"time"
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

func (suite *IntegrationTestSuite) TestTransactionsInsert() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txninsert"
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

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

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

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

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

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

	docID := "txninsertreplace"
	docValue := map[string]interface{}{
		"test": "test",
	}
	docValue2 := map[string]interface{}{
		"test": "test2",
	}

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

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

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

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

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

	_, err = txns.Run(func(ctx *TransactionAttemptContext) error {
		return ErrOopsieDoodle
	}, nil)
	suite.Require().ErrorIs(err, ErrOopsieDoodle)
}

func (suite *IntegrationTestSuite) TestTransactionsGetDocNotFoundAllowsContinue() {
	suite.skipIfUnsupported(TransactionsFeature)

	docID := "txndocnotfoundallowscontinue"
	docValue := map[string]interface{}{
		"test": "test",
	}

	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

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

func (suite *IntegrationTestSuite) TestMultipleTransactionObjects() {
	txns, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns.Close()

	txns2, err := globalCluster.Cluster.Transactions()
	suite.Require().Nil(err)
	defer txns2.Close()

	suite.Assert().Equal(&txns, &txns2)
}

// Skip this, will have to break it up or maybe make it a benchmark.
func (suite *IntegrationTestSuite) TestTransactions() {
	suite.T().Skipf("Skipping test")
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(TransactionsFeature)

	k00tok19 := make([]string, 20)
	for i := range k00tok19 {
		k00tok19[i] = fmt.Sprintf("k%02d", i)
	}

	k19tok00 := make([]string, 20)
	for i := range k00tok19 {
		k19tok00[i] = fmt.Sprintf("k%02d", 49-i)
	}

	k20tok39 := make([]string, 20)
	for i := range k20tok39 {
		k20tok39[i] = fmt.Sprintf("k%02d", 50+i)
	}

	k000tok099 := make([]string, 100)
	for i := range k000tok099 {
		k000tok099[i] = fmt.Sprintf("k%02d", i)
	}

	k100tok199 := make([]string, 100)
	for i := range k100tok199 {
		k100tok199[i] = fmt.Sprintf("k%02d", 100+i)
	}

	k000tok499 := make([]string, 500)
	for i := range k000tok499 {
		k000tok499[i] = fmt.Sprintf("k%02d", i)
	}

	kCON := []string{"kCON"}

	kCandk00tok19 := append(append([]string{}, kCON...), k00tok19...)
	k00tok19andkC := append(append([]string{}, k00tok19...), kCON...)
	kCandk20tok39 := append(append([]string{}, kCON...), k20tok39...)
	k20tok39andkC := append(append([]string{}, k20tok39...), kCON...)

	kCandk000tok099 := append(append([]string{}, kCON...), k000tok099...)
	k000tok099andkC := append(append([]string{}, k000tok099...), kCON...)
	kCandk100tok199 := append(append([]string{}, kCON...), k100tok199...)
	k100tok199andkC := append(append([]string{}, k100tok199...), kCON...)

	type testResult struct {
		NumSuccess int
		NumError   int
		NumIters   int
		Keys       []string
		MinTime    time.Duration
		MaxTime    time.Duration
		AvgTime    time.Duration
		SumTime    time.Duration
	}
	waitCh := make(chan *testResult, 100)
	cancelCh := make(chan struct{}, 100)

	getCluster := func() (*Collection, *Cluster) {
		cluster, err := Connect("couchbase://10.112.212.101", ClusterOptions{
			Username: "Administrator",
			Password: "password",
			TransactionsConfig: TransactionsConfig{
				//TransactionDurabilityLevel: TransactionDurabilityLevelMajority,
				DurabilityLevel: TransactionDurabilityLevelNone,
			},
		})
		if err != nil {
			log.Printf("Connect Error: %+v", err)
			panic(err)
		}

		bucket := cluster.Bucket("default")
		collection := bucket.DefaultCollection()

		bucket.WaitUntilReady(15*time.Second, nil)

		return collection, cluster
	}

	resetDocs := func(allKeys []string) {
		testDummy := map[string]int{"i": 1}
		collection, cluster := getCluster()

		ustime := time.Now()

		// Flush and wait for it to finish...
		collection.Upsert("flush-watch", nil, nil)
		cluster.Buckets().FlushBucket("default", nil)
		for atmptIdx := 0; atmptIdx < 512; atmptIdx++ {
			_, err := collection.Get("flush-watch", nil)
			if err != nil {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		for _, k := range allKeys {
			_, err := collection.Insert(k, testDummy, nil)
			if err != nil {
				log.Printf("failed to setup key: %s %s", k, err)
			}
		}

		uetime := time.Now()
		udtime := uetime.Sub(ustime)

		udtime.Hours() // just to remove the warning for now
		//log.Printf("  initial upsert took %s", udtime)

		cluster.Close(nil)

		time.Sleep(2 * time.Second)
	}

	doOps := func(name string, keys []string, numIters int, useOptim, disableTxns bool) {
		collection, cluster := getCluster()

		transactions, err := cluster.Transactions()
		suite.Require().Nil(err, err)

		//log.Printf("  %s testing (%+v)", name, keys)

		var minTime time.Duration
		var maxTime time.Duration
		var sumTime time.Duration

		numSuccess := 0
		numError := 0

		numToRun := 10000000
		if numIters > 0 {
			numToRun = numIters
		}

		for runIdx := 0; runIdx < numToRun; runIdx++ {
			select {
			case <-cancelCh:
				break
			default:
				// continue
			}

			numIters = runIdx + 1

			tstime := time.Now()

			if disableTxns {
				err = func() error {
					for _, k := range keys {
						for retryIdx := 0; retryIdx < 100; retryIdx++ {
							resObj, err := collection.Get(k, nil)
							if err != nil {
								return err
							}

							var valData map[string]int
							err = resObj.Content(&valData)
							if err != nil {
								return err
							}

							valData["i"]++

							_, err = collection.Replace(k, valData, &ReplaceOptions{
								Cas: resObj.Cas(),
							})
							if err != nil && errors.Is(err, ErrCasMismatch) {
								continue
							} else if err != nil {
								return err
							}

							break
						}
					}

					return nil
				}()
			} else {
				_, err = transactions.Run(func(ctx *TransactionAttemptContext) error {
					if useOptim {
						resObjs := make([]*TransactionGetResult, len(keys))
						valDatas := make([]map[string]int, len(keys))

						for kIdx, k := range keys {
							resObj, err := ctx.Get(collection, k)
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
							resObj, err := ctx.Get(collection, k)
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
			}
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

		avgTime := sumTime / time.Duration(numSuccess)

		log.Printf("  %s testing took %s, (%d iters, %d keys, %.2f success rate, min:%dms max:%dms avg:%dms)",
			name,
			sumTime.String(),
			numIters,
			len(keys),
			float64(numSuccess)/float64(numIters)*100,
			minTime/time.Millisecond, maxTime/time.Millisecond, avgTime/time.Millisecond)

		cluster.Close(nil)

		waitCh <- &testResult{
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

	type testGroup struct {
		name     string
		keys     []string
		isOptim  bool
		numIters int
	}

	runTestX := func(tname string, grps []testGroup, disableTxns bool) {
		log.Printf("running %s:", tname)

		allKeysMap := make(map[string]int)
		for _, grp := range grps {
			for _, key := range grp.keys {
				allKeysMap[key]++
			}
		}

		allKeys := make([]string, 0, len(allKeysMap))
		for key := range allKeysMap {
			allKeys = append(allKeys, key)
		}

		resetDocs(allKeys)

		for grpIdx, grp := range grps {
			var gname string
			if grp.isOptim {
				gname = fmt.Sprintf("%d-%s-opti", grpIdx+1, grp.name)
			} else {
				gname = fmt.Sprintf("%d-%s-pess", grpIdx+1, grp.name)
			}

			go doOps(gname, grp.keys, grp.numIters, grp.isOptim, disableTxns)
		}

		var ttlSuccess int
		var ttlError int
		var ttlIters int
		var ttlWrites int
		var minTime time.Duration
		var maxTime time.Duration
		var sumTime time.Duration
		var ttlTime time.Duration
		var numFinished int

		var numItered int
		var numInfinite int
		for _, grp := range grps {
			if grp.numIters != 0 {
				numItered++
			} else {
				numInfinite++
			}
		}

		groupVals := make(map[string]int)
		for range grps {
			tRes := <-waitCh

			numFinished++
			if numFinished == numItered {
				for i := 0; i < numInfinite; i++ {
					cancelCh <- struct{}{}
				}
			}

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

		// VALIDATE

		collection, cluster := getCluster()

		failedKeys := make([]string, 0)
		for key, val := range groupVals {
			doc, err := collection.Get(key, nil)
			if err != nil {
				panic(err)
			}

			var docContent map[string]int
			doc.Content(&docContent)
			if docContent["i"] != val+1 {
				failTxt := fmt.Sprintf("%s - expected map[i:%d] does not match actual %+v", key, val+1, docContent)
				failedKeys = append(failedKeys, failTxt)
			}
		}

		if len(failedKeys) > 0 {
			sort.Strings(failedKeys)
			log.Printf("  FAILED to validate some keys:")
			for _, txt := range failedKeys {
				log.Printf("    %s", txt)
			}
		}

		cluster.Close(nil)
	}

	runTest := func(tname string, grps []testGroup) {
		runTestX(tname, grps, false)
		//runTestX(tname+" - NO TXNS", grps, true)
		//runTestX(tname+" - WITH TXNS", grps, false)
	}

	baseNumIters := 50

	noConTests := false
	lowConTests := false
	highConTests := false
	vhighConTests := true
	medTxnTests := false
	bigTxnTests := false
	oneSidedTxnTests := false

	if noConTests {
		//*
		runTest("no contention, single thread, pess", []testGroup{
			{"k00tok19", k00tok19, false, baseNumIters},
		})
		//*/

		//*
		runTest("no contention, med txn, single thread, pess", []testGroup{
			{"k000tok099", k000tok099, false, baseNumIters},
		})
		//*/

		//*
		runTest("no contention, big txn, single thread, pess", []testGroup{
			{"k000tok499", k000tok499, false, baseNumIters},
		})
		//*/
	}

	if lowConTests {
		//*
		runTest("low,late contention, two threads, opti", []testGroup{
			{"k00tok19andkC", k00tok19andkC, true, baseNumIters},
			{"k20tok39andkC", k20tok39andkC, true, baseNumIters},
		})
		//*/

		//*
		runTest("low,late contention, two threads, pess", []testGroup{
			{"k00tok19andkC", k00tok19andkC, false, baseNumIters},
			{"k20tok39andkC", k20tok39andkC, false, baseNumIters},
		})
		//*/

		//*
		runTest("low,early contention, two threads, opti", []testGroup{
			{"kCandk00tok19", kCandk00tok19, true, baseNumIters},
			{"kCandk20tok39", kCandk20tok39, true, baseNumIters},
		})
		//*/

		//*
		runTest("low,early contention, two threads, pess", []testGroup{
			{"kCandk00tok19", kCandk00tok19, false, baseNumIters},
			{"kCandk20tok39", kCandk20tok39, false, baseNumIters},
		})
		//*/
	}

	if highConTests {
		//*
		runTest("high contention, three threads, two pess, one opti", []testGroup{
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
		})
		//*/

		//*
		runTest("high contention, three threads, two opti, one pess", []testGroup{
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
		})
		//*/

		//*
		runTest("high contention, two threads, opti", []testGroup{
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
		})
		//*/

		//*
		runTest("high contention, two threads, pess", []testGroup{
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
		})
		//*/
	}

	if vhighConTests {
		//*
		runTest("high contention, ten threads, opti", []testGroup{
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
			{"k00tok19", k00tok19, true, baseNumIters},
		})
		//*/

		//*
		runTest("high contention, ten threads, pess", []testGroup{
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
			{"k00tok19", k00tok19, false, baseNumIters},
		})
		//*/
	}

	if medTxnTests {
		//*
		runTest("low,late contention, med txn, two threads, opti", []testGroup{
			{"k000tok099andkC", k000tok099andkC, true, baseNumIters},
			{"k100tok199andkC", k100tok199andkC, true, baseNumIters},
		})
		//*/

		//*
		runTest("low,late contention, med txn, two threads, pess", []testGroup{
			{"k000tok099andkC", k000tok099andkC, false, baseNumIters},
			{"k100tok199andkC", k100tok199andkC, false, baseNumIters},
		})
		//*/

		//*
		runTest("low,early contention, med txn, two threads, opti", []testGroup{
			{"kCandk000tok099", kCandk000tok099, true, baseNumIters},
			{"kCandk100tok199", kCandk100tok199, true, baseNumIters},
		})
		//*/

		//*
		runTest("low,early contention, med txn, two threads, pess", []testGroup{
			{"kCandk000tok099", kCandk000tok099, false, baseNumIters},
			{"kCandk100tok199", kCandk100tok199, false, baseNumIters},
		})
		//*/
	}

	if oneSidedTxnTests {
		//*
		runTest("one-sided early contention, med txn, two threads, opti", []testGroup{
			{"kCON", kCON, true, baseNumIters * 101},
			{"kCandk100tok199", kCandk100tok199, true, baseNumIters},
		})
		//*/

		//*
		runTest("one-sided early contention, med txn, two threads, pess", []testGroup{
			{"kCON", kCON, false, baseNumIters * 101},
			{"kCandk100tok199", kCandk100tok199, false, baseNumIters},
		})
		//*/
	}

	if bigTxnTests {
		//*
		runTest("high contention, big txn, two threads, opti", []testGroup{
			{"k00tok199", k000tok499, true, baseNumIters},
			{"k00tok199", k000tok499, true, baseNumIters},
		})
		//*/

		//*
		runTest("high contention, big txn, two threads, pess", []testGroup{
			{"k00tok199", k000tok499, false, baseNumIters},
			{"k00tok199", k000tok499, false, baseNumIters},
		})
		//*/
	}
}
