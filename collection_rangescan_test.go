package gocb

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func makeBinaryValue(numBytes int) []byte {
	value := make([]byte, numBytes)
	for i := 0; i < numBytes; i++ {
		value[i] = byte(i)
	}

	return value
}

func makeDocIDs(numIDs int, prefix string) map[string]struct{} {
	docIDs := make(map[string]struct{})
	for i := 0; i < numIDs; i++ {
		docIDs[fmt.Sprintf("%s-%d", prefix, i)] = struct{}{}
	}

	return docIDs
}

func (suite *IntegrationTestSuite) upsertAndCreateMutationState(collection *Collection, docIDs map[string]struct{}, value interface{}, opts *UpsertOptions) *MutationState {
	mutationState := NewMutationState()
	var wg sync.WaitGroup
	wg.Add(len(docIDs))

	type tokenAndError struct {
		token *MutationToken
		err   error
	}

	ch := make(chan *tokenAndError, len(docIDs))
	for id := range docIDs {
		go func(id string) {
			res, err := collection.Upsert(id, value, opts)
			if err != nil {
				ch <- &tokenAndError{
					err: err,
				}
				wg.Done()
				return
			}

			ch <- &tokenAndError{
				token: res.MutationToken(),
			}
			wg.Done()
		}(id)
	}
	wg.Wait()
	close(ch)
	for {
		tok, ok := <-ch
		if !ok {
			break
		}

		suite.Require().Nil(tok.err, tok.err)

		mutationState.Add(*tok.token)
	}

	globalTracer.Reset()
	globalMeter.Reset()

	return mutationState
}

func (suite *IntegrationTestSuite) numVbuckets() int {
	a, err := globalBucket.Internal().IORouter()
	suite.Require().Nil(err, err)

	snap, err := a.ConfigSnapshot()
	suite.Require().Nil(err, err)

	numVbuckets, err := snap.NumVbuckets()
	suite.Require().Nil(err, err)

	return numVbuckets
}

func (suite *IntegrationTestSuite) verifyRangeScanTracing(topSpan *testSpan, numPartitions int, scanType ScanType, scopeName string, colName string, opts *ScanOptions) {
	suite.Assert().Equal("range_scan", topSpan.Name)

	suite.Assert().Equal(11, len(topSpan.Tags))
	suite.Assert().Equal("couchbase", topSpan.Tags[spanAttribDBSystemKey])
	suite.Assert().Equal(globalConfig.Bucket, topSpan.Tags[spanAttribDBNameKey])
	suite.Assert().Equal(scopeName, topSpan.Tags[spanAttribDBScopeNameKey])
	suite.Assert().Equal(colName, topSpan.Tags[spanAttribDBCollectionNameKey])
	suite.Assert().Equal("kv_scan", topSpan.Tags[spanAttribServiceKey])
	suite.Assert().Equal("range_scan", topSpan.Tags[spanAttribOperationKey])
	suite.Assert().Equal(numPartitions, topSpan.Tags["num_partitions"])
	suite.Assert().Equal(opts.IDsOnly, topSpan.Tags["without_content"])
	suite.Assert().True(topSpan.Finished)
	suite.Assert().Nil(topSpan.ParentContext)
	switch st := scanType.(type) {
	case RangeScan:
		suite.Assert().Equal("range", topSpan.Tags["scan_type"])
		suite.Assert().Equal(st.From.Term, topSpan.Tags["from_term"])
		suite.Assert().Equal(st.To.Term, topSpan.Tags["to_term"])
	case SamplingScan:
		suite.Assert().Equal("sampling", topSpan.Tags["scan_type"])
		suite.Assert().Equal(st.Limit, topSpan.Tags["limit"])
		suite.Assert().Equal(st.Seed, topSpan.Tags["seed"])
	}

	itemLimit := opts.BatchItemLimit
	if itemLimit == nil {
		limit := uint32(rangeScanDefaultItemLimit)
		itemLimit = &limit
	}
	byteLimit := opts.BatchByteLimit
	if byteLimit == nil {
		limit := uint32(rangeScanDefaultBytesLimit)
		byteLimit = &limit
	}

	suite.Assert().GreaterOrEqual(len(topSpan.Spans), 1)
	if suite.Assert().Contains(topSpan.Spans, "range_scan_partition") {
		pSpans := topSpan.Spans["range_scan_partition"]
		suite.Assert().Len(pSpans, numPartitions)
		for _, partitionSpan := range pSpans {
			suite.Assert().Equal("range_scan_partition", partitionSpan.Name)
			suite.Assert().Contains(partitionSpan.Tags, "partition_id")
			suite.Assert().True(partitionSpan.Finished)
			suite.Assert().Equal(topSpan.Context(), partitionSpan.ParentContext)
			// Partition spans may only contain a create span if there is no data in the partition.
			if suite.Assert().GreaterOrEqual(len(partitionSpan.Spans), 1) {
				if suite.Assert().Contains(partitionSpan.Spans, "range_scan_create") {
					cSpans := partitionSpan.Spans["range_scan_create"]
					if suite.Assert().Len(cSpans, 1) {
						s := cSpans[0]
						suite.Assert().Equal("range_scan_create", s.Name)
						suite.Assert().True(s.Finished)
						suite.Assert().Equal(partitionSpan.Context(), s.ParentContext)
						numTags := 2
						suite.Assert().Equal(opts.IDsOnly, s.Tags["without_content"])
						switch st := scanType.(type) {
						case RangeScan:
							suite.Assert().Equal("range", s.Tags["scan_type"])
							suite.Assert().Equal(st.From.Term, s.Tags["from_term"])
							suite.Assert().Equal(st.To.Term, s.Tags["to_term"])
							suite.Assert().Equal(st.From.Exclusive, s.Tags["from_exclusive"])
							suite.Assert().Equal(st.To.Exclusive, s.Tags["to_exclusive"])
							numTags += 4
						case SamplingScan:
							suite.Assert().Equal("sampling", s.Tags["scan_type"])
							suite.Assert().Equal(st.Limit, s.Tags["limit"])
							suite.Assert().Equal(st.Seed, s.Tags["seed"])
							numTags += 2
						}
						suite.Assert().Len(s.Tags, numTags)
						// if suite.Assert().Contains(s, memd.CmdRangeScanCreate.Name()) {
						// 	suite.AssertCmdSpan(s, memd.CmdRangeScanCreate.Name(), 0)
						// }
					}
				}
				if cSpans, ok := partitionSpan.Spans["range_scan_continue"]; ok {
					if suite.Assert().Len(cSpans, 1) {
						s := cSpans[0]
						suite.Assert().Equal("range_scan_continue", s.Name)
						suite.Assert().True(s.Finished)
						suite.Assert().Equal(partitionSpan.Context(), s.ParentContext)
						suite.Assert().Len(s.Tags, 4)
						suite.Assert().Equal(*itemLimit, s.Tags["item_limit"])
						suite.Assert().Equal(*byteLimit, s.Tags["byte_limit"])
						suite.Assert().Zero(s.Tags["time_limit"])
						suite.Assert().NotEmpty(s.Tags["range_scan_id"])
					}
				}
			}
		}
	}
}

func (suite *IntegrationTestSuite) TestRangeScanRangeWithContent() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	docIDs := makeDocIDs(100, "rangescanwithcontent-")

	value := "test"

	state := suite.upsertAndCreateMutationState(globalCollection, docIDs, value, &UpsertOptions{
		Expiry: 30 * time.Second,
	})
	scan := RangeScan{
		From: &ScanTerm{
			Term: "rangescanwithcontent",
		},
		To: &ScanTerm{
			Term: "rangescanwithcontent\xFF",
		},
	}
	opts := &ScanOptions{
		ConsistentWith: state,
	}

	res, err := globalCollection.Scan(scan, opts)
	suite.Require().Nil(err, err)

	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().NotZero(d.Cas())
		var v string
		err := d.Content(&v)
		if suite.Assert().Nil(err) {
			suite.Assert().Equal(value, v)
		}
		suite.Assert().Greater(time.Until(d.ExpiryTime()), 0*time.Second)

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, len(docIDs))
	for id := range docIDs {
		suite.Assert().Contains(ids, id)
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	numVbuckets := suite.numVbuckets()

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.verifyRangeScanTracing(nilParents[0], numVbuckets, scan, globalScope.Name(), globalCollection.Name(), opts)

	suite.AssertKVMetrics(meterNameCBOperations, "range_scan", 1, false)
}

func (suite *IntegrationTestSuite) TestRangeScanRangeWithoutContent() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	docIDs := makeDocIDs(100, "rangescanwithoutcontent-")

	value := makeBinaryValue(1)

	state := suite.upsertAndCreateMutationState(globalCollection, docIDs, value, &UpsertOptions{
		Expiry:     30 * time.Second,
		Transcoder: NewRawBinaryTranscoder(),
	})

	scan := RangeScan{
		From: &ScanTerm{
			Term: "rangescanwithoutcontent",
		},
		To: &ScanTerm{
			Term: "rangescanwithoutcontent\xFF",
		},
	}
	opts := &ScanOptions{
		IDsOnly:        true,
		ConsistentWith: state,
	}

	res, err := globalCollection.Scan(scan, opts)
	suite.Require().Nil(err, err)

	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().Zero(d.Cas())
		var v interface{}
		err := d.Content(&v)
		suite.Assert().ErrorIs(err, ErrInvalidArgument)
		suite.Assert().Zero(d.ExpiryTime())

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, len(docIDs))
	for id := range docIDs {
		suite.Assert().Contains(ids, id)
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	numVbuckets := suite.numVbuckets()

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.verifyRangeScanTracing(nilParents[0], numVbuckets, scan, globalScope.Name(), globalCollection.Name(), opts)

	suite.AssertKVMetrics(meterNameCBOperations, "range_scan", 1, false)
}

func (suite *IntegrationTestSuite) TestRangeScanRangeWithContentBinaryTranscoder() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	docIDs := makeDocIDs(100, "rangescanwithbinarycoder-")

	value := makeBinaryValue(1)
	tcoder := NewRawBinaryTranscoder()

	state := suite.upsertAndCreateMutationState(globalCollection, docIDs, value, &UpsertOptions{
		Expiry:     30 * time.Second,
		Transcoder: tcoder,
	})
	scan := RangeScan{
		From: &ScanTerm{
			Term: "rangescanwithbinarycoder",
		},
		To: &ScanTerm{
			Term: "rangescanwithbinarycoder\xFF",
		},
	}
	opts := &ScanOptions{
		Transcoder:     tcoder,
		ConsistentWith: state,
	}

	res, err := globalCollection.Scan(scan, opts)
	suite.Require().Nil(err, err)

	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().NotZero(d.Cas())
		var v []byte
		err := d.Content(&v)
		if suite.Assert().Nil(err) {
			suite.Assert().Equal(value, v)
		}
		suite.Assert().Greater(time.Until(d.ExpiryTime()), 0*time.Second)

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, len(docIDs))
	for id := range docIDs {
		suite.Assert().Contains(ids, id)
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	numVbuckets := suite.numVbuckets()

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.verifyRangeScanTracing(nilParents[0], numVbuckets, scan, globalScope.Name(), globalCollection.Name(), opts)

	suite.AssertKVMetrics(meterNameCBOperations, "range_scan", 1, false)
}

func (suite *IntegrationTestSuite) TestRangeScanRangeCancellation() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	// These keys all belong to vbid 12, which means that we can set the item limit and block the stream on
	// that vbucket from continuing.
	docIDs := map[string]struct{}{"rangekeysonly-1269": {}, "rangekeysonly-2048": {}, "rangekeysonly-4378": {},
		"rangekeysonly-7159": {}, "rangekeysonly-8898": {}, "rangekeysonly-8908": {}, "rangekeysonly-19559": {},
		"rangekeysonly-20808": {}, "rangekeysonly-20998": {}, "rangekeysonly-25889": {}}

	value := makeBinaryValue(1)

	state := suite.upsertAndCreateMutationState(globalCollection, docIDs, value, &UpsertOptions{
		Expiry:     30 * time.Second,
		Transcoder: NewRawBinaryTranscoder(),
	})

	scan := RangeScan{
		From: &ScanTerm{
			Term: "rangekeysonly",
		},
		To: &ScanTerm{
			Term: "rangekeysonly\xFF",
		},
	}
	itemLimit := uint32(1)
	opts := &ScanOptions{
		IDsOnly:        true,
		BatchItemLimit: &itemLimit,
		ConsistentWith: state,
	}

	res, err := globalCollection.Scan(scan, opts)
	suite.Require().Nil(err, err)

	stopAt := 5
	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		if len(ids) == stopAt {
			// At the point of close there should be no errors on the stream.
			err := res.Close()
			suite.Assert().Nil(err, err)
		}

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, 6)

	err = res.Err()
	suite.Require().ErrorIs(err, ErrRequestCanceled)
}

func (suite *IntegrationTestSuite) TestRangeScanSampling() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	scopeName := generateDocId("samplingrangescan")
	colMgr := globalBucket.CollectionsV2()
	err := colMgr.CreateScope(scopeName, nil)
	suite.Require().Nil(err, err)
	defer colMgr.DropScope(scopeName, nil)
	suite.EnsureScopeOnAllNodes(scopeName)

	err = colMgr.CreateCollection(scopeName, scopeName, nil, nil)
	suite.Require().Nil(err, err)

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{scopeName})

	docIDs := makeDocIDs(10, generateDocId("samplingscan"))

	value := "test"

	col := globalBucket.Scope(scopeName).Collection(scopeName)
	state := suite.upsertAndCreateMutationState(col, docIDs, value, &UpsertOptions{
		Expiry: 30 * time.Second,
	})
	scan := SamplingScan{
		Limit: 10,
		Seed:  50,
	}
	opts := &ScanOptions{
		ConsistentWith: state,
	}

	res, err := col.Scan(scan, opts)
	suite.Require().Nil(err, err)

	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().NotZero(d.Cas())
		var v string
		err := d.Content(&v)
		if suite.Assert().Nil(err) {
			suite.Assert().Equal(value, v)
		}
		suite.Assert().Greater(time.Until(d.ExpiryTime()), 0*time.Second)
		suite.Assert().NotEmpty(d.ID())

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, len(docIDs))
	for id := range docIDs {
		suite.Assert().Contains(ids, id)
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	numVbuckets := suite.numVbuckets()

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.verifyRangeScanTracing(nilParents[0], numVbuckets, scan, scopeName, scopeName, opts)

	suite.AssertKVMetrics(meterNameCBOperations, "range_scan", 1, false)
}

func (suite *IntegrationTestSuite) TestRangeScanRange() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	docIDs := makeDocIDs(100, "rangescanmutationstate-")

	value := makeBinaryValue(1)

	mutationState := suite.upsertAndCreateMutationState(globalCollection, docIDs, value, &UpsertOptions{
		Expiry:     30 * time.Second,
		Transcoder: NewRawBinaryTranscoder(),
	})

	scan := RangeScan{
		From: &ScanTerm{
			Term: "rangescanmutationstate",
		},
		To: &ScanTerm{
			Term: "rangescanmutationstate\xFF",
		},
	}
	opts := &ScanOptions{
		IDsOnly:        true,
		ConsistentWith: mutationState,
	}

	res, err := globalCollection.Scan(scan, opts)
	suite.Require().Nil(err, err)

	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().Zero(d.Cas())
		var v interface{}
		err := d.Content(&v)
		suite.Assert().ErrorIs(err, ErrInvalidArgument)
		suite.Assert().Zero(d.ExpiryTime())

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, len(docIDs))
	for id := range docIDs {
		suite.Assert().Contains(ids, id)
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	numVbuckets := suite.numVbuckets()

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.verifyRangeScanTracing(nilParents[0], numVbuckets, scan, globalScope.Name(), globalCollection.Name(), opts)

	suite.AssertKVMetrics(meterNameCBOperations, "range_scan", 1, false)
}

func (suite *IntegrationTestSuite) TestRangeScanRangeCtxCancelBeforeResults() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	scan := RangeScan{
		From: ScanTermMinimum(),
		To:   ScanTermMaximum(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts := &ScanOptions{
		IDsOnly: true,
		Context: ctx,
	}

	_, err := globalCollection.Scan(scan, opts)
	suite.Require().NotNil(err, err)
}

func (suite *IntegrationTestSuite) TestRangeScanRangeTimeoutBeforeResults() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	scan := RangeScan{
		From: ScanTermMinimum(),
		To:   ScanTermMaximum(),
	}
	opts := &ScanOptions{
		IDsOnly: true,
		Timeout: 1 * time.Nanosecond,
	}

	_, err := globalCollection.Scan(scan, opts)
	suite.Require().NotNil(err, err)

	suite.Require().ErrorIs(err, ErrTimeout)
}

func (suite *IntegrationTestSuite) TestRangeScanRangeEmoji() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RangeScanFeature)

	docIDs := map[string]struct{}{"\U0001F600": {}}

	value := makeBinaryValue(1)

	mutationState := suite.upsertAndCreateMutationState(globalCollection, docIDs, value, &UpsertOptions{
		Expiry:     30 * time.Second,
		Transcoder: NewRawBinaryTranscoder(),
	})

	scan := NewRangeScanForPrefix("\U0001F600")
	opts := &ScanOptions{
		IDsOnly:        true,
		ConsistentWith: mutationState,
	}

	res, err := globalCollection.Scan(scan, opts)
	suite.Require().Nil(err, err)

	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().Zero(d.Cas())
		var v interface{}
		err := d.Content(&v)
		suite.Assert().ErrorIs(err, ErrInvalidArgument)
		suite.Assert().Zero(d.ExpiryTime())

		ids[d.ID()] = struct{}{}
	}

	suite.Assert().Len(ids, len(docIDs))
	for id := range docIDs {
		suite.Assert().Contains(ids, id)
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	numVbuckets := suite.numVbuckets()

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.verifyRangeScanTracing(nilParents[0], numVbuckets, scan, globalScope.Name(), globalCollection.Name(), opts)

	suite.AssertKVMetrics(meterNameCBOperations, "range_scan", 1, false)
}
