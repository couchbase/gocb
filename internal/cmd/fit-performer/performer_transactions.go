package main

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/hooks"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/twoway"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	protoTransactions "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
)

func (ts *Performer) TransactionCreate(_ context.Context, req *protoTransactions.TransactionCreateRequest) (*protoTransactions.TransactionResult, error) {
	ts.logger.Logf(logrus.InfoLevel, "TransactionCreate called for %s", req.ClusterConnectionId)
	ts.lock.Lock()
	conn := ts.getConnLocked(req.ClusterConnectionId)
	ts.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	var opts *gocb.TransactionOptions
	if req.Options != nil {
		var metaCollection *gocb.Collection
		c := req.Options.MetadataCollection
		if c != nil {
			metaCollection = conn.Bucket(c.BucketName).Scope(c.ScopeName).Collection(c.CollectionName)
		}

		opts = &gocb.TransactionOptions{
			DurabilityLevel:    helpers.ProtocolDuraToSDK(req.Options.GetDurability()),
			Timeout:            time.Duration(req.Options.GetTimeoutMillis()) * time.Millisecond,
			MetadataCollection: metaCollection,
		}

		if len(req.Options.Hook) > 0 {
			tHooks := hooks.NewTransactionHooks()
			if err := tHooks.Configure(conn, req.Options.Hook); err != nil {
				return nil, status.Errorf(codes.Aborted, "failed to setup hooks: %v", err)
			}
			opts.Internal.Hooks = tHooks
		}
	}

	txn := twoway.New(conn, ts.logger)
	res, err := txn.Run(conn.Transactions(), req.ExpectedEvents, req.Attempts, opts, nil)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if ok {
			return nil, status.Errorf(grpcStatus.Code(), "transaction failed to run: %v", err)
		}
		return nil, status.Errorf(codes.Aborted, "transaction failed to run: %v", err)
	}

	return res, nil
}

// TransactionStream that can communicate bi-directionally with the tests
// This is for more complex tests that require e.g. concurrent transactions, gating each other's progress
func (ts *Performer) TransactionStream(in protocol.PerformerService_TransactionStreamServer) error {
	ts.logger.Log(logrus.InfoLevel, "TransactionStream called")
	var twTxn *twoway.Transaction
	startChan := make(chan struct{})
	endChan := make(chan struct{}, 1)
	type recvStr struct {
		recv *protoTransactions.TransactionStreamDriverToPerformer
		err  error
	}
	recvChan := make(chan recvStr)

	go func() {
		for {
			recv, err := in.Recv()
			recvChan <- recvStr{
				recv: recv,
				err:  err,
			}

			select {
			case <-endChan:
				close(recvChan)
				return
			default:
			}
		}
	}()

	for {
		var recv *protoTransactions.TransactionStreamDriverToPerformer
		var err error
		select {
		case <-endChan:
			ts.logger.Logf(logrus.InfoLevel, "Stream %p shutdown", twTxn)
			time.Sleep(100 * time.Millisecond)
			return nil
		case <-in.Context().Done():
			close(endChan)
			ts.logger.Logf(logrus.InfoLevel, "Stream %p shutdown requested from other side", twTxn)
			return in.Context().Err()
		case r := <-recvChan:
			recv = r.recv
			err = r.err
		}

		if err == io.EOF {
			ts.logger.Logf(logrus.InfoLevel, "Closing txn %p due to EOF", twTxn)
			return nil
		}
		if err != nil {
			ts.logger.Logf(logrus.ErrorLevel, "Aborting txn %p: %+v", twTxn, err)
			return status.Errorf(codes.Aborted, "aborted due to error: %v", err)
		}

		ts.logger.Logf(logrus.InfoLevel, "Stream req for txn %p, from driver: %+v", twTxn, recv.Request)

		switch req := recv.Request.(type) {
		case *protoTransactions.TransactionStreamDriverToPerformer_Create:
			go func() {
				ts.lock.Lock()
				conn := ts.getConnLocked(req.Create.ClusterConnectionId)
				ts.lock.Unlock()
				if conn == nil {
					ts.logger.Errorf("connection id %s not known", req.Create.ClusterConnectionId)
					return
				}
				twTxn = twoway.New(conn, ts.logger)

				// It's not entirely clear how to handle errors that occur here.
				twTxn.AddLatches(req.Create.Latches)
				name := req.Create.Name

				err := in.Send(&protoTransactions.TransactionStreamPerformerToDriver{
					Response: &protoTransactions.TransactionStreamPerformerToDriver_Created{
						Created: &protoTransactions.TransactionCreated{},
					},
				})
				if err != nil {
					ts.logger.Logf(logrus.ErrorLevel, "TransactionExecutor %p %s failed to send to driver: %v", twTxn, name, err)
					return
				}

				<-startChan

				var opts *gocb.TransactionOptions
				if req.Create.Options != nil {
					var metaCollection *gocb.Collection
					c := req.Create.Options.MetadataCollection
					if c != nil {
						metaCollection = conn.Bucket(c.BucketName).Scope(c.ScopeName).Collection(c.CollectionName)
					}

					opts = &gocb.TransactionOptions{
						DurabilityLevel:    helpers.ProtocolDuraToSDK(req.Create.Options.GetDurability()),
						Timeout:            time.Duration(req.Create.Options.GetTimeoutMillis()) * time.Millisecond,
						MetadataCollection: metaCollection,
					}

					if len(req.Create.Options.Hook) > 0 {
						tHooks := hooks.NewTransactionHooks()
						if err := tHooks.Configure(conn, req.Create.Options.Hook); err != nil {
							ts.logger.Logf(logrus.ErrorLevel, "failed to setup hooks: %v", err)
							return
						}
						opts.Internal.Hooks = tHooks
					}
				}

				res, err := twTxn.Run(conn.Transactions(), req.Create.ExpectedEvents, req.Create.Attempts,
					opts, in)
				if err != nil {
					ts.logger.Logf(logrus.ErrorLevel, "Failed to run transaction %p %s: %v", twTxn, name, err)
					return
				}

				ts.logger.Logf(logrus.InfoLevel, "TransactionExecutor %p %s finished, completing stream", twTxn, name)

				err = in.Send(&protoTransactions.TransactionStreamPerformerToDriver{
					Response: &protoTransactions.TransactionStreamPerformerToDriver_FinalResult{
						FinalResult: res,
					},
				})
				if err != nil {
					ts.logger.Logf(logrus.ErrorLevel, "TransactionExecutor %p %s failed to send to driver: %v", twTxn, name, err)
				}

				close(endChan)
			}()
		case *protoTransactions.TransactionStreamDriverToPerformer_Start:
			close(startChan)
		case *protoTransactions.TransactionStreamDriverToPerformer_Broadcast:
			switch bReq := req.Broadcast.Request.(type) {
			case *protoTransactions.BroadcastToOtherConcurrentTransactionsRequest_LatchSet:
				err := twTxn.CountdownLatch(bReq.LatchSet.LatchName)
				if err != nil {
					return status.Error(codes.Aborted, err.Error())
				}
			default:
				return status.Error(codes.InvalidArgument, "unknown broadcast request from driver")
			}
		default:
			return status.Error(codes.InvalidArgument, "unknown request from driver")
		}
	}
}

// Performs cleanup of a single transaction.
func (ts *Performer) TransactionCleanup(_ context.Context,
	req *protoTransactions.TransactionCleanupRequest) (*protoTransactions.TransactionCleanupAttempt, error) {
	ts.logger.Log(logrus.InfoLevel, "TransactionCleanup called")

	ts.lock.Lock()
	conn := ts.getConnLocked(req.ClusterConnectionId)
	ts.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	cHooks := hooks.NewCleanupHooks()

	if err := cHooks.Configure(conn, req.Hook); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to configure hooks: %v", err)
	}

	cfg := &gocb.TransactionsConfig{}
	cfg.Internal.Hooks = nil
	cfg.Internal.CleanupHooks = cHooks
	cfg.DurabilityLevel = gocb.DurabilityLevelMajority

	cleaner := gocb.NewTransactionsCleaner(ts.makeBucketProvider(conn), cfg)

	bucket := conn.Bucket(req.Atr.BucketName)
	collection := bucket.Scope(req.Atr.ScopeName).Collection(req.Atr.CollectionName)
	atr, err := findEntryForTransaction(req.Atr.DocId, req.AttemptId, collection)
	if err != nil {
		if !errors.Is(err, gocb.ErrPathNotFound) && !errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, status.Errorf(codes.Aborted, "Failed to find entry for transaction: %v", err)
		}
	}

	var result *gocb.TransactionCleanupAttempt
	if !errors.Is(err, gocb.ErrPathNotFound) && !errors.Is(err, gocb.ErrDocumentNotFound) {
		state, err := jsonStateToGocbState(atr.State)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "Failed to convert atr state: %v", err)
		}

		r := cleaner.CleanupAttempt(bucket, false, &gocb.TransactionCleanupRequest{
			AttemptID:         req.AttemptId,
			AtrID:             req.Atr.DocId,
			AtrCollectionName: req.Atr.CollectionName,
			AtrScopeName:      req.Atr.ScopeName,
			AtrBucketName:     req.Atr.BucketName,
			State:             state,
			Inserts:           atrMutationsToDocRecords(atr.Inserts),
			Removes:           atrMutationsToDocRecords(atr.Removes),
			Replaces:          atrMutationsToDocRecords(atr.Replaces),
			ForwardCompat:     jsonForwardCompatToTransactions(atr.ForwardCompat),
		})
		result = &r
	}

	if result == nil {
		return &protoTransactions.TransactionCleanupAttempt{
			Success: false,
			Logs:    []string{"Failed at performer to get ATR entry before running cleanupATREntry"},
			Atr: &protoTransactions.DocId{
				BucketName:     req.Atr.BucketName,
				ScopeName:      req.Atr.ScopeName,
				CollectionName: req.Atr.CollectionName,
				DocId:          req.Atr.DocId,
			},
			AttemptId: req.AttemptId,
		}, nil
	}

	return &protoTransactions.TransactionCleanupAttempt{
		Success: result.Success,
		Logs:    nil,
		Atr: &protoTransactions.DocId{
			BucketName:     result.AtrBucketName,
			ScopeName:      result.AtrScopeName,
			CollectionName: result.AtrCollectionName,
			DocId:          result.AtrID,
		},
		AttemptId: req.AttemptId,
		State:     protoTransactions.AttemptStates(protoTransactions.AttemptStates_value[atr.State]),
	}, nil
}

// Performs cleanup of a full ATR.  Useful for testing multiple performers concurrently cleaning up same ATR.
// Note it will only cleanup expired entries.
func (ts *Performer) TransactionCleanupATR(_ context.Context,
	req *protoTransactions.TransactionCleanupATRRequest) (*protoTransactions.TransactionCleanupATRResult, error) {
	ts.logger.Log(logrus.InfoLevel, "TransactionCleanupATR called")

	ts.lock.Lock()
	conn := ts.getConnLocked(req.ClusterConnectionId)
	ts.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	cHooks := hooks.NewCleanupHooks()
	crHooks := hooks.NewClientRecordHooks()

	if err := cHooks.Configure(conn, req.Hook); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to setup cleanup hooks: %v", err)
	}
	if err := crHooks.Configure(conn, req.Hook); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to setup client record hooks: %v", err)
	}

	cfg := &gocb.TransactionsConfig{}
	cfg.Internal.CleanupHooks = cHooks
	cfg.Internal.ClientRecordHooks = crHooks
	cfg.DurabilityLevel = gocb.DurabilityLevelMajority

	cleaner := gocb.NewLostTransactionsCleanup(ts.makeBucketProvider(conn), ts.makeLostATRLocationProvider(req.Atr.BucketName, req.Atr.CollectionName, req.Atr.ScopeName), cfg)
	bucket := conn.Bucket(req.Atr.BucketName)
	results, stats := cleaner.ProcessATR(bucket, req.Atr.CollectionName, req.Atr.ScopeName, req.Atr.DocId)
	var attempts []*protoTransactions.TransactionCleanupAttempt
	for _, result := range results {
		attempts = append(attempts, &protoTransactions.TransactionCleanupAttempt{
			Success: result.Success,
			Atr: &protoTransactions.DocId{
				BucketName:     result.AtrBucketName,
				ScopeName:      result.AtrScopeName,
				CollectionName: result.AtrCollectionName,
				DocId:          result.AtrID,
			},
			AttemptId: result.AttemptID,
		})
	}

	return &protoTransactions.TransactionCleanupATRResult{
		Result:            attempts,
		NumEntries:        int32(stats.NumEntries),
		NumExpiredEntries: int32(stats.NumEntriesExpired),
	}, nil
}

func (ts *Performer) CleanupSetFetch(_ context.Context, req *protoTransactions.CleanupSetFetchRequest) (*protoTransactions.CleanupSetFetchResponse, error) {
	ts.lock.Lock()
	conn := ts.getConnLocked(req.ClusterConnectionId)
	ts.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	locations := conn.Transactions().Internal().CleanupLocations()
	var protoLocs []*shared.Collection
	for _, location := range locations {
		protoLocs = append(protoLocs, &shared.Collection{
			BucketName:     location.BucketName,
			ScopeName:      location.ScopeName,
			CollectionName: location.CollectionName,
		})
	}

	return &protoTransactions.CleanupSetFetchResponse{
		CleanupSet: &protoTransactions.CleanupSet{
			CleanupSet: protoLocs,
		},
	}, nil
}

// Request that the implementation do its normal client record processing logic (creating CR if needed).
func (ts *Performer) ClientRecordProcess(_ context.Context,
	req *protoTransactions.ClientRecordProcessRequest) (*protoTransactions.ClientRecordProcessResponse, error) {
	ts.logger.Log(logrus.InfoLevel, "ClientRecordProcess called")

	ts.lock.Lock()
	conn := ts.getConnLocked(req.ClusterConnectionId)
	ts.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	cHooks := hooks.NewCleanupHooks()
	crHooks := hooks.NewClientRecordHooks()

	if err := cHooks.Configure(conn, req.Hook); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to setup cleanup hooks: %v", err)
	}
	if err := crHooks.Configure(conn, req.Hook); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to setup client record hooks: %v", err)
	}

	cfg := &gocb.TransactionsConfig{}
	cfg.Internal.CleanupHooks = cHooks
	cfg.Internal.ClientRecordHooks = crHooks
	cfg.Internal.NumATRs = 1024

	cleaner := gocb.NewLostTransactionsCleanup(ts.makeBucketProvider(conn), ts.makeLostATRLocationProvider(req.BucketName,
		req.CollectionName, req.ScopeName), cfg)
	bucket := conn.Bucket(req.BucketName)
	crd, err := cleaner.ProcessClient(bucket, req.CollectionName, req.ScopeName, req.ClientUuid)
	if err != nil {
		return &protoTransactions.ClientRecordProcessResponse{
			Success: false,
		}, nil
	}

	return &protoTransactions.ClientRecordProcessResponse{
		NumActiveClients:   int32(crd.NumActiveClients),
		IndexOfThisClient:  int32(crd.IndexOfThisClient),
		ExpiredClientIds:   crd.ExpiredClientIDs,
		NumExistingClients: int32(crd.NumExistingClients),
		NumExpiredClients:  int32(crd.NumExpiredClients),
		OverrideEnabled:    crd.OverrideEnabled,
		OverrideActive:     crd.OverrideActive,
		OverrideExpires:    crd.OverrideExpiresCas,
		CasNowNanos:        crd.CasNowNanos,
		ClientUuid:         crd.ClientUUID,
		Success:            true,
	}, nil
}

func (ts *Performer) TransactionSingleQuery(ctx context.Context,
	req *protoTransactions.TransactionSingleQueryRequest) (*protoTransactions.TransactionSingleQueryResponse, error) {
	ts.logger.Log(logrus.InfoLevel, "TransactionSingleQuery called")

	ts.lock.Lock()
	conn := ts.getConnLocked(req.ClusterConnectionId)
	ts.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	var asTransaction gocb.SingleQueryTransactionOptions
	if req.QueryOptions != nil && req.QueryOptions.SingleQueryTransactionOptions != nil {
		asTransaction = gocb.SingleQueryTransactionOptions{
			DurabilityLevel: helpers.ProtocolDuraToSDK(req.QueryOptions.SingleQueryTransactionOptions.GetDurability()),
		}

		if len(req.QueryOptions.SingleQueryTransactionOptions.Hook) > 0 {
			tHooks := hooks.NewTransactionHooks()
			if err := tHooks.Configure(conn, req.QueryOptions.SingleQueryTransactionOptions.Hook); err != nil {
				return nil, status.Errorf(codes.Aborted, "failed to setup hooks: %v", err)
			}

			asTransaction.Internal.Hooks = tHooks
		}
	}

	txn := twoway.New(conn, ts.logger)
	return txn.SingleQuery(conn.Cluster(), req.QueryOptions, asTransaction, req.Query)
}

func (ts *Performer) Echo(ctx context.Context,
	in *shared.EchoRequest) (*shared.EchoResponse, error) {
	ts.logger.Logf(logrus.InfoLevel, "================ %s : %s ================ ", in.TestName, in.Message)
	return &shared.EchoResponse{}, nil
}

func (ts *Performer) makeBucketProvider(conn *cluster.Connection) func(string) (*gocb.Bucket, string, error) {
	return func(bucketName string) (*gocb.Bucket, string, error) {
		return conn.Bucket(bucketName), "", nil
	}
}

func (ts *Performer) makeLostATRLocationProvider(bucket, collection, scope string) func() ([]gocb.TransactionKeyspace, error) {
	return func() ([]gocb.TransactionKeyspace, error) {
		return []gocb.TransactionKeyspace{
			{
				BucketName:     bucket,
				ScopeName:      scope,
				CollectionName: collection,
			},
		}, nil
	}
}

type jsonAtrMutation struct {
	BucketName     string `json:"bkt,omitempty"`
	ScopeName      string `json:"scp,omitempty"`
	CollectionName string `json:"col,omitempty"`
	DocID          string `json:"id,omitempty"`
}

type jsonAtrAttempt struct {
	TransactionID string `json:"tid,omitempty"`
	ExpiryTime    uint   `json:"exp,omitempty"`
	State         string `json:"st,omitempty"`

	PendingCAS    string `json:"tst,omitempty"`
	CommitCAS     string `json:"tsc,omitempty"`
	CompletedCAS  string `json:"tsco,omitempty"`
	AbortCAS      string `json:"tsrs,omitempty"`
	RolledBackCAS string `json:"tsrc,omitempty"`

	Inserts  []jsonAtrMutation `json:"ins,omitempty"`
	Replaces []jsonAtrMutation `json:"rep,omitempty"`
	Removes  []jsonAtrMutation `json:"rem,omitempty"`

	ForwardCompat map[string][]jsonForwardCompatibilityEntry `json:"fc,omitempty"`
}

type jsonForwardCompatibilityEntry struct {
	ProtocolVersion   string `json:"p,omitempty"`
	ProtocolExtension string `json:"e,omitempty"`
	Behaviour         string `json:"b,omitempty"`
	RetryInterval     int    `json:"ra,omitempty"`
}

func findEntryForTransaction(atrID, attemptID string, collection *gocb.Collection) (*jsonAtrAttempt, error) {
	res, err := collection.LookupIn(
		atrID,
		[]gocb.LookupInSpec{
			gocb.GetSpec("attempts."+attemptID, &gocb.GetSpecOptions{
				IsXattr: true,
			}),
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	var atr *jsonAtrAttempt
	if err := res.ContentAt(0, &atr); err != nil {
		return nil, err
	}

	return atr, nil
}

func jsonStateToGocbState(st string) (gocb.TransactionAttemptState, error) {
	switch st {
	case "PENDING":
		return gocb.TransactionAttemptStatePending, nil
	case "COMMITTED":
		return gocb.TransactionAttemptStateCommitted, nil
	case "COMPLETED":
		return gocb.TransactionAttemptStateCompleted, nil
	case "ABORTED":
		return gocb.TransactionAttemptStateAborted, nil
	case "ROLLED_BACK":
		return gocb.TransactionAttemptStateRolledBack, nil
	default:
		return gocb.TransactionAttemptState(0), nil

	}
}

func atrMutationsToDocRecords(drs []jsonAtrMutation) []gocb.TransactionDocRecord {
	var recs []gocb.TransactionDocRecord
	for _, i := range drs {
		recs = append(recs, gocb.TransactionDocRecord{
			CollectionName: i.CollectionName,
			ScopeName:      i.ScopeName,
			BucketName:     i.BucketName,
			ID:             i.DocID,
		})
	}

	return recs
}

func jsonForwardCompatToTransactions(fc map[string][]jsonForwardCompatibilityEntry) map[string][]gocb.TransactionsForwardCompatibilityEntry {
	forwardCompat := make(map[string][]gocb.TransactionsForwardCompatibilityEntry)
	for k, entries := range fc {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]gocb.TransactionsForwardCompatibilityEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = gocb.TransactionsForwardCompatibilityEntry(entry)
		}
	}

	return forwardCompat
}
