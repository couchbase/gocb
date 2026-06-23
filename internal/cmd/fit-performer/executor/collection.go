package executor

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/getreplicas"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	fitLookupIn "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
	fitStreams "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/collection/mutatein"
	collectionindexmanager "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/collection/query/indexmanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/query/index/manager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/streams"
)

func (e *Executor) handleCollectionLevelCommand(command *sdk.CollectionLevelCommand, sender sender.ResultSender, returnResult bool) (bool, error) {
	switch op := command.Command.(type) {
	case *sdk.CollectionLevelCommand_Touch:

		expiry, err := helpers.Expiry(op.Touch.Expiry)
		if err != nil {
			return false, err
		}

		return e.sendMutation(op.Touch.Location, nil, returnResult, sender,
			func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error) {
				opts, err := e.createTouchOptions(op.Touch.Options)
				if err != nil {
					return nil, time.Time{}, err
				}

				start := time.Now()
				res, err := collection.Touch(id, expiry, opts)
				return res, start, err
			})
	case *sdk.CollectionLevelCommand_GetAndTouch:
		loc, err := helpers.Location(op.GetAndTouch.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		expiry, err := helpers.Expiry(op.GetAndTouch.Expiry)
		if err != nil {
			return false, err
		}

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createGetAndTouchOptions(op.GetAndTouch.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()

		res, err := col.GetAndTouch(loc.ID(), expiry, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			var expiry *int64
			if !res.ExpiryTime().IsZero() {
				unix := res.ExpiryTime().Unix()
				expiry = &unix
			}
			content, err := helpers.ParseContentAs(op.GetAndTouch.ContentAs, func(content interface{}) error {
				return res.Content(&content)
			})
			if err != nil {
				return false, err
			}

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_GetResult{
						GetResult: &kv.GetResult{
							Cas:        int64(res.Cas()),
							ExpiryTime: expiry,
							Content:    content,
						},
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_GetAndLock:
		loc, err := helpers.Location(op.GetAndLock.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		duration := time.Duration(op.GetAndLock.Duration.GetSeconds()) * time.Second

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createGetAndLockOptions(op.GetAndLock.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()

		res, err := col.GetAndLock(loc.ID(), duration, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			var expiry *int64
			if !res.ExpiryTime().IsZero() {
				unix := res.ExpiryTime().Unix()
				expiry = &unix
			}
			content, err := helpers.ParseContentAs(op.GetAndLock.ContentAs, func(content interface{}) error {
				return res.Content(&content)
			})
			if err != nil {
				return false, err
			}
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_GetResult{
						GetResult: &kv.GetResult{
							Cas:        int64(res.Cas()),
							ExpiryTime: expiry,
							Content:    content,
						},
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}
		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_Unlock:
		loc, err := helpers.Location(op.Unlock.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		cas := gocb.Cas(op.Unlock.Cas)

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}

		opts, err := e.createUnlockOptions(op.Unlock.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()

		err = col.Unlock(loc.ID(), cas, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		result.Result = &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_Success{Success: true},
			},
		}
		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_Exists:
		loc, err := helpers.Location(op.Exists.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createExistsOptions(op.Exists.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()

		res, err := col.Exists(loc.ID(), opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_ExistsResult{
						ExistsResult: &kv.ExistsResult{
							Cas:    int64(res.Cas()),
							Exists: res.Exists(),
						},
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_Binary:

		blc := op.Binary
		switch blc.Command.(type) {
		case *sdk.BinaryCollectionLevelCommand_Append:
			return e.sendMutation(blc.GetAppend().Location, nil, returnResult, sender,
				func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error) {
					opts, err := e.createAppendOptions(blc.GetAppend().Options)
					if err != nil {
						return nil, time.Time{}, err
					}

					start := time.Now()
					res, err := collection.Binary().Append(id, blc.GetAppend().Content, opts)
					return res, start, err
				})

		case *sdk.BinaryCollectionLevelCommand_Prepend:
			return e.sendMutation(op.Binary.GetPrepend().Location, nil, returnResult, sender,
				func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error) {
					opts, err := e.createPrependOptions(blc.GetPrepend().Options)
					if err != nil {
						return nil, time.Time{}, err
					}

					start := time.Now()
					res, err := collection.Binary().Prepend(id, blc.GetPrepend().Content, opts)
					return res, start, err
				})
		case *sdk.BinaryCollectionLevelCommand_Decrement:
			return e.sendCounter(blc.GetDecrement().Location, nil, returnResult, sender,
				func(collection *gocb.Collection, id string, content interface{}) (*gocb.CounterResult, time.Time, error) {
					opts, err := e.createDecrementOptions(blc.GetDecrement().Options)
					if err != nil {
						return nil, time.Time{}, err
					}

					start := time.Now()
					res, err := collection.Binary().Decrement(id, opts)
					return res, start, err
				})
		case *sdk.BinaryCollectionLevelCommand_Increment:
			return e.sendCounter(blc.GetIncrement().Location, nil, returnResult, sender,
				func(collection *gocb.Collection, id string, content interface{}) (*gocb.CounterResult, time.Time, error) {
					opts, err := e.createIncrementOptions(blc.GetIncrement().Options)
					if err != nil {
						return nil, time.Time{}, err
					}

					start := time.Now()
					res, err := collection.Binary().Increment(id, opts)
					return res, start, err
				})
		}
	case *sdk.CollectionLevelCommand_MutateIn:
		loc, err := helpers.Location(op.MutateIn.Location, e.counters)
		if err != nil {
			return false, err
		}
		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())
		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createMutateInOptions(op.MutateIn.Options)
		if err != nil {
			return false, err
		}
		specs, err := createMutateInSpecs(op.MutateIn.Spec)
		if err != nil {
			return false, err
		}
		res, err := col.MutateIn(loc.ID(), specs, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		if returnResult {
			sdkResult := &mutatein.MutateInResult{
				Cas: int64(res.Cas()),
			}
			tok := res.MutationToken()
			if res.MutationToken() != nil {
				sdkResult.MutationToken = &shared.MutationToken{
					PartitionId:    int32(tok.PartitionID()),
					PartitionUuid:  int64(tok.PartitionUUID()),
					SequenceNumber: int64(tok.SequenceNumber()),
					BucketName:     tok.BucketName(),
				}
			}
			var resultContent []*mutatein.MutateInSpecResult
			for i, spec := range op.MutateIn.Spec {
				specResult := mutatein.MutateInSpecResult{}
				if spec.ContentAs != nil {
					content, err := helpers.ParseContentAs(spec.GetContentAs(), func(content interface{}) error {
						return res.ContentAt(uint(i), &content)
					})
					if err != nil {
						return false, err
					}

					specResult.ContentAsResult = &shared.ContentOrError{
						Result: &shared.ContentOrError_Content{
							Content: content,
						},
					}

				}
				resultContent = append(resultContent, &specResult)
			}
			sdkResult.Results = resultContent

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_MutateInResult{
						MutateInResult: sdkResult,
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_GetAnyReplica:
		loc, err := helpers.Location(op.GetAnyReplica.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createGetAnyReplicaOptions(op.GetAnyReplica.Options)
		if err != nil {
			return false, err
		}

		res, err := col.GetAnyReplica(loc.ID(), opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		var content json.RawMessage
		err = res.Content(&content)
		if err != nil {
			return false, err
		}

		if returnResult {
			content, err := helpers.ParseContentAs(op.GetAnyReplica.ContentAs, func(content interface{}) error {
				return res.Content(&content)
			})
			if err != nil {
				return false, err
			}

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_GetReplicaResult{
						GetReplicaResult: &kv.GetReplicaResult{
							Content:   content,
							Cas:       int64(res.Cas()),
							IsReplica: res.IsReplica(),
						},
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}
		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_GetAllReplicas:
		loc, err := helpers.Location(op.GetAllReplicas.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		opts, err := e.createGetAllReplicasOptions(op.GetAllReplicas.Options)
		if err != nil {
			return false, err
		}

		var streamID string
		if op.GetAllReplicas.StreamConfig != nil {
			streamID = op.GetAllReplicas.StreamConfig.StreamId
		}

		e.logger.Logf(logrus.InfoLevel, "Starting GetAllReplicas stream %s", streamID)
		initiated := timestamppb.Now()
		start := time.Now()
		res, err := col.GetAllReplicas(loc.ID(), opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result := &run.Result{
			Initiated:    initiated,
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Result: &run.Result_Stream{
				Stream: &streams.Signal{
					Signal: &streams.Signal_Created{
						Created: &streams.Created{
							StreamId: streamID,
							Type:     streams.Type_STREAM_KV_GET_ALL_REPLICAS,
						},
					},
				},
			},
		}

		// Send the created signal.
		sender.Send(result)

		e.logger.Logf(logrus.InfoLevel, "GetAllReplicas created sent")

		stream := getreplicas.NewGetAllReplicasStream(res, streamID, e.runID, op.GetAllReplicas.ContentAs)

		e.logger.Logf(logrus.InfoLevel, "Adding stream %s to owner", streamID)
		e.streamOwner.Add(streamID, &fitStreams.StreamSender{
			Stream: stream,
			Sender: sender,
		})

		switch op.GetAllReplicas.StreamConfig.StreamWhen.(type) {
		case *streams.Config_OnDemand:
			e.logger.Logf(logrus.InfoLevel, "On demand streaming detected for %s", streamID)

			return true, nil
		case *streams.Config_Automatically:
		default:
			return false, status.Error(codes.Unimplemented, "unknown streamwhen type")
		}

		defer stream.Finish()

		e.logger.Logf(logrus.InfoLevel, "Automatic streaming detected for %s, starting", streamID)
		var sent int
		for {
			streamItem := stream.Next()
			if streamItem == nil {
				e.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", streamID, sent)
				sender.Send(helpers.MakeStreamCompleteResult(streamID))
				return true, nil
			}
			sent++

			item := streamItem.(*getreplicas.GetAllReplicasResultItem) //nolint:errcheck

			content, err := helpers.ParseContentAs(op.GetAllReplicas.ContentAs, func(content interface{}) error {
				getResult := item.GetResult()
				return getResult.Content(&content)
			})
			if err != nil {
				e.logger.Logf(logrus.InfoLevel, "Failed to parse content for stream %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
				continue
			}
			result := &kv.GetReplicaResult{
				IsReplica: item.IsReplica(),
				Cas:       *item.Cas(),
				Content:   content,
				StreamId:  &streamID,
			}

			sender.Send(helpers.MakeGetAllReplicasSuccessResult(result))
		}

	case *sdk.CollectionLevelCommand_QueryIndexManager:
		switch sharedOp := op.QueryIndexManager.Command.(type) {
		case *collectionindexmanager.Command_Shared:
			switch indexOp := sharedOp.Shared.Command.(type) {
			case *manager.Command_CreatePrimaryIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()
				err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					CreatePrimaryIndex(
						&gocb.CreatePrimaryQueryIndexOptions{
							IgnoreIfExists: indexOp.CreatePrimaryIndex.Options.GetIgnoreIfExists(),
							Deferred:       indexOp.CreatePrimaryIndex.Options.GetDeferred(),
							Timeout:        time.Duration(indexOp.CreatePrimaryIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
							ScopeName:      indexOp.CreatePrimaryIndex.Options.GetScopeName(),
							CollectionName: indexOp.CreatePrimaryIndex.Options.GetCollectionName(),
							CustomName:     indexOp.CreatePrimaryIndex.Options.GetIndexName(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_Success{Success: true},
					},
				}

				sender.Send(result)
				return true, nil
			case *manager.Command_CreateIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()
				err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					CreateIndex(
						indexOp.CreateIndex.IndexName,
						indexOp.CreateIndex.Fields,
						&gocb.CreateQueryIndexOptions{
							IgnoreIfExists: indexOp.CreateIndex.Options.GetIgnoreIfExists(),
							Deferred:       indexOp.CreateIndex.Options.GetDeferred(),
							Timeout:        time.Duration(indexOp.CreateIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
							ScopeName:      indexOp.CreateIndex.Options.GetScopeName(),
							CollectionName: indexOp.CreateIndex.Options.GetCollectionName(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_Success{Success: true},
					},
				}

				sender.Send(result)
				return true, nil
			case *manager.Command_GetAllIndexes:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()
				indexes,
					err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					GetAllIndexes(
						&gocb.GetAllQueryIndexesOptions{
							Timeout:        time.Duration(indexOp.GetAllIndexes.Options.GetTimeoutMsecs()) * time.Millisecond,
							ScopeName:      indexOp.GetAllIndexes.Options.GetScopeName(),
							CollectionName: indexOp.GetAllIndexes.Options.GetCollectionName(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()
				if returnResult {
					resIndexes := make([]*manager.QueryIndex, len(indexes))
					for i, index := range indexes {
						resIndexes[i] = queryIndexToProto(index)
					}

					result.Result = &run.Result_Sdk{
						Sdk: &sdk.Result{
							Result: &sdk.Result_QueryIndexes{
								QueryIndexes: &manager.QueryIndexes{
									Indexes: resIndexes,
								},
							},
						},
					}
				} else {
					result.Result = &run.Result_Sdk{
						Sdk: &sdk.Result{
							Result: &sdk.Result_Success{Success: true},
						},
					}
				}

				sender.Send(result)
				return true, nil
			case *manager.Command_DropPrimaryIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()

				err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					DropPrimaryIndex(
						&gocb.DropPrimaryQueryIndexOptions{
							IgnoreIfNotExists: indexOp.DropPrimaryIndex.Options.GetIgnoreIfNotExists(),
							Timeout:           time.Duration(indexOp.DropPrimaryIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
							ScopeName:         indexOp.DropPrimaryIndex.Options.GetScopeName(),
							CollectionName:    indexOp.DropPrimaryIndex.Options.GetCollectionName(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_Success{Success: true},
					},
				}

				sender.Send(result)
				return true, nil
			case *manager.Command_DropIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()

				err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					DropIndex(
						indexOp.DropIndex.IndexName,
						&gocb.DropQueryIndexOptions{
							IgnoreIfNotExists: indexOp.DropIndex.Options.GetIgnoreIfNotExists(),
							Timeout:           time.Duration(indexOp.DropIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
							ScopeName:         indexOp.DropIndex.Options.GetScopeName(),
							CollectionName:    indexOp.DropIndex.Options.GetCollectionName(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_Success{Success: true},
					},
				}

				sender.Send(result)
				return true, nil
			case *manager.Command_BuildDeferredIndexes:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()
				_, err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					BuildDeferredIndexes(
						&gocb.BuildDeferredQueryIndexOptions{
							Timeout:        time.Duration(indexOp.BuildDeferredIndexes.Options.GetTimeoutMsecs()) * time.Millisecond,
							ScopeName:      indexOp.BuildDeferredIndexes.Options.GetScopeName(),
							CollectionName: indexOp.BuildDeferredIndexes.Options.GetCollectionName(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_Success{Success: true},
					},
				}

				sender.Send(result)
				return true, nil
			case *manager.Command_WatchIndexes:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				start := time.Now()
				err := e.conn.Bucket(command.Collection.BucketName).
					Scope(command.Collection.ScopeName).
					Collection(command.Collection.CollectionName).
					QueryIndexes().
					WatchIndexes(
						indexOp.WatchIndexes.IndexNames,
						time.Duration(indexOp.WatchIndexes.GetTimeoutMsecs())*time.Millisecond,
						&gocb.WatchQueryIndexOptions{
							ScopeName:      indexOp.WatchIndexes.Options.GetScopeName(),
							CollectionName: indexOp.WatchIndexes.Options.GetCollectionName(),
							WatchPrimary:   indexOp.WatchIndexes.Options.GetWatchPrimary(),
						},
					)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_Success{Success: true},
					},
				}

				sender.Send(result)
				return true, nil
			default:
				return false, status.Error(codes.Unimplemented, "unknown query index manager command type")
			}
		}

		return true, nil

	case *sdk.CollectionLevelCommand_LookupIn:
		loc, err := helpers.Location(op.LookupIn.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createLookupInOptions(op.LookupIn.Options)
		if err != nil {
			return false, err
		}

		specs, err := createLookupInSpecs(op.LookupIn.Spec)
		if err != nil {
			return false, err
		}

		start := time.Now()

		res, err := col.LookupIn(loc.ID(), specs, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			results := e.parseLookupinResults(op.LookupIn.Spec, res)

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_LookupInResult{
						LookupInResult: &lookupin.LookupInResult{
							Results: results,
						},
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil

	case *sdk.CollectionLevelCommand_LookupInAnyReplica:
		loc, err := helpers.Location(op.LookupInAnyReplica.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createLookupInAnyReplicaOptions(op.LookupInAnyReplica.Options)
		if err != nil {
			return false, err
		}

		specs, err := createLookupInSpecs(op.LookupInAnyReplica.Spec)
		if err != nil {
			return false, err
		}

		start := time.Now()

		res, err := col.LookupInAnyReplica(loc.ID(), specs, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			results := e.parseLookupinResults(op.LookupInAnyReplica.Spec, res)

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_LookupInAnyReplicaResult{
						LookupInAnyReplicaResult: &lookupin.LookupInReplicaResult{
							Results:   results,
							IsReplica: res.IsReplica(),
						},
					},
				},
			}
		} else {
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil
	case *sdk.CollectionLevelCommand_LookupInAllReplicas:
		loc, err := helpers.Location(op.LookupInAllReplicas.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		opts, err := e.createLookupInAllReplicasOptions(op.LookupInAllReplicas.Options)
		if err != nil {
			return false, err
		}

		specs, err := createLookupInSpecs(op.LookupInAllReplicas.Spec)
		if err != nil {
			return false, err
		}

		initiated := timestamppb.Now()
		start := time.Now()

		res, err := col.LookupInAllReplicas(loc.ID(), specs, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		var streamID string
		if op.LookupInAllReplicas.StreamConfig != nil {
			streamID = op.LookupInAllReplicas.StreamConfig.StreamId
		}

		result := &run.Result{
			Initiated:    initiated,
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Result: &run.Result_Stream{
				Stream: &streams.Signal{
					Signal: &streams.Signal_Created{
						Created: &streams.Created{
							StreamId: streamID,
							Type:     streams.Type_STREAM_LOOKUP_IN_ALL_REPLICAS,
						},
					},
				},
			},
		}
		// Send the created signal.
		sender.Send(result)

		e.logger.Logf(logrus.InfoLevel, "LookupInAllReplicas created sent")

		stream := fitLookupIn.NewLookupInAllReplicasStreamStream(res, streamID, e.runID, op.LookupInAllReplicas.Spec)

		e.logger.Logf(logrus.InfoLevel, "Adding stream %s to owner", streamID)
		e.streamOwner.Add(streamID, &fitStreams.StreamSender{
			Stream: stream,
			Sender: sender,
		})

		switch op.LookupInAllReplicas.StreamConfig.StreamWhen.(type) {
		case *streams.Config_OnDemand:
			e.logger.Logf(logrus.InfoLevel, "On demand streaming detected for %s", streamID)

			return true, nil
		case *streams.Config_Automatically:
		default:
			return false, status.Error(codes.Unimplemented, "unknown streamwhen type")
		}

		defer stream.Finish()

		e.logger.Logf(logrus.InfoLevel, "Automatic streaming detected for %s, starting", streamID)
		var sent int
		for {
			next := stream.Next()
			if next == nil {
				e.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", streamID, sent)
				sender.Send(helpers.MakeStreamCompleteResult(streamID))
				return true, nil
			}
			sent++

			item := fitLookupIn.ParseLookupInAllItem(next)

			sender.Send(helpers.MakeLookupInAllReplicasSuccessResult(&lookupin.LookupInAllReplicasResult{
				LookupInReplicaResult: item,
				StreamId:              streamID,
			}))
		}

	default:
		return false, status.Error(codes.Unimplemented, "unknown command type")
	}
	return false, nil
}
