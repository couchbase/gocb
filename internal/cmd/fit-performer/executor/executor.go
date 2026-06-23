package executor

import (
	"errors"
	"time"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/telemetry"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/counter"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
	fitStreams "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/rangescan"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/streams"
	fitRangeScan "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/rangescan"
)

type durabilitySettings struct {
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel gocb.DurabilityLevel
}

type Executor struct {
	conn        *cluster.Connection
	counters    *counter.Counters
	runID       string
	streamOwner *fitStreams.StreamOwner
	spanOwner   *telemetry.SpanOwner
	logger      *logrus.Logger
}

func NewExecutor(conn *cluster.Connection, counters *counter.Counters, runID string, streamOwner *fitStreams.StreamOwner,
	logger *logrus.Logger, spanOwner *telemetry.SpanOwner) *Executor {
	return &Executor{
		conn:        conn,
		counters:    counters,
		runID:       runID,
		streamOwner: streamOwner,
		spanOwner:   spanOwner,
		logger:      logger,
	}
}

func (e *Executor) PerformOperation(command *sdk.Command, sender sender.ResultSender) (bool, error) {
	return e.performOperation(command, sender)
}

func (e *Executor) performOperation(command *sdk.Command, sender sender.ResultSender) (bool, error) {
	switch op := command.Command.(type) {
	case *sdk.Command_Insert:
		return e.sendMutation(op.Insert.Location, op.Insert.Content, command.ReturnResult, sender,
			func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error) {
				opts, err := e.createInsertOptions(op.Insert.Options)
				if err != nil {
					return nil, time.Time{}, err
				}

				start := time.Now()
				res, err := collection.Insert(id, content, opts)
				return res, start, err
			})
	case *sdk.Command_Replace:
		return e.sendMutation(op.Replace.Location, op.Replace.Content, command.ReturnResult, sender,
			func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error) {
				opts, err := e.createReplaceOptions(op.Replace.Options)
				if err != nil {
					return nil, time.Time{}, err
				}

				start := time.Now()
				res, err := collection.Replace(id, content, opts)
				return res, start, err
			})
	case *sdk.Command_Upsert:
		return e.sendMutation(op.Upsert.Location, op.Upsert.Content, command.ReturnResult, sender,
			func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error) {
				opts, err := e.createUpsertOptions(op.Upsert.Options)
				if err != nil {
					return nil, time.Time{}, err
				}

				start := time.Now()
				res, err := collection.Upsert(id, content, opts)
				return res, start, err
			})
	case *sdk.Command_Remove:
		return e.sendMutation(op.Remove.Location, nil, command.ReturnResult, sender,
			func(collection *gocb.Collection, id string, _content interface{}) (*gocb.MutationResult, time.Time, error) {
				opts, err := e.createRemoveOptions(op.Remove.Options)
				if err != nil {
					return nil, time.Time{}, err
				}

				start := time.Now()
				res, err := collection.Remove(id, opts)
				return res, start, err
			})
	case *sdk.Command_Get:
		loc, err := helpers.Location(op.Get.Location, e.counters)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

		result := &run.Result{
			Initiated: timestamppb.Now(),
		}
		opts, err := e.createGetOptions(op.Get.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()

		res, err := col.Get(loc.ID(), opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if command.ReturnResult {
			var expiry *int64
			if !res.ExpiryTime().IsZero() {
				unix := res.ExpiryTime().Unix()
				expiry = &unix
			}
			content, err := helpers.ParseContentAs(op.Get.ContentAs, func(content interface{}) error {
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
	case *sdk.Command_ClusterCommand:
		return e.handleClusterLevelCommand(op.ClusterCommand, sender, command.ReturnResult)
	case *sdk.Command_BucketCommand:
		return e.handleBucketLevelCommand(op.BucketCommand, sender, command.ReturnResult)
	case *sdk.Command_CollectionCommand:
		return e.handleCollectionLevelCommand(op.CollectionCommand, sender, command.ReturnResult)

	case *sdk.Command_RangeScan:
		opts, err := e.createScanOptions(op.RangeScan.Options)
		if err != nil {
			return false, err
		}

		col := e.conn.Collection(op.RangeScan.Collection.BucketName, op.RangeScan.Collection.ScopeName, op.RangeScan.Collection.CollectionName)

		var scanType gocb.ScanType
		switch st := op.RangeScan.ScanType.Type.(type) {
		case *rangescan.ScanType_Range:
			rangeType := gocb.RangeScan{}

			switch stRange := st.Range.Range.(type) {
			case *rangescan.RangeScan_FromTo:
				switch choice := stRange.FromTo.To.Choice.(type) {
				case *rangescan.ScanTermChoice_Minimum:
					min := gocb.ScanTermMinimum()
					rangeType.To = min
				case *rangescan.ScanTermChoice_Maximum:
					max := gocb.ScanTermMaximum()
					rangeType.To = max
				case *rangescan.ScanTermChoice_Term:
					rangeType.To = &gocb.ScanTerm{
						Exclusive: choice.Term.GetExclusive(),
					}
					switch t := choice.Term.Term.(type) {
					case *rangescan.ScanTerm_AsString:
						rangeType.To.Term = t.AsString
					case *rangescan.ScanTerm_AsBytes:
						rangeType.To.Term = string(t.AsBytes) //nolint:staticcheck
					}
				case *rangescan.ScanTermChoice_Default:
					// Do nothing, this will let the SDK automatically populate from and to.
				}

				switch choice := stRange.FromTo.From.Choice.(type) {
				case *rangescan.ScanTermChoice_Minimum:
					min := gocb.ScanTermMinimum()
					rangeType.From = min
				case *rangescan.ScanTermChoice_Maximum:
					max := gocb.ScanTermMaximum()
					rangeType.From = max
				case *rangescan.ScanTermChoice_Term:
					rangeType.From = &gocb.ScanTerm{
						Exclusive: choice.Term.GetExclusive(),
					}
					switch t := choice.Term.Term.(type) {
					case *rangescan.ScanTerm_AsString:
						rangeType.From.Term = t.AsString
					case *rangescan.ScanTerm_AsBytes:
						rangeType.From.Term = string(t.AsBytes) //nolint:staticcheck
					}
				case *rangescan.ScanTermChoice_Default:
					// Do nothing, this will let the SDK automatically populate from and to.
				}
			case *rangescan.RangeScan_DocIdPrefix:
				rangeType = gocb.NewRangeScanForPrefix(stRange.DocIdPrefix)
			}
			scanType = rangeType
		case *rangescan.ScanType_Sampling:
			scanType = gocb.SamplingScan{
				Limit: st.Sampling.Limit,
				Seed:  st.Sampling.GetSeed(),
			}
		default:
			return false, status.Error(codes.Unimplemented, "unsupported range scan type")
		}

		var streamID string
		if op.RangeScan.StreamConfig != nil {
			streamID = op.RangeScan.StreamConfig.StreamId
		}

		e.logger.Logf(logrus.InfoLevel, "Starting range scan %s", streamID)
		initiated := timestamppb.Now()
		start := time.Now()
		scan, err := col.Scan(scanType, opts)
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
							Type:     streams.Type_STREAM_KV_RANGE_SCAN,
						},
					},
				},
			},
		}
		// Send the created signal.
		sender.Send(result)

		e.logger.Logf(logrus.InfoLevel, "Range scan created sent")

		stream := fitRangeScan.NewRangeScanStream(scan, streamID, e.runID, &fitRangeScan.RangeScanOptions{
			IDsOnly:   op.RangeScan.Options.GetIdsOnly(),
			ContentAs: op.RangeScan.ContentAs,
		})

		e.logger.Logf(logrus.InfoLevel, "Adding stream %s to owner", streamID)
		e.streamOwner.Add(streamID, &fitStreams.StreamSender{
			Stream: stream,
			Sender: sender,
		})

		switch op.RangeScan.StreamConfig.StreamWhen.(type) {
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
				if err := scan.Err(); err == nil {
					sender.Send(helpers.MakeStreamCompleteResult(streamID))
				} else {
					e.logger.Logf(logrus.InfoLevel, "Error detected on stream end for %s: %s", streamID, err)
					sender.Send(helpers.MakeStreamErrorResult(streamID, err))
				}
				return true, nil
			}
			sent++

			item := streamItem.(*fitRangeScan.RangeScanResultItem) //nolint:errcheck

			var content *shared.ContentTypes
			if op.RangeScan.ContentAs != nil {
				content, err = item.Content()
				if err != nil {
					e.logger.Logf(logrus.InfoLevel, "Failed to parse content for stream %s: %s", streamID, err)
					sender.Send(helpers.MakeStreamErrorResult(streamID, err))
					continue
				}
			}
			result := &rangescan.ScanResult{
				Id:         item.ID(),
				StreamId:   streamID,
				Cas:        item.Cas(),
				ExpiryTime: item.ExpiryTime(),
				Content:    content,
				IdOnly:     item.IDOnly(),
			}

			sender.Send(helpers.MakeRangeScanSuccessResult(result))
		}

	case *sdk.Command_ScopeCommand:
		return e.handleScopeLevelCommand(op.ScopeCommand, sender, command.ReturnResult)
	default:
		return false, status.Error(codes.Unimplemented, "unknown command type")
	}
}

type mutationFn func(collection *gocb.Collection, id string, content interface{}) (*gocb.MutationResult, time.Time, error)

type counterFn func(collection *gocb.Collection, id string, content interface{}) (*gocb.CounterResult, time.Time, error)

func (e *Executor) sendSDKError(err error, sender sender.ResultSender) {
	sender.Send(&run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_Exception{
					Exception: helpers.MapErrorToProto(err),
				},
			},
		},
		Initiated: timestamppb.Now(),
	})
}

func (e *Executor) sendMutation(docLocation *shared.DocLocation, sharedContent *shared.Content, returnResult bool,
	sender sender.ResultSender, opFn mutationFn) (bool, error) {

	loc, err := helpers.Location(docLocation, e.counters)
	if err != nil {
		return false, err
	}

	col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

	var contentConverted interface{}
	if sharedContent != nil {
		var err error
		contentConverted, err = helpers.ContentFromShared(sharedContent)
		if err != nil {
			return false, err
		}
	}

	result := &run.Result{
		Initiated: timestamppb.Now(),
	}
	res, start, err := opFn(col, loc.ID(), contentConverted)
	if err != nil {
		e.sendSDKError(err, sender)
		return false, nil
	}

	result.ElapsedNanos = time.Since(start).Nanoseconds()
	if returnResult {
		result.Result = &run.Result_Sdk{
			Sdk: makeSdkResult(res),
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
}

func (e *Executor) sendCounter(docLocation *shared.DocLocation, sharedContent *shared.Content, returnResult bool,
	sender sender.ResultSender, opFn counterFn) (bool, error) {

	loc, err := helpers.Location(docLocation, e.counters)
	if err != nil {
		return false, err
	}

	col := e.conn.Collection(loc.Bucket(), loc.Scope(), loc.Collection())

	var contentConverted interface{}
	if sharedContent != nil {
		var err error
		contentConverted, err = helpers.ContentFromShared(sharedContent)
		if err != nil {
			return false, err
		}
	}

	result := &run.Result{
		Initiated: timestamppb.Now(),
	}
	res, start, err := opFn(col, loc.ID(), contentConverted)
	if err != nil {
		e.sendSDKError(err, sender)
		return false, nil
	}

	result.ElapsedNanos = time.Since(start).Nanoseconds()
	if returnResult {
		content := res.Content()

		sdkResult := &kv.CounterResult{
			Cas:     int64(res.Cas()),
			Content: int64(content),
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

		result.Result = &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_CounterResult{
					CounterResult: sdkResult,
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
}

func makeSdkResult(res *gocb.MutationResult) *sdk.Result {
	sdkResult := &kv.MutationResult{
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

	return &sdk.Result{
		Result: &sdk.Result_MutationResult{
			MutationResult: sdkResult,
		},
	}
}

func (e *Executor) makeSuccessResult() *run.Result_Sdk {
	return &run.Result_Sdk{
		Sdk: &sdk.Result{
			Result: &sdk.Result_Success{Success: true},
		},
	}
}

func durabilityLevel(t *shared.DurabilityType) (*durabilitySettings, error) {
	switch typ := t.Durability.(type) {
	case *shared.DurabilityType_DurabilityLevel:
		return &durabilitySettings{
			DurabilityLevel: helpers.ProtocolDuraToSDK(typ.DurabilityLevel),
		}, nil
	case *shared.DurabilityType_Observe:
		return &durabilitySettings{
			PersistTo:   uint(typ.Observe.PersistTo),
			ReplicateTo: uint(typ.Observe.ReplicateTo),
		}, nil
	default:
		return nil, errors.New("unknown durability level type")
	}
}
