package executor

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/streams"
	fitSearch "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/search"
	fitStreams "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"
	"github.com/sirupsen/logrus"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/waituntilready"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/bucketmanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/query/indexmanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/collection/mutatein"
	queryManager "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/query/index/manager"
	searchManager "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search/index-manager"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
)

func (e *Executor) handleClusterLevelCommand(command *sdk.ClusterLevelCommand, sender sender.ResultSender, returnResult bool) (bool, error) {
	switch op := command.Command.(type) {
	case *sdk.ClusterLevelCommand_Authenticator:
		result := &run.Result{
			Initiated: timestamppb.Now(),
		}

		var auth gocb.Authenticator
		switch a := op.Authenticator.Authenticator.(type) {
		case *shared.Authenticator_PasswordAuth:
			auth = &gocb.PasswordAuthenticator{
				Username: a.PasswordAuth.Username,
				Password: a.PasswordAuth.Password,
			}
		case *shared.Authenticator_CertificateAuth:
			cert, err := tls.X509KeyPair([]byte(a.CertificateAuth.Cert), []byte(a.CertificateAuth.Key))
			if err != nil {
				e.logger.Warnf("Error reading client cert: %v", err)
				return false, status.Errorf(codes.Aborted, "unexpected error reading client cert: %v", err)
			}

			auth = &gocb.CertificateAuthenticator{
				ClientCertificate: &cert,
			}
		case *shared.Authenticator_JwtAuth:
			auth = &gocb.JWTAuthenticator{
				Token: a.JwtAuth.Jwt,
			}
		}

		start := time.Now()
		err := e.conn.Cluster().SetAuthenticator(gocb.SetAuthenticatorOptions{
			Authenticator: auth,
		})
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
	case *sdk.ClusterLevelCommand_QueryIndexManager:
		switch sharedOp := op.QueryIndexManager.Command.(type) {
		case *indexmanager.Command_Shared:
			switch indexOp := sharedOp.Shared.Command.(type) {
			case *queryManager.Command_CreatePrimaryIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.CreatePrimaryQueryIndexOptions{
					IgnoreIfExists: indexOp.CreatePrimaryIndex.Options.GetIgnoreIfExists(),
					Deferred:       indexOp.CreatePrimaryIndex.Options.GetDeferred(),
					Timeout:        time.Duration(indexOp.CreatePrimaryIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
					CustomName:     indexOp.CreatePrimaryIndex.Options.GetIndexName(),
				}

				if indexOp.CreatePrimaryIndex.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.CreatePrimaryIndex.Options.GetParentSpanId())
					if !ok {
						return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", indexOp.CreatePrimaryIndex.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.CreatePrimaryIndex.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.CreatePrimaryIndex.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				err := e.conn.Cluster().QueryIndexes().CreatePrimaryIndex(
					op.QueryIndexManager.BucketName,
					opts,
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
			case *queryManager.Command_CreateIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.CreateQueryIndexOptions{
					IgnoreIfExists: indexOp.CreateIndex.Options.GetIgnoreIfExists(),
					Deferred:       indexOp.CreateIndex.Options.GetDeferred(),
					Timeout:        time.Duration(indexOp.CreateIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
				}

				if indexOp.CreateIndex.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.CreateIndex.Options.GetParentSpanId())
					if !ok {
						return false, fmt.Errorf("unknown parent span id: %s", indexOp.CreateIndex.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.CreateIndex.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.CreateIndex.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				err := e.conn.Cluster().QueryIndexes().CreateIndex(
					op.QueryIndexManager.BucketName,
					indexOp.CreateIndex.IndexName,
					indexOp.CreateIndex.Fields,
					opts,
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
			case *queryManager.Command_GetAllIndexes:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.GetAllQueryIndexesOptions{
					Timeout: time.Duration(indexOp.GetAllIndexes.Options.GetTimeoutMsecs()) * time.Millisecond,
				}

				if indexOp.GetAllIndexes.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.GetAllIndexes.Options.GetParentSpanId())
					if !ok {
						return false, fmt.Errorf("unknown parent span id: %s", indexOp.GetAllIndexes.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.GetAllIndexes.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.GetAllIndexes.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				indexes, err := e.conn.Cluster().QueryIndexes().GetAllIndexes(
					op.QueryIndexManager.BucketName,
					opts,
				)
				if err != nil {
					e.sendSDKError(err, sender)
					return false, nil
				}

				result.ElapsedNanos = time.Since(start).Nanoseconds()
				if returnResult {
					resIndexes := make([]*queryManager.QueryIndex, len(indexes))
					for i, index := range indexes {
						resIndexes[i] = queryIndexToProto(index)
					}

					result.Result = &run.Result_Sdk{
						Sdk: &sdk.Result{
							Result: &sdk.Result_QueryIndexes{
								QueryIndexes: &queryManager.QueryIndexes{
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
			case *queryManager.Command_DropPrimaryIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.DropPrimaryQueryIndexOptions{
					IgnoreIfNotExists: indexOp.DropPrimaryIndex.Options.GetIgnoreIfNotExists(),
					Timeout:           time.Duration(indexOp.DropPrimaryIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
				}

				if indexOp.DropPrimaryIndex.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.DropPrimaryIndex.Options.GetParentSpanId())
					if !ok {
						return false, fmt.Errorf("unknown parent span id: %s", indexOp.DropPrimaryIndex.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.DropPrimaryIndex.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.DropPrimaryIndex.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				err := e.conn.Cluster().QueryIndexes().DropPrimaryIndex(
					op.QueryIndexManager.BucketName,
					opts,
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
			case *queryManager.Command_DropIndex:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.DropQueryIndexOptions{
					IgnoreIfNotExists: indexOp.DropIndex.Options.GetIgnoreIfNotExists(),
					Timeout:           time.Duration(indexOp.DropIndex.Options.GetTimeoutMsecs()) * time.Millisecond,
				}

				if indexOp.DropIndex.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.DropIndex.Options.GetParentSpanId())
					if !ok {
						return false, fmt.Errorf("unknown parent span id: %s", indexOp.DropIndex.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.DropIndex.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.DropIndex.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				err := e.conn.Cluster().QueryIndexes().DropIndex(
					op.QueryIndexManager.BucketName,
					indexOp.DropIndex.IndexName,
					opts,
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
			case *queryManager.Command_BuildDeferredIndexes:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.BuildDeferredQueryIndexOptions{
					Timeout: time.Duration(indexOp.BuildDeferredIndexes.Options.GetTimeoutMsecs()) * time.Millisecond,
				}

				if indexOp.BuildDeferredIndexes.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.BuildDeferredIndexes.Options.GetParentSpanId())
					if !ok {
						return false, fmt.Errorf("unknown parent span id: %s", indexOp.BuildDeferredIndexes.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.BuildDeferredIndexes.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.BuildDeferredIndexes.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				_, err := e.conn.Cluster().QueryIndexes().BuildDeferredIndexes(
					op.QueryIndexManager.BucketName,
					opts,
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
			case *queryManager.Command_WatchIndexes:
				result := &run.Result{
					Initiated: timestamppb.Now(),
				}

				opts := &gocb.WatchQueryIndexOptions{
					WatchPrimary: indexOp.WatchIndexes.Options.GetWatchPrimary(),
				}

				if indexOp.WatchIndexes.Options.GetParentSpanId() != "" {
					parent, ok := e.spanOwner.GetSpan(indexOp.WatchIndexes.Options.GetParentSpanId())
					if !ok {
						return false, fmt.Errorf("unknown parent span id: %s", indexOp.WatchIndexes.Options.GetParentSpanId())
					}
					opts.ParentSpan = parent
				}

				opts.ScopeName = indexOp.WatchIndexes.Options.GetScopeName()           //nolint:staticcheck
				opts.CollectionName = indexOp.WatchIndexes.Options.GetCollectionName() //nolint:staticcheck

				start := time.Now()
				err := e.conn.Cluster().QueryIndexes().WatchIndexes(
					op.QueryIndexManager.BucketName,
					indexOp.WatchIndexes.IndexNames,
					time.Duration(indexOp.WatchIndexes.GetTimeoutMsecs())*time.Millisecond,
					opts,
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
	case *sdk.ClusterLevelCommand_Query:
		result := &run.Result{
			Initiated: timestamppb.Now(),
		}

		opts, err := e.createQueryOptions(op.Query.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()
		res, err := e.conn.Cluster().Query(op.Query.Statement, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			queryRes, err := parseQueryResult(op.Query.ContentAs, res)
			if err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_QueryResult{
						QueryResult: queryRes,
					},
				},
			}
		} else {
			for res.Next() {
			}
			if err := res.Close(); err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}
			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil
	case *sdk.ClusterLevelCommand_BucketManager:
		switch sharedOp := op.BucketManager.Command.(type) {
		case *bucketmanager.Command_CreateBucket:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			settings := gocb.CreateBucketSettings{
				BucketSettings: gocb.BucketSettings{
					Name:       sharedOp.CreateBucket.Settings.Settings.Name,
					RAMQuotaMB: uint64(sharedOp.CreateBucket.Settings.Settings.RamQuota_MB),
					// BucketType is a required field in Go.
					BucketType: toGocbBucketType(sharedOp.CreateBucket.Settings.Settings.GetBucketType()),
				},
			}
			if sharedOp.CreateBucket.Settings.ConflictResolutionType != nil {
				settings.ConflictResolutionType = toGocbConflictResolutionTyoe(sharedOp.CreateBucket.Settings.GetConflictResolutionType())
			}
			if sharedOp.CreateBucket.Settings.Settings.FlushEnabled != nil {
				settings.FlushEnabled = sharedOp.CreateBucket.Settings.Settings.GetFlushEnabled()
			}
			if sharedOp.CreateBucket.Settings.Settings.ReplicaIndexes == nil {
				settings.ReplicaIndexDisabled = true
			} else {
				settings.ReplicaIndexDisabled = !sharedOp.CreateBucket.Settings.Settings.GetReplicaIndexes()
			}
			if sharedOp.CreateBucket.Settings.Settings.NumReplicas != nil {
				settings.NumReplicas = uint32(sharedOp.CreateBucket.Settings.Settings.GetNumReplicas())
			}
			if sharedOp.CreateBucket.Settings.Settings.EvictionPolicy != nil {
				settings.EvictionPolicy = toGocbEvictionPolicy(sharedOp.CreateBucket.Settings.Settings.GetEvictionPolicy())
			}
			if sharedOp.CreateBucket.Settings.Settings.MaxExpirySeconds != nil {
				settings.MaxExpiry = time.Duration(sharedOp.CreateBucket.Settings.Settings.GetMaxExpirySeconds()) * time.Second
			}
			if sharedOp.CreateBucket.Settings.Settings.CompressionMode != nil {
				settings.CompressionMode = toGocbCompressionMode(sharedOp.CreateBucket.Settings.Settings.GetCompressionMode())
			}
			if sharedOp.CreateBucket.Settings.Settings.MinimumDurabilityLevel != nil {
				settings.MinimumDurabilityLevel = helpers.ProtocolDuraToSDK(sharedOp.CreateBucket.Settings.Settings.GetMinimumDurabilityLevel())
			}

			if sharedOp.CreateBucket.Settings.Settings.EvictionPolicy != nil {
				settings.EvictionPolicy = toGocbEvictionPolicy(sharedOp.CreateBucket.Settings.Settings.GetEvictionPolicy())
			}

			if sharedOp.CreateBucket.Settings.Settings.StorageBackend != nil {
				settings.StorageBackend = toGocbStorageBacked(sharedOp.CreateBucket.Settings.Settings.GetStorageBackend())
			}

			if sharedOp.CreateBucket.Settings.Settings.HistoryRetentionCollectionDefault != nil {
				if *sharedOp.CreateBucket.Settings.Settings.HistoryRetentionCollectionDefault {
					settings.HistoryRetentionCollectionDefault = gocb.HistoryRetentionCollectionDefaultEnabled
				} else {
					settings.HistoryRetentionCollectionDefault = gocb.HistoryRetentionCollectionDefaultDisabled
				}
			}

			settings.HistoryRetentionDuration = time.Duration(sharedOp.CreateBucket.Settings.Settings.GetHistoryRetentionSeconds()) * time.Second
			settings.HistoryRetentionBytes = sharedOp.CreateBucket.Settings.Settings.GetHistoryRetentionBytes()

			settings.NumVBuckets = uint16(sharedOp.CreateBucket.Settings.Settings.GetNumVbuckets())

			opts := &gocb.CreateBucketOptions{
				Timeout: time.Duration(sharedOp.CreateBucket.Options.GetTimeoutMsecs()) * time.Millisecond,
			}
			if sharedOp.CreateBucket.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*sharedOp.CreateBucket.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *sharedOp.CreateBucket.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			err := e.conn.Cluster().Buckets().CreateBucket(
				settings,
				opts,
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
		case *bucketmanager.Command_UpdateBucket:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			settings := gocb.BucketSettings{
				Name:       sharedOp.UpdateBucket.Settings.Name,
				RAMQuotaMB: uint64(sharedOp.UpdateBucket.Settings.RamQuota_MB),
				// BucketType is a required field in Go.
				BucketType: toGocbBucketType(sharedOp.UpdateBucket.Settings.GetBucketType()),
			}

			if sharedOp.UpdateBucket.Settings.FlushEnabled != nil {
				settings.FlushEnabled = sharedOp.UpdateBucket.Settings.GetFlushEnabled()
			}
			if sharedOp.UpdateBucket.Settings.ReplicaIndexes == nil {
				settings.ReplicaIndexDisabled = true
			} else {
				settings.ReplicaIndexDisabled = !sharedOp.UpdateBucket.Settings.GetReplicaIndexes()
			}
			if sharedOp.UpdateBucket.Settings.NumReplicas != nil {
				settings.NumReplicas = uint32(sharedOp.UpdateBucket.Settings.GetNumReplicas())
			}
			if sharedOp.UpdateBucket.Settings.EvictionPolicy != nil {
				settings.EvictionPolicy = toGocbEvictionPolicy(sharedOp.UpdateBucket.Settings.GetEvictionPolicy())
			}
			if sharedOp.UpdateBucket.Settings.MaxExpirySeconds != nil {
				settings.MaxExpiry = time.Duration(sharedOp.UpdateBucket.Settings.GetMaxExpirySeconds()) * time.Second
			}
			if sharedOp.UpdateBucket.Settings.CompressionMode != nil {
				settings.CompressionMode = toGocbCompressionMode(sharedOp.UpdateBucket.Settings.GetCompressionMode())
			}
			if sharedOp.UpdateBucket.Settings.MinimumDurabilityLevel != nil {
				settings.MinimumDurabilityLevel = helpers.ProtocolDuraToSDK(sharedOp.UpdateBucket.Settings.GetMinimumDurabilityLevel())
			}
			if sharedOp.UpdateBucket.Settings.StorageBackend != nil {
				settings.StorageBackend = toGocbStorageBacked(sharedOp.UpdateBucket.Settings.GetStorageBackend())
			}
			if sharedOp.UpdateBucket.Settings.HistoryRetentionCollectionDefault != nil {
				if *sharedOp.UpdateBucket.Settings.HistoryRetentionCollectionDefault {
					settings.HistoryRetentionCollectionDefault = gocb.HistoryRetentionCollectionDefaultEnabled
				} else {
					settings.HistoryRetentionCollectionDefault = gocb.HistoryRetentionCollectionDefaultDisabled
				}
			}

			settings.HistoryRetentionDuration = time.Duration(sharedOp.UpdateBucket.Settings.GetHistoryRetentionSeconds()) * time.Second
			settings.HistoryRetentionBytes = sharedOp.UpdateBucket.Settings.GetHistoryRetentionBytes()
			settings.NumVBuckets = uint16(sharedOp.UpdateBucket.Settings.GetNumVbuckets())

			opts := &gocb.UpdateBucketOptions{
				Timeout: time.Duration(sharedOp.UpdateBucket.Options.GetTimeoutMsecs()) * time.Millisecond,
			}
			if sharedOp.UpdateBucket.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*sharedOp.UpdateBucket.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *sharedOp.UpdateBucket.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			err := e.conn.Cluster().Buckets().UpdateBucket(
				settings,
				opts,
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
		case *bucketmanager.Command_DropBucket:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.DropBucketOptions{
				Timeout: time.Duration(sharedOp.DropBucket.Options.GetTimeoutMsecs()) * time.Millisecond,
			}
			if sharedOp.DropBucket.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*sharedOp.DropBucket.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *sharedOp.DropBucket.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			err := e.conn.Cluster().Buckets().DropBucket(
				sharedOp.DropBucket.BucketName,
				opts,
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
		case *bucketmanager.Command_FlushBucket:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}
			opts := &gocb.FlushBucketOptions{
				Timeout: time.Duration(sharedOp.FlushBucket.Options.GetTimeoutMsecs()) * time.Millisecond,
			}
			if sharedOp.FlushBucket.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*sharedOp.FlushBucket.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *sharedOp.FlushBucket.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			err := e.conn.Cluster().Buckets().FlushBucket(
				sharedOp.FlushBucket.BucketName,
				opts,
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
		case *bucketmanager.Command_GetBucket:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.GetBucketOptions{
				Timeout: time.Duration(sharedOp.GetBucket.Options.GetTimeoutMsecs()) * time.Millisecond,
			}
			if sharedOp.GetBucket.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*sharedOp.GetBucket.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *sharedOp.GetBucket.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			settings, err := e.conn.Cluster().Buckets().GetBucket(
				sharedOp.GetBucket.GetBucketName(),
				opts,
			)
			if err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.ElapsedNanos = time.Since(start).Nanoseconds()

			if returnResult {
				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_BucketManagerResult{
							BucketManagerResult: &bucketmanager.Result{
								Result: &bucketmanager.Result_BucketSettings{
									BucketSettings: parseGocbBucketSettings(settings),
								},
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
		case *bucketmanager.Command_GetAllBuckets:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.GetAllBucketsOptions{
				Timeout: time.Duration(sharedOp.GetAllBuckets.Options.GetTimeoutMsecs()) * time.Millisecond,
			}
			if sharedOp.GetAllBuckets.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*sharedOp.GetAllBuckets.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *sharedOp.GetAllBuckets.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			buckets, err := e.conn.Cluster().Buckets().GetAllBuckets(
				opts,
			)
			if err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.ElapsedNanos = time.Since(start).Nanoseconds()

			if returnResult {
				resBuckets := make(map[string]*bucketmanager.BucketSettings, len(buckets))
				for name, b := range buckets {
					resBuckets[name] = parseGocbBucketSettings(&b)
				}

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_BucketManagerResult{
							BucketManagerResult: &bucketmanager.Result{
								Result: &bucketmanager.Result_GetAllBucketsResult{
									GetAllBucketsResult: &bucketmanager.GetAllBucketsResult{
										Result: resBuckets,
									},
								},
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
		}

		return true, nil
	case *sdk.ClusterLevelCommand_Search:
		return e.handleClusterSearch(op, sender, returnResult)
	case *sdk.ClusterLevelCommand_SearchIndexManager:
		return e.handleClusterSearchIndexManager(op, sender, returnResult)
	case *sdk.ClusterLevelCommand_SearchV2:
		return e.handleClusterSearchV2(op, sender, returnResult)
	case *sdk.ClusterLevelCommand_WaitUntilReady:
		result := &run.Result{
			Initiated: timestamppb.Now(),
		}

		var opts *gocb.WaitUntilReadyOptions
		if op.WaitUntilReady.Options != nil {
			opts = &gocb.WaitUntilReadyOptions{}
			opOpts := op.WaitUntilReady.Options
			var serviceTypes []gocb.ServiceType
			for _, serviceType := range opOpts.ServiceTypes {
				st, err := protoServiceTypeToGocb(serviceType)
				if err != nil {
					return false, err
				}

				serviceTypes = append(serviceTypes, st)
			}
			opts.ServiceTypes = serviceTypes

			if opOpts.DesiredState != nil {
				switch *opOpts.DesiredState {
				case waituntilready.ClusterState_DEGRADED:
					opts.DesiredState = gocb.ClusterStateDegraded
				case waituntilready.ClusterState_OFFLINE:
					opts.DesiredState = gocb.ClusterStateOffline
				case waituntilready.ClusterState_ONLINE:
					opts.DesiredState = gocb.ClusterStateOnline
				default:
					e.sendSDKError(status.Error(codes.Unimplemented, "unknown desired state"), sender)
				}
			}
		}

		start := time.Now()
		err := e.conn.Cluster().WaitUntilReady(time.Duration(op.WaitUntilReady.TimeoutMillis)*time.Millisecond, opts)
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
		return false, status.Error(codes.Unimplemented, "unknown command type")
	}
}

func (e *Executor) handleClusterSearchV2(op *sdk.ClusterLevelCommand_SearchV2, sender sender.ResultSender, returnResult bool) (bool, error) {
	initiated := timestamppb.Now()

	searchRequest, err := e.parseSearchRequest(op.SearchV2.Search.Request)
	if err != nil {
		return false, err
	}
	opts, err := e.parseSearchOptions(op.SearchV2.Search.Options)
	if err != nil {
		return false, err
	}

	start := time.Now()
	res, err := e.conn.Cluster().Search(op.SearchV2.Search.IndexName, *searchRequest, opts)
	if err != nil {
		e.sendSDKError(err, sender)
		return false, nil
	}

	result := &run.Result{
		Initiated:    initiated,
		ElapsedNanos: time.Since(start).Nanoseconds(),
	}

	return e.handleSearchResult(res, result, op.SearchV2.StreamConfig, op.SearchV2.FieldsAs, sender, returnResult)
}

func (e *Executor) handleClusterSearch(op *sdk.ClusterLevelCommand_Search, sender sender.ResultSender, returnResult bool) (bool, error) {
	initiated := timestamppb.Now()

	query, err := e.parseSearchQuery(op.Search.Query)
	if err != nil {
		return false, err
	}

	opts, err := e.parseSearchOptions(op.Search.Options)
	if err != nil {
		return false, err
	}

	start := time.Now()
	res, err := e.conn.Cluster().SearchQuery(op.Search.IndexName, query, opts)
	if err != nil {
		e.sendSDKError(err, sender)
		return false, nil
	}

	result := &run.Result{
		Initiated:    initiated,
		ElapsedNanos: time.Since(start).Nanoseconds(),
	}

	return e.handleSearchResult(res, result, op.Search.StreamConfig, op.Search.FieldsAs, sender, returnResult)
}

func (e *Executor) handleSearchResult(res *gocb.SearchResult, result *run.Result, streamConfig *streams.Config,
	fieldContentAs *shared.ContentAs, sender sender.ResultSender, returnResult bool) (bool, error) {
	if streamConfig == nil {
		if returnResult {
			searchRes, err := e.parseSearchResult(fieldContentAs, res)
			if err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_SearchBlockingResult{
						SearchBlockingResult: searchRes,
					},
				},
			}
		} else {
			for res.Next() {
			}
			if err := res.Close(); err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_Success{Success: true},
				},
			}
		}

		sender.Send(result)
		return true, nil
	}

	streamID := streamConfig.StreamId
	e.logger.Logf(logrus.InfoLevel, "Starting search streaming %s", streamID)

	result.Result = &run.Result_Stream{
		Stream: &streams.Signal{
			Signal: &streams.Signal_Created{
				Created: &streams.Created{
					StreamId: streamID,
					Type:     streams.Type_STREAM_FULL_TEXT_SEARCH,
				},
			},
		},
	}
	// Send the created signal.
	sender.Send(result)

	e.logger.Logf(logrus.InfoLevel, "Search stream created sent")

	stream := fitSearch.NewSearchStream(res, streamID, e.runID, &fitSearch.SearchStreamOptions{
		FieldContentAs: fieldContentAs,
	})

	e.logger.Logf(logrus.InfoLevel, "Adding stream %s to owner", streamID)
	e.streamOwner.Add(streamID, &fitStreams.StreamSender{
		Stream: stream,
		Sender: sender,
	})

	switch streamConfig.StreamWhen.(type) {
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
			meta, err := stream.Metadata()
			if err != nil {
				e.logger.Logf(logrus.InfoLevel, "Error detected on metadata call for %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
				return true, nil
			}
			facets, err := stream.Facets()
			if err != nil {
				e.logger.Logf(logrus.InfoLevel, "Error detected on facets call for %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
				return true, nil
			}

			sender.Send(helpers.MakeSearchStreamMetaResult(streamID, meta))
			sender.Send(helpers.MakeSearchStreamFacetsResult(streamID, facets))

			e.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", streamID, sent)
			if err := stream.Err(); err == nil {
				sender.Send(helpers.MakeStreamCompleteResult(streamID))
			} else {
				e.logger.Logf(logrus.InfoLevel, "Error detected on stream end for %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
			}
			return true, nil
		}
		sent++

		item := streamItem.(*fitSearch.SearchStreamItem) //nolint:errcheck

		row, err := item.Row()
		if err != nil {
			e.logger.Logf(logrus.InfoLevel, "Failed to parse row for stream %s: %s", streamID, err)
			sender.Send(helpers.MakeStreamErrorResult(streamID, err))
			continue
		}

		sender.Send(helpers.MakeSearchStreamRowResult(streamID, row))
	}
}

func toGocbStoreSemantic(storeSemantic mutatein.StoreSemantics) gocb.StoreSemantics {
	switch storeSemantic {
	case mutatein.StoreSemantics_INSERT:
		return gocb.StoreSemanticsInsert
	case mutatein.StoreSemantics_REPLACE:
		return gocb.StoreSemanticsReplace
	case mutatein.StoreSemantics_UPSERT:
		return gocb.StoreSemanticsUpsert
	}
	return gocb.StoreSemanticsReplace
}

func toGocbBucketType(bucketType bucketmanager.BucketType) gocb.BucketType {
	switch bucketType {
	case bucketmanager.BucketType_COUCHBASE:
		return gocb.CouchbaseBucketType
	case bucketmanager.BucketType_EPHEMERAL:
		return gocb.EphemeralBucketType
	case bucketmanager.BucketType_MEMCACHED:
		return gocb.MemcachedBucketType
	}

	return ""
}

func toGocbEvictionPolicy(policy bucketmanager.EvictionPolicyType) gocb.EvictionPolicyType {
	switch policy {
	case bucketmanager.EvictionPolicyType_NO_EVICTION:
		return gocb.EvictionPolicyTypeNoEviction
	case bucketmanager.EvictionPolicyType_FULL:
		return gocb.EvictionPolicyTypeFull
	case bucketmanager.EvictionPolicyType_NOT_RECENTLY_USED:
		return gocb.EvictionPolicyTypeNotRecentlyUsed
	case bucketmanager.EvictionPolicyType_VALUE_ONLY:
		return gocb.EvictionPolicyTypeValueOnly
	}

	return ""
}

func toGocbCompressionMode(mode bucketmanager.CompressionMode) gocb.CompressionMode {
	switch mode {
	case bucketmanager.CompressionMode_ACTIVE:
		return gocb.CompressionModeActive
	case bucketmanager.CompressionMode_OFF:
		return gocb.CompressionModeOff
	case bucketmanager.CompressionMode_PASSIVE:
		return gocb.CompressionModePassive
	}

	return ""
}

func toGocbStorageBacked(backend bucketmanager.StorageBackend) gocb.StorageBackend {
	switch backend {
	case bucketmanager.StorageBackend_COUCHSTORE:
		return gocb.StorageBackendCouchstore
	case bucketmanager.StorageBackend_MAGMA:
		return gocb.StorageBackendMagma
	}

	return ""
}

func toGocbConflictResolutionTyoe(conflict bucketmanager.ConflictResolutionType) gocb.ConflictResolutionType {
	switch conflict {
	case bucketmanager.ConflictResolutionType_TIMESTAMP:
		return gocb.ConflictResolutionTypeTimestamp
	case bucketmanager.ConflictResolutionType_SEQUENCE_NUMBER:
		return gocb.ConflictResolutionTypeSequenceNumber

	case bucketmanager.ConflictResolutionType_CUSTOM:
		return gocb.ConflictResolutionTypeCustom

	}

	return ""
}

func fromGocbBucketType(bucketType gocb.BucketType) bucketmanager.BucketType {
	switch bucketType {
	case gocb.CouchbaseBucketType:
		return bucketmanager.BucketType_COUCHBASE
	case gocb.EphemeralBucketType:
		return bucketmanager.BucketType_EPHEMERAL
	case gocb.MemcachedBucketType:
		return bucketmanager.BucketType_MEMCACHED
	}

	return -1
}

func fromGocbEvictionPolicy(policy gocb.EvictionPolicyType) bucketmanager.EvictionPolicyType {
	switch policy {
	case gocb.EvictionPolicyTypeNoEviction:
		return bucketmanager.EvictionPolicyType_NO_EVICTION
	case gocb.EvictionPolicyTypeFull:
		return bucketmanager.EvictionPolicyType_FULL
	case gocb.EvictionPolicyTypeNotRecentlyUsed:
		return bucketmanager.EvictionPolicyType_NOT_RECENTLY_USED
	case gocb.EvictionPolicyTypeValueOnly:
		return bucketmanager.EvictionPolicyType_VALUE_ONLY
	}

	return -1
}

func fromGocbCompressionMode(mode gocb.CompressionMode) bucketmanager.CompressionMode {
	switch mode {
	case gocb.CompressionModeActive:
		return bucketmanager.CompressionMode_ACTIVE
	case gocb.CompressionModeOff:
		return bucketmanager.CompressionMode_OFF
	case gocb.CompressionModePassive:
		return bucketmanager.CompressionMode_PASSIVE
	}

	return -1
}

func fromGocbStorageBacked(backend gocb.StorageBackend) bucketmanager.StorageBackend {
	switch backend {
	case gocb.StorageBackendCouchstore:
		return bucketmanager.StorageBackend_COUCHSTORE
	case gocb.StorageBackendMagma:
		return bucketmanager.StorageBackend_MAGMA
	}

	return -1
}

func parseGocbBucketSettings(settings *gocb.BucketSettings) *bucketmanager.BucketSettings {
	protoSettings := &bucketmanager.BucketSettings{
		Name:                              settings.Name,
		FlushEnabled:                      &settings.FlushEnabled,
		RamQuota_MB:                       int64(settings.RAMQuotaMB),
		HistoryRetentionCollectionDefault: nil,
		HistoryRetentionSeconds:           nil,
		HistoryRetentionBytes:             nil,
	}

	replicas := int32(settings.NumReplicas)
	protoSettings.NumReplicas = &replicas
	replicaIndexes := !settings.ReplicaIndexDisabled
	protoSettings.ReplicaIndexes = &replicaIndexes
	bucketType := fromGocbBucketType(settings.BucketType)
	protoSettings.BucketType = &bucketType
	evictionPolicy := fromGocbEvictionPolicy(settings.EvictionPolicy)
	protoSettings.EvictionPolicy = &evictionPolicy
	compression := fromGocbCompressionMode(settings.CompressionMode)
	protoSettings.CompressionMode = &compression
	dura := helpers.SDKDuraToProtocol(settings.MinimumDurabilityLevel)
	protoSettings.MinimumDurabilityLevel = &dura
	if settings.MaxExpiry > 0 {
		expiry := int32(settings.MaxExpiry.Seconds())
		protoSettings.MaxExpirySeconds = &expiry
	}

	backend := fromGocbStorageBacked(settings.StorageBackend)
	protoSettings.StorageBackend = &backend

	if settings.HistoryRetentionCollectionDefault != gocb.HistoryRetentionCollectionDefaultUnset {
		if settings.HistoryRetentionCollectionDefault == gocb.HistoryRetentionCollectionDefaultEnabled {
			trueBool := true
			protoSettings.HistoryRetentionCollectionDefault = &trueBool
		} else {
			falseBool := false
			protoSettings.HistoryRetentionCollectionDefault = &falseBool
		}
	}

	protoSettings.HistoryRetentionBytes = &settings.HistoryRetentionBytes
	if settings.HistoryRetentionDuration > 0 {
		seconds := int64(settings.HistoryRetentionDuration.Seconds())
		protoSettings.HistoryRetentionSeconds = &seconds
	}

	if settings.NumVBuckets > 0 {
		protoSettings.NumVbuckets = new(uint32)
		*protoSettings.NumVbuckets = uint32(settings.NumVBuckets)
	}
	settings.NumVBuckets = uint16(protoSettings.GetNumVbuckets())

	return protoSettings
}

func (e *Executor) handleClusterSearchIndexManager(clusterOp *sdk.ClusterLevelCommand_SearchIndexManager, sender sender.ResultSender, returnResult bool) (bool, error) {
	mgr := e.conn.Cluster().SearchIndexes()
	result := &run.Result{
		Initiated: timestamppb.Now(),
	}

	switch op := clusterOp.SearchIndexManager.GetShared().Command.(type) {
	case *searchManager.Command_GetIndex:
		cmd := op.GetIndex
		opts, err := e.parseGetSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		index, err := mgr.GetIndex(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		if returnResult {
			protoRes, err := e.makeGetSearchIndexResult(index)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil

	case *searchManager.Command_GetAllIndexes:
		opts, err := e.parseGetAllSearchIndexOptions(op.GetAllIndexes)
		if err != nil {
			return false, err
		}

		start := time.Now()
		indexes, err := mgr.GetAllIndexes(opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		if returnResult {
			protoRes, err := e.makeGetAllSearchIndexResult(indexes)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil

	case *searchManager.Command_UpsertIndex:
		cmd := op.UpsertIndex
		opts, err := e.parseUpsertSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		index, err := toGocbSearchIndex(cmd.GetIndexDefinition())
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.UpsertIndex(*index, opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_DropIndex:
		cmd := op.DropIndex
		opts, err := e.parseDropSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.DropIndex(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_GetIndexedDocumentsCount:
		cmd := op.GetIndexedDocumentsCount
		opts, err := e.parseGetIndexedDocumentsCountOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		count, err := mgr.GetIndexedDocumentsCount(op.GetIndexedDocumentsCount.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		if returnResult {
			protoRes, err := e.makeGetIndexedDocumentCountResult(count)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil

	case *searchManager.Command_PauseIngest:
		opts, err := e.parsePauseIngestSearchIndexOptions(op.PauseIngest)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.PauseIngest(op.PauseIngest.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_ResumeIngest:
		cmd := op.ResumeIngest
		opts, err := e.parseResumeIngestSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.ResumeIngest(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_AllowQuerying:
		cmd := op.AllowQuerying
		opts, err := e.parseAllowQueryingSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.AllowQuerying(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_DisallowQuerying:
		cmd := op.DisallowQuerying
		var opts *gocb.AllowQueryingSearchIndexOptions
		if cmd.GetOptions() != nil {
			opts = &gocb.AllowQueryingSearchIndexOptions{
				Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
			}

			if cmd.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*op.DisallowQuerying.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *op.DisallowQuerying.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}
		}
		start := time.Now()
		err := mgr.DisallowQuerying(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_FreezePlan:
		cmd := op.FreezePlan
		var opts *gocb.AllowQueryingSearchIndexOptions
		if cmd.GetOptions() != nil {
			opts = &gocb.AllowQueryingSearchIndexOptions{
				Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
			}

			if cmd.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*op.FreezePlan.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *op.FreezePlan.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}
		}
		start := time.Now()
		err := mgr.FreezePlan(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_UnfreezePlan:
		var opts *gocb.AllowQueryingSearchIndexOptions
		if op.UnfreezePlan.GetOptions() != nil {
			opts = &gocb.AllowQueryingSearchIndexOptions{
				Timeout: time.Duration(op.UnfreezePlan.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
			}

			if op.UnfreezePlan.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*op.UnfreezePlan.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *op.UnfreezePlan.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}
		}
		start := time.Now()
		err := mgr.UnfreezePlan(op.UnfreezePlan.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchManager.Command_AnalyzeDocument:
		var opts *gocb.AnalyzeDocumentOptions
		if op.AnalyzeDocument.GetOptions() != nil {
			opts = &gocb.AnalyzeDocumentOptions{
				Timeout: time.Duration(op.AnalyzeDocument.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
			}

			if op.AnalyzeDocument.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(*op.AnalyzeDocument.Options.ParentSpanId)
				if !ok {
					return false, fmt.Errorf("unknown parent span id: %s", *op.AnalyzeDocument.Options.ParentSpanId)
				}
				opts.ParentSpan = parent
			}
		}
		var doc interface{}
		err := json.Unmarshal(op.AnalyzeDocument.GetDocument(), &doc)
		if err != nil {
			return false, err
		}
		start := time.Now()
		analyzeResults, err := mgr.AnalyzeDocument(op.AnalyzeDocument.GetIndexName(), doc, opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		if returnResult {
			protoRes, err := e.makeAnalyzeDocumentResult(analyzeResults)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil
	default:
		return false, status.Error(codes.Unimplemented, "unknown command type")
	}
}

func protoServiceTypeToGocb(serviceType waituntilready.ServiceType) (gocb.ServiceType, error) {
	switch serviceType {
	case waituntilready.ServiceType_ANALYTICS:
		return gocb.ServiceTypeAnalytics, nil
	case waituntilready.ServiceType_KV:
		return gocb.ServiceTypeKeyValue, nil
	case waituntilready.ServiceType_MANAGER:
		return gocb.ServiceTypeManagement, nil
	case waituntilready.ServiceType_QUERY:
		return gocb.ServiceTypeQuery, nil
	case waituntilready.ServiceType_SEARCH:
		return gocb.ServiceTypeSearch, nil
	case waituntilready.ServiceType_VIEWS:
		return gocb.ServiceTypeViews, nil
	case waituntilready.ServiceType_EVENTING:
		return gocb.ServiceTypeEventing, nil
	}

	return 0, status.Error(codes.Unimplemented, "unknown service")
}
