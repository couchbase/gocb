package executor

import (
	"time"

	"github.com/couchbase/gocb/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/bucket/collectionmanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/waituntilready"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
)

func (e *Executor) handleBucketLevelCommand(command *sdk.BucketLevelCommand, sender sender.ResultSender, returnResult bool) (bool, error) {
	switch op := command.Command.(type) {
	case *sdk.BucketLevelCommand_CollectionManager:
		switch sharedOp := op.CollectionManager.Command.(type) {
		case *collectionmanager.Command_GetAllScopes:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.GetAllScopesOptions{
				Timeout: time.Duration(sharedOp.GetAllScopes.Options.GetTimeoutMsecs()) * time.Millisecond,
			}

			if sharedOp.GetAllScopes.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(sharedOp.GetAllScopes.Options.GetParentSpanId())
				if !ok {
					return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", sharedOp.GetAllScopes.Options.GetParentSpanId())
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			res, err := e.conn.Cluster().
				Bucket(command.GetBucketName()).
				Collections().
				GetAllScopes(opts)
			if err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.ElapsedNanos = time.Since(start).Nanoseconds()
			if returnResult {
				scopes := make([]*collectionmanager.ScopeSpec, len(res))
				for i, scope := range res {
					collections := make([]*collectionmanager.CollectionSpec, len(scope.Collections))
					for j, collection := range scope.Collections {
						collections[j] = &collectionmanager.CollectionSpec{
							Name:      collection.Name,
							ScopeName: collection.ScopeName,
						}

						if collection.MaxExpiry > 0 {
							expiry := int32(collection.MaxExpiry.Seconds())
							collections[j].ExpirySecs = &expiry
						}

						if collection.History != nil {
							collections[j].History = &collection.History.Enabled
						}
					}

					scopes[i] = &collectionmanager.ScopeSpec{
						Name:        scope.Name,
						Collections: collections,
					}
				}

				result.Result = &run.Result_Sdk{
					Sdk: &sdk.Result{
						Result: &sdk.Result_CollectionManagerResult{
							CollectionManagerResult: &collectionmanager.Result{
								Result: &collectionmanager.Result_GetAllScopesResult{
									GetAllScopesResult: &collectionmanager.GetAllScopesResult{
										Result: scopes,
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
		case *collectionmanager.Command_CreateScope:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.CreateScopeOptions{
				Timeout: time.Duration(sharedOp.CreateScope.Options.GetTimeoutMsecs()) * time.Millisecond,
			}

			if sharedOp.CreateScope.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(sharedOp.CreateScope.Options.GetParentSpanId())
				if !ok {
					return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", sharedOp.CreateScope.Options.GetParentSpanId())
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			err := e.conn.Cluster().
				Bucket(command.GetBucketName()).
				Collections().
				CreateScope(sharedOp.CreateScope.Name, opts)
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
		case *collectionmanager.Command_DropScope:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.DropScopeOptions{
				Timeout: time.Duration(sharedOp.DropScope.Options.GetTimeoutMsecs()) * time.Millisecond,
			}

			if sharedOp.DropScope.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(sharedOp.DropScope.Options.GetParentSpanId())
				if !ok {
					return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", sharedOp.DropScope.Options.GetParentSpanId())
				}
				opts.ParentSpan = parent
			}

			start := time.Now()
			err := e.conn.Cluster().
				Bucket(command.GetBucketName()).
				Collections().
				DropScope(sharedOp.DropScope.Name, opts)
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
		case *collectionmanager.Command_CreateCollection:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.CreateCollectionOptions{
				Timeout: time.Duration(sharedOp.CreateCollection.Options.GetTimeoutMsecs()) * time.Millisecond,
			}

			if sharedOp.CreateCollection.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(sharedOp.CreateCollection.Options.GetParentSpanId())
				if !ok {
					return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", sharedOp.CreateCollection.Options.GetParentSpanId())
				}
				opts.ParentSpan = parent
			}

			spec := gocb.CollectionSpec{
				Name:      sharedOp.CreateCollection.Name,
				ScopeName: sharedOp.CreateCollection.ScopeName,
			}
			if sharedOp.CreateCollection.Settings != nil {
				if sharedOp.CreateCollection.Settings.ExpirySecs != nil {
					spec.MaxExpiry = time.Duration(sharedOp.CreateCollection.Settings.GetExpirySecs()) * time.Second
				}
				if sharedOp.CreateCollection.Settings.History != nil {
					spec.History = &gocb.CollectionHistorySettings{
						Enabled: sharedOp.CreateCollection.Settings.GetHistory(),
					}
				}
			}

			start := time.Now()
			err := e.conn.Cluster().
				Bucket(command.GetBucketName()).
				Collections().
				CreateCollection(spec, opts)
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
		case *collectionmanager.Command_UpdateCollection:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.UpdateCollectionOptions{
				Timeout: time.Duration(sharedOp.UpdateCollection.Options.GetTimeoutMsecs()) * time.Millisecond,
			}

			if sharedOp.UpdateCollection.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(sharedOp.UpdateCollection.Options.GetParentSpanId())
				if !ok {
					return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", sharedOp.UpdateCollection.Options.GetParentSpanId())
				}
				opts.ParentSpan = parent
			}

			spec := gocb.CollectionSpec{
				Name:      sharedOp.UpdateCollection.Name,
				ScopeName: sharedOp.UpdateCollection.ScopeName,
			}
			if sharedOp.UpdateCollection.Settings != nil {
				if sharedOp.UpdateCollection.Settings.ExpirySecs != nil {
					spec.MaxExpiry = time.Duration(sharedOp.UpdateCollection.Settings.GetExpirySecs()) * time.Second
				}
				if sharedOp.UpdateCollection.Settings.History != nil {
					spec.History = &gocb.CollectionHistorySettings{
						Enabled: sharedOp.UpdateCollection.Settings.GetHistory(),
					}
				}
			}

			start := time.Now()
			err := e.conn.Cluster().
				Bucket(command.GetBucketName()).
				Collections().
				UpdateCollection(spec, opts)
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
		case *collectionmanager.Command_DropCollection:
			result := &run.Result{
				Initiated: timestamppb.Now(),
			}

			opts := &gocb.DropCollectionOptions{
				Timeout: time.Duration(sharedOp.DropCollection.Options.GetTimeoutMsecs()) * time.Millisecond,
			}

			if sharedOp.DropCollection.Options.GetParentSpanId() != "" {
				parent, ok := e.spanOwner.GetSpan(sharedOp.DropCollection.Options.GetParentSpanId())
				if !ok {
					return false, status.Errorf(codes.InvalidArgument, "unknown parent span id: %s", sharedOp.DropCollection.Options.GetParentSpanId())
				}
				opts.ParentSpan = parent
			}

			spec := gocb.CollectionSpec{
				Name:      sharedOp.DropCollection.Name,
				ScopeName: sharedOp.DropCollection.ScopeName,
			}

			start := time.Now()
			err := e.conn.Cluster().
				Bucket(command.GetBucketName()).
				Collections().
				DropCollection(spec, opts)
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
		}
		return false, status.Error(codes.Unimplemented, "unknown command type")
	case *sdk.BucketLevelCommand_WaitUntilReady:
		result := &run.Result{
			Initiated: timestamppb.Now(),
		}

		var opts *gocb.WaitUntilReadyOptions
		if op.WaitUntilReady.Options != nil {
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
					return false, status.Error(codes.Unimplemented, "unknown desired state")
				}
			}
		}

		start := time.Now()
		err := e.conn.Cluster().WaitUntilReady(time.Duration(op.WaitUntilReady.TimeoutMillis)*time.Millisecond, opts)
		if err != nil {
			return false, err
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
