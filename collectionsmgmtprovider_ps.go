package gocb

import (
	"context"
	"time"

	"google.golang.org/grpc/status"

	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
)

type collectionsManagementProviderPs struct {
	provider admin_collection_v1.CollectionAdminServiceClient

	defaultTimeout time.Duration
	bucketName     string
	tracer         RequestTracer
	meter          *meterWrapper
}

func (cm *collectionsManagementProviderPs) GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error) {
	start := time.Now()
	defer cm.meter.ValueRecord(meterValueServiceManagement, "manager_collections_get_all_scopes", start)

	span := createSpan(cm.tracer, opts.ParentSpan, "manager_collections_get_all_scopes", "management")
	span.SetAttribute("db.name", cm.bucketName)
	span.SetAttribute("db.operation", "ListCollections")
	defer span.End()

	req := &admin_collection_v1.ListCollectionsRequest{
		BucketName: cm.bucketName,
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = cm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := cm.provider.ListCollections(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, true)
		if gocbErr == nil {
			gocbErr = err
		}

		return nil, makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	var scopes []ScopeSpec
	for _, scope := range resp.GetScopes() {
		var collections []CollectionSpec
		for _, col := range scope.Collections {
			collections = append(collections, CollectionSpec{
				Name:      col.Name,
				ScopeName: scope.Name,
				MaxExpiry: time.Duration(col.GetMaxExpirySecs()) * time.Second,
			})
		}
		scopes = append(scopes, ScopeSpec{
			Name:        scope.Name,
			Collections: collections,
		})
	}

	return scopes, nil
}

// CreateCollection creates a new collection on the bucket.
func (cm *collectionsManagementProviderPs) CreateCollection(spec CollectionSpec, opts *CreateCollectionOptions) error {
	start := time.Now()
	defer cm.meter.ValueRecord(meterValueServiceManagement, "manager_collections_create_collection", start)

	span := createSpan(cm.tracer, opts.ParentSpan, "manager_collections_create_collection", "management")
	span.SetAttribute("db.name", cm.bucketName)
	span.SetAttribute("db.couchbase.scope", spec.ScopeName)
	span.SetAttribute("db.couchbase.collection", spec.Name)
	span.SetAttribute("db.operation", "CreateCollection")
	defer span.End()

	req := &admin_collection_v1.CreateCollectionRequest{
		BucketName:     cm.bucketName,
		ScopeName:      spec.ScopeName,
		CollectionName: spec.Name,
	}
	if spec.MaxExpiry > 0 {
		expiry := uint32(spec.MaxExpiry.Seconds())
		req.MaxExpirySecs = &expiry
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = cm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := cm.provider.CreateCollection(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}

// DropCollection removes a collection.
func (cm *collectionsManagementProviderPs) DropCollection(spec CollectionSpec, opts *DropCollectionOptions) error {
	start := time.Now()
	defer cm.meter.ValueRecord(meterValueServiceManagement, "manager_collections_drop_collection", start)

	span := createSpan(cm.tracer, opts.ParentSpan, "manager_collections_drop_collection", "management")
	span.SetAttribute("db.name", cm.bucketName)
	span.SetAttribute("db.couchbase.scope", spec.ScopeName)
	span.SetAttribute("db.couchbase.collection", spec.Name)
	span.SetAttribute("db.operation", "DeleteCollection")
	defer span.End()

	req := &admin_collection_v1.DeleteCollectionRequest{
		BucketName:     cm.bucketName,
		ScopeName:      spec.ScopeName,
		CollectionName: spec.Name,
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = cm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := cm.provider.DeleteCollection(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}

// CreateScope creates a new scope on the bucket.
func (cm *collectionsManagementProviderPs) CreateScope(scopeName string, opts *CreateScopeOptions) error {
	start := time.Now()
	defer cm.meter.ValueRecord(meterValueServiceManagement, "manager_collections_create_scope", start)

	span := createSpan(cm.tracer, opts.ParentSpan, "manager_collections_create_scope", "management")
	span.SetAttribute("db.name", cm.bucketName)
	span.SetAttribute("db.couchbase.scope", scopeName)
	span.SetAttribute("db.operation", "CreateScope")
	defer span.End()

	req := &admin_collection_v1.CreateScopeRequest{
		BucketName: cm.bucketName,
		ScopeName:  scopeName,
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = cm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := cm.provider.CreateScope(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}

// DropScope removes a scope.
func (cm *collectionsManagementProviderPs) DropScope(scopeName string, opts *DropScopeOptions) error {
	start := time.Now()
	defer cm.meter.ValueRecord(meterValueServiceManagement, "manager_collections_drop_scope", start)

	span := createSpan(cm.tracer, opts.ParentSpan, "manager_collections_drop_scope", "management")
	span.SetAttribute("db.name", cm.bucketName)
	span.SetAttribute("db.couchbase.scope", scopeName)
	span.SetAttribute("db.operation", "DeleteScope")
	defer span.End()

	req := &admin_collection_v1.DeleteScopeRequest{
		BucketName: cm.bucketName,
		ScopeName:  scopeName,
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = cm.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := cm.provider.DeleteScope(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return makeGenericMgmtError(err, nil, nil, err.Error())
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, false)
		if gocbErr == nil {
			gocbErr = err
		}

		return makeGenericMgmtError(gocbErr, nil, nil, err.Error())
	}

	return nil
}
