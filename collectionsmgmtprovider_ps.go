package gocb

import (
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
)

type collectionsManagementProviderPs struct {
	provider admin_collection_v1.CollectionAdminServiceClient

	managerProvider *psOpManagerProvider
	bucketName      string
}

func (cm collectionsManagementProviderPs) newOpManager(parentSpan RequestSpan, opName string, attribs map[string]interface{}) *psOpManagerDefault {
	return cm.managerProvider.NewManager(parentSpan, opName, attribs)
}

func (cm *collectionsManagementProviderPs) GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error) {
	manager := cm.newOpManager(opts.ParentSpan, "manager_collections_get_all_scopes", map[string]interface{}{
		"db.name":      cm.bucketName,
		"db.operation": "ListCollections",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return nil, err
	}

	req := &admin_collection_v1.ListCollectionsRequest{
		BucketName: cm.bucketName,
	}

	resp, err := wrapPSOp(manager, req, cm.provider.ListCollections)
	if err != nil {
		return nil, err
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
	manager := cm.newOpManager(opts.ParentSpan, "manager_collections_create_collection", map[string]interface{}{
		"db.name":                 cm.bucketName,
		"db.couchbase.scope":      spec.ScopeName,
		"db.couchbase.collection": spec.Name,
		"db.operation":            "CreateCollection",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_collection_v1.CreateCollectionRequest{
		BucketName:     cm.bucketName,
		ScopeName:      spec.ScopeName,
		CollectionName: spec.Name,
	}
	if spec.MaxExpiry > 0 {
		expiry := uint32(spec.MaxExpiry.Seconds())
		req.MaxExpirySecs = &expiry
	}

	_, err := wrapPSOp(manager, req, cm.provider.CreateCollection)
	if err != nil {
		return err
	}

	return nil
}

func (cm *collectionsManagementProviderPs) UpdateCollection(spec CollectionSpec, opts *UpdateCollectionOptions) error {
	manager := cm.newOpManager(opts.ParentSpan, "manager_collections_update_collection", map[string]interface{}{
		"db.name":                 cm.bucketName,
		"db.couchbase.scope":      spec.ScopeName,
		"db.couchbase.collection": spec.Name,
		"db.operation":            "UpdateCollection",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_collection_v1.UpdateCollectionRequest{
		BucketName:     cm.bucketName,
		ScopeName:      spec.ScopeName,
		CollectionName: spec.Name,
	}
	if spec.MaxExpiry > 0 {
		expiry := uint32(spec.MaxExpiry.Seconds())
		req.MaxExpirySecs = &expiry
	}
	if spec.History != nil {
		req.HistoryRetentionEnabled = &spec.History.Enabled
	}

	_, err := wrapPSOp(manager, req, cm.provider.UpdateCollection)
	if err != nil {
		return err
	}

	return nil
}

// DropCollection removes a collection.
func (cm *collectionsManagementProviderPs) DropCollection(spec CollectionSpec, opts *DropCollectionOptions) error {
	manager := cm.newOpManager(opts.ParentSpan, "manager_collections_drop_collection", map[string]interface{}{
		"db.name":                 cm.bucketName,
		"db.couchbase.scope":      spec.ScopeName,
		"db.couchbase.collection": spec.Name,
		"db.operation":            "DeleteCollection",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_collection_v1.DeleteCollectionRequest{
		BucketName:     cm.bucketName,
		ScopeName:      spec.ScopeName,
		CollectionName: spec.Name,
	}

	_, err := wrapPSOp(manager, req, cm.provider.DeleteCollection)
	if err != nil {
		return err
	}

	return nil
}

// CreateScope creates a new scope on the bucket.
func (cm *collectionsManagementProviderPs) CreateScope(scopeName string, opts *CreateScopeOptions) error {
	manager := cm.newOpManager(opts.ParentSpan, "manager_collections_create_scope", map[string]interface{}{
		"db.name":            cm.bucketName,
		"db.couchbase.scope": scopeName,
		"db.operation":       "CreateScope",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_collection_v1.CreateScopeRequest{
		BucketName: cm.bucketName,
		ScopeName:  scopeName,
	}

	_, err := wrapPSOp(manager, req, cm.provider.CreateScope)
	if err != nil {
		return err
	}

	return nil
}

// DropScope removes a scope.
func (cm *collectionsManagementProviderPs) DropScope(scopeName string, opts *DropScopeOptions) error {
	manager := cm.newOpManager(opts.ParentSpan, "manager_collections_drop_scope", map[string]interface{}{
		"db.name":            cm.bucketName,
		"db.couchbase.scope": scopeName,
		"db.operation":       "DeleteScope",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_collection_v1.DeleteScopeRequest{
		BucketName: cm.bucketName,
		ScopeName:  scopeName,
	}

	_, err := wrapPSOp(manager, req, cm.provider.DeleteScope)
	if err != nil {
		return err
	}

	return nil
}
