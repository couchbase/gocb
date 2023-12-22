package gocb

import "time"

// CollectionManagerV2 provides methods for performing collections management.
// # UNCOMMITTED: This API may change in the future.
type CollectionManagerV2 struct {
	getProvider func() (collectionsManagementProvider, error)
}

// GetAllScopes gets all scopes from the bucket.
// # UNCOMMITTED: This API may change in the future.
func (cm *CollectionManagerV2) GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error) {
	if opts == nil {
		opts = &GetAllScopesOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.GetAllScopes(opts)
}

// CreateCollectionSettings specifies settings for a collection to be created
// # UNCOMMITTED: This API may change in the future.
type CreateCollectionSettings struct {
	MaxExpiry time.Duration

	// # UNCOMMITTED
	//
	// This API is UNCOMMITTED and may change in the future.
	History *CollectionHistorySettings
}

// CreateCollection creates a new collection on the bucket.
// # UNCOMMITTED: This API may change in the future.
func (cm *CollectionManagerV2) CreateCollection(scopeName string, collectionName string, settings *CreateCollectionSettings, opts *CreateCollectionOptions) error {
	if scopeName == "" {
		return makeInvalidArgumentsError("collection name cannot be empty")
	}

	if collectionName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if settings == nil {
		settings = &CreateCollectionSettings{}
	}

	if opts == nil {
		opts = &CreateCollectionOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.CreateCollection(scopeName, collectionName, settings, opts)
}

// UpdateCollectionSettings specifies the settings for a collection that should be updated.
// # UNCOMMITTED: This API may change in the future.
type UpdateCollectionSettings struct {
	MaxExpiry time.Duration

	// # UNCOMMITTED
	//
	// This API is UNCOMMITTED and may change in the future.
	History *CollectionHistorySettings
}

// UpdateCollection updates the settings of an existing collection.
// # UNCOMMITTED: This API may change in the future.
func (cm *CollectionManagerV2) UpdateCollection(scopeName string, collectionName string, settings UpdateCollectionSettings, opts *UpdateCollectionOptions) error {
	if scopeName == "" {
		return makeInvalidArgumentsError("collection name cannot be empty")
	}

	if collectionName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &UpdateCollectionOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.UpdateCollection(scopeName, collectionName, settings, opts)
}

// DropCollection removes a collection.
// # UNCOMMITTED: This API may change in the future.
func (cm *CollectionManagerV2) DropCollection(scopeName string, collectionName string, opts *DropCollectionOptions) error {
	if scopeName == "" {
		return makeInvalidArgumentsError("collection name cannot be empty")
	}

	if collectionName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &DropCollectionOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.DropCollection(scopeName, collectionName, opts)
}

// CreateScope creates a new scope on the bucket.
// # UNCOMMITTED: This API may change in the future.
func (cm *CollectionManagerV2) CreateScope(scopeName string, opts *CreateScopeOptions) error {
	if scopeName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &CreateScopeOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.CreateScope(scopeName, opts)
}

// DropScope removes a scope.
// # UNCOMMITTED: This API may change in the future.
func (cm *CollectionManagerV2) DropScope(scopeName string, opts *DropScopeOptions) error {
	if scopeName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &DropScopeOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.DropScope(scopeName, opts)
}
