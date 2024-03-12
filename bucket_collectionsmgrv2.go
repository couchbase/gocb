package gocb

import "time"

// CollectionManagerV2 provides methods for performing collections management.
type CollectionManagerV2 struct {
	getProvider func() (collectionsManagementProvider, error)
}

// GetAllScopes gets all scopes from the bucket.
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
type CreateCollectionSettings struct {
	// MaxExpiry is the maximum expiry all documents in the collection can have.
	// Defaults to the bucket-level setting.
	// Value of -1 seconds (time.Duration(-1) * time.Second)  denotes 'no expiry'.
	MaxExpiry time.Duration
	History   *CollectionHistorySettings
}

// CreateCollection creates a new collection on the bucket.
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
type UpdateCollectionSettings struct {
	// MaxExpiry is the maximum expiry all documents in the collection can have.
	// Defaults to the bucket-level setting.
	// Value of -1 seconds (time.Duration(-1) * time.Second)  denotes 'no expiry'.
	MaxExpiry time.Duration
	History   *CollectionHistorySettings
}

// UpdateCollection updates the settings of an existing collection.
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
