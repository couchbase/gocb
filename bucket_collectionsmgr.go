package gocb

import (
	"context"
	"time"
)

// CollectionSpec describes the specification of a collection.
type CollectionSpec struct {
	Name      string
	ScopeName string
	MaxExpiry time.Duration
}

// ScopeSpec describes the specification of a scope.
type ScopeSpec struct {
	Name        string
	Collections []CollectionSpec
}

// CollectionManager provides methods for performing collections management.
type CollectionManager struct {
	getProvider func() (collectionsManagementProvider, error)
}

// GetAllScopesOptions is the set of options available to the GetAllScopes operation.
type GetAllScopesOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// GetAllScopes gets all scopes from the bucket.
func (cm *CollectionManager) GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error) {
	if opts == nil {
		opts = &GetAllScopesOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.GetAllScopes(opts)
}

// CreateCollectionOptions is the set of options available to the CreateCollection operation.
type CreateCollectionOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// CreateCollection creates a new collection on the bucket.
func (cm *CollectionManager) CreateCollection(spec CollectionSpec, opts *CreateCollectionOptions) error {
	if spec.Name == "" {
		return makeInvalidArgumentsError("collection name cannot be empty")
	}

	if spec.ScopeName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &CreateCollectionOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.CreateCollection(spec, opts)
}

// DropCollectionOptions is the set of options available to the DropCollection operation.
type DropCollectionOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// DropCollection removes a collection.
func (cm *CollectionManager) DropCollection(spec CollectionSpec, opts *DropCollectionOptions) error {
	if spec.Name == "" {
		return makeInvalidArgumentsError("collection name cannot be empty")
	}

	if spec.ScopeName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &DropCollectionOptions{}
	}

	provider, err := cm.getProvider()
	if err != nil {
		return err
	}

	return provider.DropCollection(spec, opts)
}

// CreateScopeOptions is the set of options available to the CreateScope operation.
type CreateScopeOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// CreateScope creates a new scope on the bucket.
func (cm *CollectionManager) CreateScope(scopeName string, opts *CreateScopeOptions) error {
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

// DropScopeOptions is the set of options available to the DropScope operation.
type DropScopeOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// DropScope removes a scope.
func (cm *CollectionManager) DropScope(scopeName string, opts *DropScopeOptions) error {
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
