package gocb

type collectionsManagementProvider interface {
	GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error)
	CreateCollection(spec CollectionSpec, opts *CreateCollectionOptions) error
	UpdateCollection(spec CollectionSpec, opts *UpdateCollectionOptions) error
	DropCollection(spec CollectionSpec, opts *DropCollectionOptions) error
	CreateScope(scopeName string, opts *CreateScopeOptions) error
	DropScope(scopeName string, opts *DropScopeOptions) error
}
