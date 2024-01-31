package gocb

// ScopeSearchIndexManager provides methods for performing scope-level search index management operations.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
type ScopeSearchIndexManager struct {
	getProvider func() (searchIndexProvider, error)

	scope *Scope
}

// GetAllIndexes retrieves all of the search indexes for the scope.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) GetAllIndexes(opts *GetAllSearchIndexOptions) ([]SearchIndex, error) {
	if opts == nil {
		opts = &GetAllSearchIndexOptions{}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.GetAllIndexes(sm.scope, opts)
}

// GetIndex retrieves a specific search index by name.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) GetIndex(indexName string, opts *GetSearchIndexOptions) (*SearchIndex, error) {
	if opts == nil {
		opts = &GetSearchIndexOptions{}
	}

	if indexName == "" {
		return nil, invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.GetIndex(sm.scope, indexName, opts)
}

// UpsertIndex creates or updates a search index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) UpsertIndex(indexDefinition SearchIndex, opts *UpsertSearchIndexOptions) error {
	if opts == nil {
		opts = &UpsertSearchIndexOptions{}
	}

	if indexDefinition.Name == "" {
		return invalidArgumentsError{"index name cannot be empty"}
	}
	if indexDefinition.Type == "" {
		return invalidArgumentsError{"index type cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.UpsertIndex(sm.scope, indexDefinition, opts)
}

// DropIndex removes the search index with the specific name.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) DropIndex(indexName string, opts *DropSearchIndexOptions) error {
	if opts == nil {
		opts = &DropSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.DropIndex(sm.scope, indexName, opts)
}

// AnalyzeDocument returns how a doc is analyzed against a specific index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) AnalyzeDocument(indexName string, doc interface{}, opts *AnalyzeDocumentOptions) ([]interface{}, error) {
	if opts == nil {
		opts = &AnalyzeDocumentOptions{}
	}

	if indexName == "" {
		return nil, invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.AnalyzeDocument(sm.scope, indexName, doc, opts)
}

// GetIndexedDocumentsCount retrieves the document count for a search index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) GetIndexedDocumentsCount(indexName string, opts *GetIndexedDocumentsCountOptions) (uint64, error) {
	if opts == nil {
		opts = &GetIndexedDocumentsCountOptions{}
	}

	if indexName == "" {
		return 0, invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return 0, err
	}

	return provider.GetIndexedDocumentsCount(sm.scope, indexName, opts)
}

// PauseIngest pauses updates and maintenance for an index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) PauseIngest(indexName string, opts *PauseIngestSearchIndexOptions) error {
	if opts == nil {
		opts = &PauseIngestSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.PauseIngest(sm.scope, indexName, opts)
}

// ResumeIngest resumes updates and maintenance for an index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) ResumeIngest(indexName string, opts *ResumeIngestSearchIndexOptions) error {
	if opts == nil {
		opts = &ResumeIngestSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.ResumeIngest(sm.scope, indexName, opts)
}

// AllowQuerying allows querying against an index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) AllowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.AllowQuerying(sm.scope, indexName, opts)
}

// DisallowQuerying disallows querying against an index.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) DisallowQuerying(indexName string, opts *DisallowQueryingSearchIndexOptions) error {
	if opts == nil {
		opts = &DisallowQueryingSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.DisallowQuerying(sm.scope, indexName, opts)
}

// FreezePlan freezes the assignment of index partitions to nodes.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) FreezePlan(indexName string, opts *FreezePlanSearchIndexOptions) error {
	if opts == nil {
		opts = &FreezePlanSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.FreezePlan(sm.scope, indexName, opts)
}

// UnfreezePlan unfreezes the assignment of index partitions to nodes.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func (sm *ScopeSearchIndexManager) UnfreezePlan(indexName string, opts *UnfreezePlanSearchIndexOptions) error {
	if opts == nil {
		opts = &UnfreezePlanSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	provider, err := sm.getProvider()
	if err != nil {
		return err
	}

	return provider.UnfreezePlan(sm.scope, indexName, opts)
}
