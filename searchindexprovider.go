package gocb

type searchIndexProvider interface {
	GetAllIndexes(opts *GetAllSearchIndexOptions) ([]SearchIndex, error)
	GetIndex(indexName string, opts *GetSearchIndexOptions) (*SearchIndex, error)
	UpsertIndex(indexDefinition SearchIndex, opts *UpsertSearchIndexOptions) error
	DropIndex(indexName string, opts *DropSearchIndexOptions) error
	AnalyzeDocument(indexName string, doc interface{}, opts *AnalyzeDocumentOptions) ([]interface{}, error)
	GetIndexedDocumentsCount(indexName string, opts *GetIndexedDocumentsCountOptions) (uint64, error)
	PauseIngest(indexName string, opts *PauseIngestSearchIndexOptions) error
	ResumeIngest(indexName string, opts *ResumeIngestSearchIndexOptions) error
	AllowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error
	DisallowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error
	FreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error
	UnfreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error
}
