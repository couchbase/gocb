package gocb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
)

type queryIndexProviderPs struct {
	provider admin_query_v1.QueryAdminServiceClient

	defaultTimeout time.Duration
	tracer         RequestTracer
	meter          *meterWrapper
}

func (qpc *queryIndexProviderPs) CreatePrimaryIndex(c *Collection, bucketName string, opts *CreatePrimaryQueryIndexOptions) error {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_create_primary_index", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_create_primary_index", "management")
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)
	var numReplicas int32
	if opts.NumReplicas != 0 {
		numReplicas = int32(opts.NumReplicas)
	}
	var name *string
	if opts.CustomName != "" {
		name = &opts.CustomName
	}
	req := &admin_query_v1.CreatePrimaryIndexRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
		NumReplicas:    &numReplicas,
		Deferred:       &opts.Deferred,
		Name:           name,
	}
	_, err := qpc.provider.CreatePrimaryIndex(ctx, req)
	if err != nil {
		err = qpc.handleError(err)

		if opts.IgnoreIfExists && errors.Is(err, ErrIndexExists) {
			return nil
		}

		return err
	}

	return nil
}

func (qpc *queryIndexProviderPs) CreateIndex(c *Collection, bucketName, indexName string, fields []string, opts *CreateQueryIndexOptions) error {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_create_index", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_create_index", "management")
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)
	var numReplicas int32
	if opts.NumReplicas != 0 {
		numReplicas = int32(opts.NumReplicas)
	}
	req := &admin_query_v1.CreateIndexRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
		Name:           indexName,
		NumReplicas:    &numReplicas,
		Fields:         fields,
		Deferred:       &opts.Deferred,
	}
	_, err := qpc.provider.CreateIndex(ctx, req)
	if err != nil {
		err = qpc.handleError(err)

		if opts.IgnoreIfExists && errors.Is(err, ErrIndexExists) {
			return nil
		}

		return err
	}

	return nil
}

func (qpc *queryIndexProviderPs) DropPrimaryIndex(c *Collection, bucketName string, opts *DropPrimaryQueryIndexOptions) error {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_drop_primary_index", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_drop_primary_index", "management")
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	var name *string
	if opts.CustomName != "" {
		name = &opts.CustomName
	}

	req := &admin_query_v1.DropPrimaryIndexRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
		Name:           name,
	}
	_, err := qpc.provider.DropPrimaryIndex(ctx, req)
	if err != nil {
		err = qpc.handleError(err)

		if opts.IgnoreIfNotExists && errors.Is(err, ErrIndexNotFound) {
			return nil
		}

		return err
	}

	return nil
}

func (qpc *queryIndexProviderPs) DropIndex(c *Collection, bucketName, indexName string, opts *DropQueryIndexOptions) error {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_drop_index", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_drop_index", "management")
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	req := &admin_query_v1.DropIndexRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
		Name:           indexName,
	}
	_, err := qpc.provider.DropIndex(ctx, req)
	if err != nil {
		err = qpc.handleError(err)

		if opts.IgnoreIfNotExists && errors.Is(err, ErrIndexNotFound) {
			return nil
		}

		return err
	}

	return nil
}

func (qpc *queryIndexProviderPs) GetAllIndexes(c *Collection, bucketName string, opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_get_all_indexes", start)

	return qpc.getAllIndexes(c, bucketName, opts)
}

func (qpc *queryIndexProviderPs) getAllIndexes(c *Collection, bucketName string, opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_get_all_indexes", "management")
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	req := &admin_query_v1.GetAllIndexesRequest{
		BucketName:     &bucket,
		ScopeName:      scope,
		CollectionName: collection,
	}
	resp, err := qpc.provider.GetAllIndexes(ctx, req)
	if err != nil {
		return nil, qpc.handleError(err)
	}

	var indexes []QueryIndex
	for _, index := range resp.Indexes {
		var indexType QueryIndexType
		switch index.Type {
		case admin_query_v1.IndexType_INDEX_TYPE_VIEW:
			indexType = QueryIndexTypeView
		case admin_query_v1.IndexType_INDEX_TYPE_GSI:
			indexType = QueryIndexTypeGsi
		default:
			logInfof("Unknown query index type: %s", index.Type)
		}

		indexes = append(indexes, QueryIndex{
			Name:           index.Name,
			IsPrimary:      index.IsPrimary,
			Type:           indexType,
			State:          index.State.String(),
			IndexKey:       index.Fields,
			Condition:      index.GetCondition(),
			Partition:      index.GetPartition(),
			Keyspace:       "",
			Namespace:      "",
			CollectionName: index.CollectionName,
			ScopeName:      index.ScopeName,
			BucketName:     index.BucketName,
		})
	}

	return indexes, nil
}

func (qpc *queryIndexProviderPs) BuildDeferredIndexes(c *Collection, bucketName string, opts *BuildDeferredQueryIndexOptions) ([]string, error) {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_build_deferred_indexes", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_build_deferred_indexes", "management")
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.defaultTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	req := &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
	}
	_, err := qpc.provider.BuildDeferredIndexes(ctx, req)
	if err != nil {
		return nil, qpc.handleError(err)
	}

	return []string{""}, nil
}

func checkIndexesActivePs(indexes []QueryIndex, checkList []string) (bool, error) {
	var checkIndexes []QueryIndex
	for i := 0; i < len(checkList); i++ {
		indexName := checkList[i]

		for j := 0; j < len(indexes); j++ {
			if indexes[j].Name == indexName {
				checkIndexes = append(checkIndexes, indexes[j])
				break
			}
		}
	}

	if len(checkIndexes) != len(checkList) {
		return false, ErrIndexNotFound
	}

	for i := 0; i < len(checkIndexes); i++ {
		if checkIndexes[i].State != admin_query_v1.IndexState_INDEX_STATE_ONLINE.String() {
			logDebugf("Index not online: %s is in state %s", checkIndexes[i].Name, checkIndexes[i].State)
			return false, nil
		}
	}
	return true, nil
}

func (qpc *queryIndexProviderPs) WatchIndexes(c *Collection, bucketName string, watchList []string, timeout time.Duration, opts *WatchQueryIndexOptions,
) error {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceManagement, "manager_query_watch_indexes", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "manager_query_watch_indexes", "management")
	defer span.End()

	if opts.WatchPrimary {
		watchList = append(watchList, "#primary")
	}

	deadline := time.Now().Add(timeout)

	curInterval := 50 * time.Millisecond
	for {
		if deadline.Before(time.Now()) {
			return ErrUnambiguousTimeout
		}

		indexes, err := qpc.getAllIndexes(
			c,
			bucketName,
			&GetAllQueryIndexesOptions{
				Timeout:        time.Until(deadline),
				RetryStrategy:  opts.RetryStrategy,
				ParentSpan:     span,
				ScopeName:      opts.ScopeName,
				CollectionName: opts.CollectionName,
				Context:        opts.Context,
			})
		if err != nil {
			return err
		}

		allOnline, err := checkIndexesActivePs(indexes, watchList)
		if err != nil {
			return err
		}

		if allOnline {
			break
		}

		curInterval += 500 * time.Millisecond
		if curInterval > 1000 {
			curInterval = 1000
		}

		// Make sure we don't sleep past our overall deadline, if we adjust the
		// deadline then it will be caught at the top of this loop as a timeout.
		sleepDeadline := time.Now().Add(curInterval)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}

		// wait till our next poll interval
		time.Sleep(time.Until(sleepDeadline))
	}

	return nil
}

func (qpc *queryIndexProviderPs) normaliseCollectionKeyspace(c *Collection) (string, string) {
	// Ensure scope and collection names are populated, if the DefaultX functions on bucket are
	// used then the names will be empty by default.
	scope := c.scope
	if scope == "" {
		scope = "_default"
	}
	collection := c.collectionName
	if collection == "" {
		collection = "_default"
	}

	return scope, collection
}

func (qpc *queryIndexProviderPs) makeKeyspace(c *Collection, bucketName, scopeName, collectionName string) (string, *string, *string) {
	if c != nil {
		// If we have a collection then we need to build the namespace using it rather than options.
		scope, collection := qpc.normaliseCollectionKeyspace(c)

		return c.bucketName(), &scope, &collection
	}

	if scopeName != "" && collectionName != "" {
		return bucketName, &scopeName, &collectionName
	} else if collectionName == "" && scopeName != "" {
		return bucketName, &scopeName, nil
	} else if collectionName != "" && scopeName == "" {
		return bucketName, nil, &collectionName
	}
	return bucketName, nil, nil
}

func (qpc *queryIndexProviderPs) handleError(err error) error {
	gocbErr := mapPsErrorToGocbError(err, false)
	if errors.Is(gocbErr, ErrInternalServerFailure) {
		gocbErr = qpc.tryParseErrorMessage(gocbErr)
	}

	return gocbErr
}

// tryParseErrorMessage is temporary until protostellar gives us the correct errors.
func (qpc *queryIndexProviderPs) tryParseErrorMessage(err *GenericError) *GenericError {
	server, ok := err.Context["server"]
	if !ok {
		return err
	}
	msg, ok := server.(string)
	if !ok {
		return err
	}

	var innerErr error
	if strings.Contains(msg, " 12016 ") {
		innerErr = ErrIndexNotFound
	} else if strings.Contains(msg, " 4300 ") {
		innerErr = ErrIndexExists
	}

	if innerErr == nil {
		return err
	}

	return makeGenericError(innerErr, err.Context)
}
