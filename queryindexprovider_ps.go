package gocb

import (
	"errors"
	"strings"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
)

type queryIndexProviderPs struct {
	provider admin_query_v1.QueryAdminServiceClient

	managerProvider *psOpManagerProvider
}

func (qpc *queryIndexProviderPs) newOpManager(parentSpan RequestSpan, opName string, attribs map[string]interface{}) *psOpManagerDefault {
	return qpc.managerProvider.NewManager(parentSpan, opName, attribs)
}

func (qpc *queryIndexProviderPs) CreatePrimaryIndex(c *Collection, bucketName string, opts *CreatePrimaryQueryIndexOptions) error {
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_create_primary_index", map[string]interface{}{
		"db.operation": "CreatePrimaryIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

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

	_, err := wrapPSOp(manager, req, qpc.provider.CreatePrimaryIndex)
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
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_create_index", map[string]interface{}{
		"db.operation": "CreateIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

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

	_, err := wrapPSOp(manager, req, qpc.provider.CreateIndex)
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
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_drop_primary_index", map[string]interface{}{
		"db.operation": "DropPrimaryIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

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

	_, err := wrapPSOp(manager, req, qpc.provider.DropPrimaryIndex)
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
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_drop_index", map[string]interface{}{
		"db.operation": "DropIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	req := &admin_query_v1.DropIndexRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
		Name:           indexName,
	}

	_, err := wrapPSOp(manager, req, qpc.provider.DropIndex)
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
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_get_all_indexes", map[string]interface{}{
		"db.operation": "GetAllIndexes",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return nil, err
	}

	return qpc.getAllIndexes(c, bucketName, manager, opts)
}

func (qpc *queryIndexProviderPs) getAllIndexes(c *Collection, bucketName string, manager *psOpManagerDefault,
	opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	req := &admin_query_v1.GetAllIndexesRequest{
		BucketName:     &bucket,
		ScopeName:      scope,
		CollectionName: collection,
	}

	resp, err := wrapPSOp(manager, req, qpc.provider.GetAllIndexes)
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

		var state queryIndexState
		switch index.State {
		case admin_query_v1.IndexState_INDEX_STATE_DEFERRED:
			state = queryIndexStateDeferred
		case admin_query_v1.IndexState_INDEX_STATE_BUILDING:
			state = queryIndexStateBuilding
		case admin_query_v1.IndexState_INDEX_STATE_PENDING:
			state = queryIndexStatePending
		case admin_query_v1.IndexState_INDEX_STATE_ONLINE:
			state = queryIndexStateOnline
		case admin_query_v1.IndexState_INDEX_STATE_OFFLINE:
			state = queryIndexStateOffline
		case admin_query_v1.IndexState_INDEX_STATE_ABRIDGED:
			state = queryIndexStateAbridged
		case admin_query_v1.IndexState_INDEX_STATE_SCHEDULED:
			state = queryIndexStateScheduled
		}

		indexes = append(indexes, QueryIndex{
			Name:           index.Name,
			IsPrimary:      index.IsPrimary,
			Type:           indexType,
			State:          string(state),
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
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_build_deferred_indexes", map[string]interface{}{
		"db.operation": "BuildDeferredIndexes",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return nil, err
	}

	bucket, scope, collection := qpc.makeKeyspace(c, bucketName, opts.ScopeName, opts.CollectionName)

	req := &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
	}

	resp, err := wrapPSOp(manager, req, qpc.provider.BuildDeferredIndexes)
	if err != nil {
		return nil, qpc.handleError(err)
	}

	indexNames := make([]string, len(resp.Indexes))
	for i, index := range resp.Indexes {
		fullName := index.BucketName
		if index.ScopeName != nil || index.CollectionName != nil {
			scopeName := index.GetScopeName()
			if scopeName == "" {
				scopeName = "_default"
			}
			collectionName := index.GetCollectionName()
			if collectionName == "" {
				collectionName = "_default"
			}

			fullName += "." + scopeName + "." + collectionName
		}

		fullName = fullName + "." + index.Name

		indexNames[i] = fullName
	}

	return indexNames, nil
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
		if checkIndexes[i].State != string(queryIndexStateOnline) {
			logDebugf("Index not online: %s is in state %s", checkIndexes[i].Name, checkIndexes[i].State)
			return false, nil
		}
	}
	return true, nil
}

func (qpc *queryIndexProviderPs) WatchIndexes(c *Collection, bucketName string, watchList []string, timeout time.Duration, opts *WatchQueryIndexOptions,
) error {
	manager := qpc.newOpManager(opts.ParentSpan, "manager_query_watch_indexes", map[string]interface{}{})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	if opts.WatchPrimary {
		watchList = append(watchList, "#primary")
	}

	start := time.Now()
	deadline := start.Add(timeout)

	curInterval := 50 * time.Millisecond
	for {
		if deadline.Before(time.Now()) {
			return &TimeoutError{
				InnerError:   ErrUnambiguousTimeout,
				TimeObserved: time.Since(start),
			}
		}

		span := manager.NewSpan("manager_query_get_all_indexes")

		indexes, err := qpc.getAllIndexes(
			c,
			bucketName,
			manager,
			&GetAllQueryIndexesOptions{
				Timeout:        time.Until(deadline),
				RetryStrategy:  opts.RetryStrategy,
				ParentSpan:     span,
				ScopeName:      opts.ScopeName,
				CollectionName: opts.CollectionName,
				Context:        opts.Context,
			})
		span.End()
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
	if errors.Is(err, ErrInternalServerFailure) {
		var gocbErr *GenericError
		if errors.As(err, &gocbErr) {
			return qpc.tryParseErrorMessage(gocbErr)
		}
	}

	return err
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
