package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/google/uuid"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// AnalyticsIndexManager provides methods for performing Couchbase Analytics index management.
// Volatile: This API is subject to change at any time.
type AnalyticsIndexManager struct {
	httpClient   httpProvider
	executeQuery func(tracectx requestSpanContext, statement string, startTime time.Time,
		opts *AnalyticsOptions) (*AnalyticsResult, error)
	globalTimeout        time.Duration
	defaultRetryStrategy *retryStrategyWrapper
	tracer               requestTracer
}

// AnalyticsDataset contains information about an analytics dataset,
type AnalyticsDataset struct {
	Name          string `json:"DatasetName"`
	DataverseName string `json:"DataverseName"`
	LinkName      string `json:"LinkName"`
	BucketName    string `json:"BucketName"`
}

// AnalyticsIndex contains information about an analytics index,
type AnalyticsIndex struct {
	Name          string `json:"IndexName"`
	DatasetName   string `json:"DatasetName"`
	DataverseName string `json:"DataverseName"`
	IsPrimary     bool   `json:"IsPrimary"`
}

// CreateAnalyticsDataverseOptions is the set of options available to the AnalyticsManager CreateDataverse operation.
type CreateAnalyticsDataverseOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy

	IgnoreIfExists bool
}

// CreateDataverse creates a new analytics dataset.
func (am *AnalyticsIndexManager) CreateDataverse(dataverseName string, opts *CreateAnalyticsDataverseOptions) error {
	if dataverseName == "" {
		return invalidArgumentsError{
			message: "dataset name cannot be empty",
		}
	}

	startTime := time.Now()
	if opts == nil {
		opts = &CreateAnalyticsDataverseOptions{}
	}

	span := am.tracer.StartSpan("CreateDataverse", nil).SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfExists {
		ignoreStr = "IF NOT EXISTS"
	}

	q := fmt.Sprintf("CREATE DATAVERSE `%s` %s", dataverseName, ignoreStr)
	result, err := am.executeQuery(span.Context(), q, startTime, &AnalyticsOptions{
		Context:       ctx,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// DropAnalyticsDataverseOptions is the set of options available to the AnalyticsManager DropDataverse operation.
type DropAnalyticsDataverseOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy

	IgnoreIfNotExists bool
}

// DropDataverse drops an analytics dataset.
func (am *AnalyticsIndexManager) DropDataverse(dataverseName string, opts *DropAnalyticsDataverseOptions) error {
	startTime := time.Now()
	if opts == nil {
		opts = &DropAnalyticsDataverseOptions{}
	}

	span := am.tracer.StartSpan("DropDataverse", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfNotExists {
		ignoreStr = "IF EXISTS"
	}

	q := fmt.Sprintf("DROP DATAVERSE %s %s", dataverseName, ignoreStr)
	result, err := am.executeQuery(span.Context(), q, startTime, &AnalyticsOptions{
		Context:       ctx,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// CreateAnalyticsDatasetOptions is the set of options available to the AnalyticsManager CreateDataset operation.
type CreateAnalyticsDatasetOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy

	IgnoreIfExists bool
	// Condition can be used to set the WHERE clause for the dataset creation.
	Condition     string
	DataverseName string
}

// CreateDataset creates a new analytics dataset.
func (am *AnalyticsIndexManager) CreateDataset(datasetName, bucketName string, opts *CreateAnalyticsDatasetOptions) error {
	startTime := time.Now()
	if datasetName == "" {
		return invalidArgumentsError{
			message: "dataset name cannot be empty",
		}
	}

	span := am.tracer.StartSpan("CreateDataset", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	if opts == nil {
		opts = &CreateAnalyticsDatasetOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfExists {
		ignoreStr = "IF NOT EXISTS"
	}

	var where string
	if opts.Condition != "" {
		if !strings.HasPrefix(strings.ToUpper(opts.Condition), "WHERE") {
			where = "WHERE "
		}
		where += opts.Condition
	}

	if opts.DataverseName == "" {
		datasetName = fmt.Sprintf("`%s`", datasetName)
	} else {
		datasetName = fmt.Sprintf("`%s`.`%s`", opts.DataverseName, datasetName)
	}

	q := fmt.Sprintf("CREATE DATASET %s %s ON `%s` %s", ignoreStr, datasetName, bucketName, where)
	result, err := am.executeQuery(span.Context(), q, startTime, &AnalyticsOptions{
		Context:       ctx,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// DropAnalyticsDatasetOptions is the set of options available to the AnalyticsManager DropDataset operation.
type DropAnalyticsDatasetOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy

	IgnoreIfNotExists bool
	DataverseName     string
}

// DropDataset drops an analytics dataset.
func (am *AnalyticsIndexManager) DropDataset(datasetName string, opts *DropAnalyticsDatasetOptions) error {
	startTime := time.Now()
	if opts == nil {
		opts = &DropAnalyticsDatasetOptions{}
	}

	span := am.tracer.StartSpan("DropDataset", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfNotExists {
		ignoreStr = "IF EXISTS"
	}

	if opts.DataverseName == "" {
		datasetName = fmt.Sprintf("`%s`", datasetName)
	} else {
		datasetName = fmt.Sprintf("`%s`.`%s`", opts.DataverseName, datasetName)
	}

	q := fmt.Sprintf("DROP DATASET %s %s", datasetName, ignoreStr)
	result, err := am.executeQuery(span.Context(), q, startTime, &AnalyticsOptions{
		Context:       ctx,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// GetAllAnalyticsDatasetsOptions is the set of options available to the AnalyticsManager GetAllDatasets operation.
type GetAllAnalyticsDatasetsOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetAllDatasets gets all analytics datasets.
func (am *AnalyticsIndexManager) GetAllDatasets(opts *GetAllAnalyticsDatasetsOptions) ([]AnalyticsDataset, error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAllAnalyticsDatasetsOptions{}
	}

	span := am.tracer.StartSpan("GetAllDatasets", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	result, err := am.executeQuery(span.Context(),
		"SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"",
		startTime,
		&AnalyticsOptions{
			Context:       ctx,
			ReadOnly:      true,
			RetryStrategy: opts.RetryStrategy,
		})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return nil, analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return nil, err
	}

	var datasets []AnalyticsDataset
	var dataset AnalyticsDataset
	for result.Next(&dataset) {
		datasets = append(datasets, dataset)
	}

	err = result.Close()
	if err != nil {
		return nil, err
	}

	return datasets, nil
}

// CreateAnalyticsIndexOptions is the set of options available to the AnalyticsManager CreateIndex operation.
type CreateAnalyticsIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy

	IgnoreIfExists bool
	DataverseName  string
}

// CreateIndex creates a new analytics dataset.
func (am *AnalyticsIndexManager) CreateIndex(datasetName, indexName string, fields map[string]string, opts *CreateAnalyticsIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{
			message: "index name cannot be empty",
		}
	}
	if len(fields) <= 0 {
		return invalidArgumentsError{
			message: "you must specify at least one field to index",
		}
	}

	startTime := time.Now()
	if opts == nil {
		opts = &CreateAnalyticsIndexOptions{}
	}

	span := am.tracer.StartSpan("CreateIndex", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfExists {
		ignoreStr = "IF NOT EXISTS"
	}

	var indexFields []string
	for name, typ := range fields {
		indexFields = append(indexFields, name+":"+typ)
	}

	if opts.DataverseName == "" {
		datasetName = fmt.Sprintf("`%s`", datasetName)
	} else {
		datasetName = fmt.Sprintf("`%s`.`%s`", opts.DataverseName, datasetName)
	}

	q := fmt.Sprintf("CREATE INDEX `%s` %s ON %s (%s)", indexName, ignoreStr, datasetName, strings.Join(indexFields, ","))
	result, err := am.executeQuery(span.Context(), q, startTime, &AnalyticsOptions{
		Context:       ctx,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// DropAnalyticsIndexOptions is the set of options available to the AnalyticsManager DropIndex operation.
type DropAnalyticsIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy

	IgnoreIfNotExists bool
	DataverseName     string
}

// DropIndex drops an analytics index.
func (am *AnalyticsIndexManager) DropIndex(datasetName, indexName string, opts *DropAnalyticsIndexOptions) error {
	startTime := time.Now()
	if opts == nil {
		opts = &DropAnalyticsIndexOptions{}
	}

	span := am.tracer.StartSpan("DropIndex", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfNotExists {
		ignoreStr = "IF EXISTS"
	}

	if opts.DataverseName == "" {
		datasetName = fmt.Sprintf("`%s`", datasetName)
	} else {
		datasetName = fmt.Sprintf("`%s`.`%s`", opts.DataverseName, datasetName)
	}

	q := fmt.Sprintf("DROP INDEX %s.%s %s", datasetName, indexName, ignoreStr)
	result, err := am.executeQuery(span.Context(), q, startTime, &AnalyticsOptions{
		Context:       ctx,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// GetAllAnalyticsIndexesOptions is the set of options available to the AnalyticsManager GetAllIndexes operation.
type GetAllAnalyticsIndexesOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetAllIndexes gets all analytics indexes.
func (am *AnalyticsIndexManager) GetAllIndexes(opts *GetAllAnalyticsIndexesOptions) ([]AnalyticsIndex, error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAllAnalyticsIndexesOptions{}
	}

	span := am.tracer.StartSpan("GetAllIndexes", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	result, err := am.executeQuery(span.Context(),
		"SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> \"Metadata\"",
		startTime,
		&AnalyticsOptions{
			Context:       ctx,
			RetryStrategy: opts.RetryStrategy,
			ReadOnly:      true,
		})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return nil, analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return nil, err
	}

	var indexes []AnalyticsIndex
	var index AnalyticsIndex
	for result.Next(&index) {
		indexes = append(indexes, index)
	}

	err = result.Close()
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

// ConnectAnalyticsLinkOptions is the set of options available to the AnalyticsManager ConnectLink operation.
type ConnectAnalyticsLinkOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
	// Name of the link, if empty defaults to Local
	LinkName string
}

// ConnectLink connects an analytics link.
func (am *AnalyticsIndexManager) ConnectLink(opts *ConnectAnalyticsLinkOptions) error {
	startTime := time.Now()
	if opts == nil {
		opts = &ConnectAnalyticsLinkOptions{}
	}

	span := am.tracer.StartSpan("ConnectLink", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	if opts.LinkName == "" {
		opts.LinkName = "Local"
	}

	result, err := am.executeQuery(span.Context(),
		fmt.Sprintf("CONNECT LINK %s", opts.LinkName),
		startTime,
		&AnalyticsOptions{
			Context:       ctx,
			RetryStrategy: opts.RetryStrategy,
		})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// DisconnectAnalyticsLinkOptions is the set of options available to the AnalyticsManager DisconnectLink operation.
type DisconnectAnalyticsLinkOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
	// Name of the link, if empty defaults to Local
	LinkName string
}

// DisconnectLink disconnects an analytics link.
func (am *AnalyticsIndexManager) DisconnectLink(opts *DisconnectAnalyticsLinkOptions) error {
	startTime := time.Now()
	if opts == nil {
		opts = &DisconnectAnalyticsLinkOptions{}
	}

	span := am.tracer.StartSpan("DisconnectLink", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	if opts.LinkName == "" {
		opts.LinkName = "Local"
	}

	result, err := am.executeQuery(span.Context(),
		fmt.Sprintf("DISCONNECT LINK %s", opts.LinkName),
		startTime,
		&AnalyticsOptions{
			Context:       ctx,
			RetryStrategy: opts.RetryStrategy,
		})
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if ok {
			return analyticsIndexesError{
				statusCode:    aErr.HTTPStatus(),
				message:       aErr.Message(),
				analyticsCode: aErr.Code(),
			}
		}
		return err
	}

	return result.Close()
}

// GetPendingMutationsAnalyticsOptions is the set of options available to the user manager GetPendingMutations operation.
type GetPendingMutationsAnalyticsOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetPendingMutations returns the number of pending mutations for all indexes in the form of dataverse.dataset:mutations.
func (am *AnalyticsIndexManager) GetPendingMutations(opts *GetPendingMutationsAnalyticsOptions) (map[string]int, error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetPendingMutationsAnalyticsOptions{}
	}

	span := am.tracer.StartSpan("GetPendingMutations", nil).
		SetTag("couchbase.service", "cbas")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, am.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := am.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(AnalyticsService),
		Method:        "GET",
		Path:          fmt.Sprintf("/analytics/node/agg/stats/remaining"),
		Context:       ctx,
		RetryStrategy: retryStrategy,
		UniqueId:      uuid.New().String(),
	}

	dspan := am.tracer.StartSpan("dispatch", span.Context())
	resp, err := am.httpClient.DoHttpRequest(req)
	dspan.Finish()
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "cbas",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, analyticsIndexesError{
			statusCode: resp.StatusCode,
			message:    string(data),
		}
	}

	pending := make(map[string]int)
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&pending)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return pending, nil
}
