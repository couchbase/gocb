package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	opentracing "github.com/opentracing/opentracing-go"
)

// AnalyticsIndexManager provides methods for performing Couchbase Analytics index management.
// Experimental: This API is subject to change at any time.
type AnalyticsIndexManager struct {
	httpClient   httpProvider
	executeQuery func(statement string, opts *AnalyticsQueryOptions) (*AnalyticsResults, error)
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

// CreateAnalyticsDatasetOptions is the set of options available to the AnalyticsManager CreateDataset operation.
type CreateAnalyticsDatasetOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	IgnoreIfExists bool
	// Condition can be used to set the WHERE clause for the dataset creation.
	Condition string
}

// CreateDataset creates a new analytics dataset.
func (am *AnalyticsIndexManager) CreateDataset(bucketName, datasetName string, opts *CreateAnalyticsDatasetOptions) error {
	if datasetName == "" {
		return invalidArgumentsError{
			message: "dataset name cannot be empty",
		}
	}

	if opts == nil {
		opts = &CreateAnalyticsDatasetOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "CreateDataset", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
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

	q := fmt.Sprintf("CREATE DATASET %s `%s` ON `%s` %s", ignoreStr, datasetName, bucketName, where)
	result, err := am.executeQuery(q, &AnalyticsQueryOptions{
		Context:           ctx,
		ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	IgnoreIfNotExists bool
}

// DropDataset drops an analytics dataset.
func (am *AnalyticsIndexManager) DropDataset(datasetName string, opts *DropAnalyticsDatasetOptions) error {
	if opts == nil {
		opts = &DropAnalyticsDatasetOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "DropDataset", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfNotExists {
		ignoreStr = "IF EXISTS"
	}

	q := fmt.Sprintf("DROP DATASET %s %s", datasetName, ignoreStr)
	result, err := am.executeQuery(q, &AnalyticsQueryOptions{
		Context:           ctx,
		ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetAllDatasets gets all analytics datasets.
func (am *AnalyticsIndexManager) GetAllDatasets(opts *GetAllAnalyticsDatasetsOptions) ([]AnalyticsDataset, error) {
	if opts == nil {
		opts = &GetAllAnalyticsDatasetsOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "GetAllDatasets", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	result, err := am.executeQuery(
		"SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"",
		&AnalyticsQueryOptions{
			Context:           ctx,
			ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	IgnoreIfExists bool
}

// CreateIndex creates a new analytics dataset.
func (am *AnalyticsIndexManager) CreateIndex(datasetName string, indexName string, fields map[string]string, opts *CreateAnalyticsIndexOptions) error {
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

	if opts == nil {
		opts = &CreateAnalyticsIndexOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "CreateIndex", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
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

	q := fmt.Sprintf("CREATE INDEX `%s` %s ON `%s` (%s)", indexName, ignoreStr, datasetName, strings.Join(indexFields, ","))
	result, err := am.executeQuery(q, &AnalyticsQueryOptions{
		Context:           ctx,
		ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	IgnoreIfNotExists bool
}

// DropIndex drops an analytics index.
func (am *AnalyticsIndexManager) DropIndex(datasetName, indexName string, opts *DropAnalyticsIndexOptions) error {
	if opts == nil {
		opts = &DropAnalyticsIndexOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "DropIndex", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	var ignoreStr string
	if opts.IgnoreIfNotExists {
		ignoreStr = "IF EXISTS"
	}

	q := fmt.Sprintf("DROP INDEX %s.%s %s", datasetName, indexName, ignoreStr)
	result, err := am.executeQuery(q, &AnalyticsQueryOptions{
		Context:           ctx,
		ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetAllIndexes gets all analytics indexes.
func (am *AnalyticsIndexManager) GetAllIndexes(opts *GetAllAnalyticsIndexesOptions) ([]AnalyticsIndex, error) {
	if opts == nil {
		opts = &GetAllAnalyticsIndexesOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "DropIndex", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	result, err := am.executeQuery(
		"SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> \"Metadata\"",
		&AnalyticsQueryOptions{
			Context:           ctx,
			ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// ConnectLink connects an analytics link.
func (am *AnalyticsIndexManager) ConnectLink(linkName string, opts *ConnectAnalyticsLinkOptions) error {
	if linkName == "" {
		return invalidArgumentsError{
			message: "link name cannot be empty",
		}
	}

	if opts == nil {
		opts = &ConnectAnalyticsLinkOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "ConnectLink", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	result, err := am.executeQuery(
		fmt.Sprintf("CONNECT LINK %s", linkName),
		&AnalyticsQueryOptions{
			Context:           ctx,
			ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// DisconnectLink disconnects an analytics link.
func (am *AnalyticsIndexManager) DisconnectLink(linkName string, opts *DisconnectAnalyticsLinkOptions) error {
	if opts == nil {
		opts = &DisconnectAnalyticsLinkOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "DisconnectLink", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	result, err := am.executeQuery(
		fmt.Sprintf("DISCONNECT LINK %s", linkName),
		&AnalyticsQueryOptions{
			Context:           ctx,
			ParentSpanContext: span.Context(),
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetPendingMutations returns the number of pending mutations for all indexes in the form of dataverse.dataset:mutations.
func (am *AnalyticsIndexManager) GetPendingMutations(opts *GetPendingMutationsAnalyticsOptions) (map[string]int, error) {
	if opts == nil {
		opts = &GetPendingMutationsAnalyticsOptions{}
	}

	span := startSpan(opts.ParentSpanContext, "GetPendingMutations", "analyticsidxmgr")
	defer span.Finish()

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(AnalyticsService),
		Method:  "GET",
		Path:    fmt.Sprintf("/analytics/node/agg/stats/remaining"),
		Context: ctx,
	}

	resp, err := am.httpClient.DoHttpRequest(req)
	if err != nil {
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
