package gocb

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v8"
)

// CollectionSpec describes the specification of a collection.
type CollectionSpec struct {
	Name      string
	ScopeName string
}

// ScopeSpec describes the specification of a scope.
type ScopeSpec struct {
	Name        string
	Collections []CollectionSpec
}

// These 3 types are temporary. They are necessary for now as the server beta was released with ns_server returning
// a different jsonManifest format to what it will return in the future.
type jsonManifest struct {
	UID    uint64                       `json:"uid"`
	Scopes map[string]jsonManifestScope `json:"scopes"`
}

type jsonManifestScope struct {
	UID         uint32                            `json:"uid"`
	Collections map[string]jsonManifestCollection `json:"collections"`
}

type jsonManifestCollection struct {
	UID uint32 `json:"uid"`
}

// CollectionManager provides methods for performing collections management.
type CollectionManager struct {
	httpClient           httpProvider
	bucketName           string
	globalTimeout        time.Duration
	defaultRetryStrategy *retryStrategyWrapper
	tracer               requestTracer
}

// GetAllScopesOptions is the set of options available to the GetAllScopes operation.
type GetAllScopesOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAllScopes gets all scopes from the bucket.
func (cm *CollectionManager) GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error) {
	if opts == nil {
		opts = &GetAllScopesOptions{}
	}

	span := cm.tracer.StartSpan("GetAllScopes", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "GET",
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueID:      uuid.New().String(),
	}

	dspan := cm.tracer.StartSpan("dispatch", span.Context())
	resp, err := cm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
	}()

	if resp.StatusCode != 200 {
		return nil, makeHTTPBadStatusError("failed to get all scopes", req, resp)
	}

	var scopes []ScopeSpec
	var mfest gocbcore.Manifest
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&mfest)
	if err == nil {
		for _, scope := range mfest.Scopes {
			var collections []CollectionSpec
			for _, col := range scope.Collections {
				collections = append(collections, CollectionSpec{
					Name:      col.Name,
					ScopeName: scope.Name,
				})
			}
			scopes = append(scopes, ScopeSpec{
				Name:        scope.Name,
				Collections: collections,
			})
		}
	} else {
		// Temporary support for older server version
		var oldMfest jsonManifest
		jsonDec := json.NewDecoder(resp.Body)
		err = jsonDec.Decode(&oldMfest)
		if err != nil {
			return nil, err
		}

		for scopeName, scope := range oldMfest.Scopes {
			var collections []CollectionSpec
			for colName := range scope.Collections {
				collections = append(collections, CollectionSpec{
					Name:      colName,
					ScopeName: scopeName,
				})
			}
			scopes = append(scopes, ScopeSpec{
				Name:        scopeName,
				Collections: collections,
			})
		}
	}

	return scopes, nil
}

// CreateCollectionOptions is the set of options available to the CreateCollection operation.
type CreateCollectionOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
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

	span := cm.tracer.StartSpan("CreateCollection", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts := url.Values{}
	posts.Add("name", spec.Name)

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections/%s", cm.bucketName, spec.ScopeName),
		Method:        "POST",
		Body:          []byte(posts.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := cm.tracer.StartSpan("dispatch", span.Context())
	resp, err := cm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		errBody := tryReadHTTPBody(resp)
		errText := strings.ToLower(errBody)

		if strings.Contains(errText, "already exists") && strings.Contains(errText, "collection") {
			return makeGenericHTTPError(ErrCollectionExists, req, resp)
		}

		if strings.Contains(errText, "not found") && strings.Contains(errText, "scope") {
			return makeGenericHTTPError(ErrScopeNotFound, req, resp)
		}

		return makeHTTPBadStatusError("failed to create collection", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// DropCollectionOptions is the set of options available to the DropCollection operation.
type DropCollectionOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
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

	span := cm.tracer.StartSpan("DropCollection", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", cm.bucketName, spec.ScopeName, spec.Name),
		Method:        "DELETE",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := cm.tracer.StartSpan("dispatch", span.Context())
	resp, err := cm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		errBody := tryReadHTTPBody(resp)
		errText := strings.ToLower(errBody)

		if strings.Contains(errText, "not found") && strings.Contains(errText, "collection") {
			return makeGenericHTTPError(ErrCollectionNotFound, req, resp)
		}

		return makeHTTPBadStatusError("failed to drop collection", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// CreateScopeOptions is the set of options available to the CreateScope operation.
type CreateScopeOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// CreateScope creates a new scope on the bucket.
func (cm *CollectionManager) CreateScope(scopeName string, opts *CreateScopeOptions) error {
	if scopeName == "" {
		return makeInvalidArgumentsError("scope name cannot be empty")
	}

	if opts == nil {
		opts = &CreateScopeOptions{}
	}

	span := cm.tracer.StartSpan("CreateScope", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts := url.Values{}
	posts.Add("name", scopeName)

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "POST",
		Body:          []byte(posts.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := cm.tracer.StartSpan("dispatch", span.Context())
	resp, err := cm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		errBody := tryReadHTTPBody(resp)
		errText := strings.ToLower(errBody)

		if strings.Contains(errText, "already exists") && strings.Contains(errText, "scope") {
			return makeGenericHTTPError(ErrScopeExists, req, resp)
		}

		return makeHTTPBadStatusError("failed to create scope", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// DropScopeOptions is the set of options available to the DropScope operation.
type DropScopeOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// DropScope removes a scope.
func (cm *CollectionManager) DropScope(scopeName string, opts *DropScopeOptions) error {
	if opts == nil {
		opts = &DropScopeOptions{}
	}

	span := cm.tracer.StartSpan("DropScope", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections/%s", cm.bucketName, scopeName),
		Method:        "DELETE",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := cm.tracer.StartSpan("dispatch", span.Context())
	resp, err := cm.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode != 200 {
		errBody := tryReadHTTPBody(resp)
		errText := strings.ToLower(errBody)

		if strings.Contains(errText, "not found") && strings.Contains(errText, "scope") {
			return makeGenericHTTPError(ErrScopeNotFound, req, resp)
		}

		return makeHTTPBadStatusError("failed to drop scope", req, resp)
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}
