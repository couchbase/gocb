package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v8"
)

// CollectionManager provides methods for performing collections management.
// Volatile: This API is subject to change at any time.
type CollectionManager struct {
	httpClient           httpProvider
	bucketName           string
	globalTimeout        time.Duration
	defaultRetryStrategy *retryStrategyWrapper
}

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

// CollectionExistsOptions is the set of options available to the CollectionExists operation.
type CollectionExistsOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// These 3 types are temporary. They are necessary for now as the server beta was released with ns_server returning
// a different manifest format to what it will return in the future.
type manifest struct {
	UID    uint64                   `json:"uid"`
	Scopes map[string]manifestScope `json:"scopes"`
}

type manifestScope struct {
	UID         uint32                        `json:"uid"`
	Collections map[string]manifestCollection `json:"collections"`
}

type manifestCollection struct {
	UID uint32 `json:"uid"`
}

// CollectionExists verifies whether or not a collection exists on the bucket.
func (cm *CollectionManager) CollectionExists(spec CollectionSpec, opts *CollectionExistsOptions) (bool, error) {
	startTime := time.Now()
	if spec.Name == "" {
		return false, invalidArgumentsError{
			message: "collection name cannot be empty",
		}
	}

	if spec.ScopeName == "" {
		return false, invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &CollectionExistsOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts := url.Values{}
	posts.Add("name", spec.Name)

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "GET",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return false, timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return false, err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
	}()

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}

		bodyMessage := string(data)
		return false, collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
	}

	var mfest gocbcore.Manifest
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&mfest)
	if err == nil {
		for _, scope := range mfest.Scopes {
			if scope.Name == spec.ScopeName {
				for _, col := range scope.Collections {
					if col.Name == spec.Name {
						return true, nil
					}
					break
				}
				break
			}
		}
	} else {
		// Temporary support for older server version
		var oldMfest manifest
		jsonDec := json.NewDecoder(resp.Body)
		err = jsonDec.Decode(&oldMfest)
		if err != nil {
			return false, err
		}

		for scopeName, scope := range oldMfest.Scopes {
			if scopeName == spec.ScopeName {
				for colName := range scope.Collections {
					if colName == spec.Name {
						return true, nil
					}
					break
				}
				break
			}
		}
	}
	return false, nil
}

// ScopeExistsOptions is the set of options available to the ScopeExists operation.
type ScopeExistsOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// ScopeExists verifies whether or not a scope exists on the bucket.
func (cm *CollectionManager) ScopeExists(scopeName string, opts *ScopeExistsOptions) (bool, error) {
	startTime := time.Now()
	if scopeName == "" {
		return false, invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &ScopeExistsOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "GET",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return false, timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return false, err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
	}()

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}

		bodyMessage := string(data)
		return false, collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
	}

	var mfest gocbcore.Manifest
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&mfest)
	if err == nil {
		for _, scope := range mfest.Scopes {
			if scope.Name == scopeName {
				return true, nil
			}
		}
	} else {
		// Temporary support for older server version
		var oldMfest manifest
		jsonDec := json.NewDecoder(resp.Body)
		err = jsonDec.Decode(&oldMfest)
		if err != nil {
			return false, err
		}

		for scopeName := range oldMfest.Scopes {
			if scopeName == scopeName {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetScopeOptions is the set of options available to the GetScope operation.
type GetScopeOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetScope gets a scope from the bucket.
func (cm *CollectionManager) GetScope(scopeName string, opts *GetScopeOptions) (*ScopeSpec, error) {
	startTime := time.Now()
	if scopeName == "" {
		return nil, invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &GetScopeOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "GET",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return nil, err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
	}()

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		bodyMessage := string(data)
		return nil, collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
	}

	var collections []CollectionSpec
	var mfest gocbcore.Manifest
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&mfest)
	if err == nil {
		var selectedScope gocbcore.ManifestScope
		for _, scope := range mfest.Scopes {
			if scope.Name == scopeName {
				selectedScope = scope
			}
		}

		if selectedScope.Name == "" {
			// Fake a scope not found error.
			return nil, collectionMgrError{
				statusCode: 404,
				message:    "scope not found",
			}
		}

		for _, col := range selectedScope.Collections {
			collections = append(collections, CollectionSpec{
				Name:      col.Name,
				ScopeName: scopeName,
			})
		}
	} else {
		// Temporary support for older server version
		var oldMfest manifest
		jsonDec := json.NewDecoder(resp.Body)
		err = jsonDec.Decode(&oldMfest)
		if err != nil {
			return nil, err
		}

		var selectedScope manifestScope
		for scopeName, scope := range oldMfest.Scopes {
			if scopeName == scopeName {
				selectedScope = scope
			}
		}

		for colName := range selectedScope.Collections {
			collections = append(collections, CollectionSpec{
				Name:      colName,
				ScopeName: scopeName,
			})
		}
	}

	return &ScopeSpec{
		Name:        scopeName,
		Collections: collections,
	}, nil
}

// GetAllScopesOptions is the set of options available to the GetAllScopes operation.
type GetAllScopesOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetAllScopes gets all scopes from the bucket.
func (cm *CollectionManager) GetAllScopes(opts *GetAllScopesOptions) ([]ScopeSpec, error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAllScopesOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "GET",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return nil, err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
	}()

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		bodyMessage := string(data)
		return nil, collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
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
		var oldMfest manifest
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
	Context       context.Context
	RetryStrategy RetryStrategy
}

// CreateCollection creates a new collection on the bucket.
func (cm *CollectionManager) CreateCollection(spec CollectionSpec, opts *CreateCollectionOptions) error {
	startTime := time.Now()
	if spec.Name == "" {
		return invalidArgumentsError{
			message: "collection name cannot be empty",
		}
	}

	if spec.ScopeName == "" {
		return invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &CreateCollectionOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts := url.Values{}
	posts.Add("name", spec.Name)

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections/%s", cm.bucketName, spec.ScopeName),
		Method:        "POST",
		Body:          []byte(posts.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		bodyMessage := string(data)
		return collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
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
	Context       context.Context
	RetryStrategy RetryStrategy
}

// DropCollection removes a collection.
func (cm *CollectionManager) DropCollection(spec CollectionSpec, opts *DropCollectionOptions) error {
	startTime := time.Now()
	if spec.Name == "" {
		return invalidArgumentsError{
			message: "collection name cannot be empty",
		}
	}

	if spec.ScopeName == "" {
		return invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &DropCollectionOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", cm.bucketName, spec.ScopeName, spec.Name),
		Method:        "DELETE",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		bodyMessage := string(data)
		return collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
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
	Context       context.Context
	RetryStrategy RetryStrategy
}

// CreateScope creates a new scope on the bucket.
func (cm *CollectionManager) CreateScope(scopeName string, opts *CreateScopeOptions) error {
	startTime := time.Now()
	if scopeName == "" {
		return invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &CreateScopeOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	posts := url.Values{}
	posts.Add("name", scopeName)

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:        "POST",
		Body:          []byte(posts.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		bodyMessage := string(data)
		return collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
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
	Context       context.Context
	RetryStrategy RetryStrategy
}

// DropScope removes a scope.
func (cm *CollectionManager) DropScope(scopeName string, opts *DropScopeOptions) error {
	startTime := time.Now()
	if opts == nil {
		opts = &DropScopeOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout, cm.globalTimeout)
	if cancel != nil {
		defer cancel()
	}

	retryStrategy := cm.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Path:          fmt.Sprintf("/pools/default/buckets/%s/collections/%s", cm.bucketName, scopeName),
		Method:        "DELETE",
		Context:       ctx,
		RetryStrategy: retryStrategy,
		UniqueId:      uuid.New().String(),
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
		if err == context.DeadlineExceeded {
			return timeoutError{
				operationID:   req.UniqueId,
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				operation:     "mgmt",
				elapsed:       time.Now().Sub(startTime),
			}
		}

		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		bodyMessage := string(data)
		return collectionMgrError{
			message:    bodyMessage,
			statusCode: resp.StatusCode,
		}
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}
