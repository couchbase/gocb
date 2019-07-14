package gocb

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

// CollectionManager provides methods for performing collections management.
type CollectionManager struct {
	httpClient httpProvider
	bucketName string
}

// CreateCollectionOptions is the set of options available to the CreateCollection operation.
type CreateCollectionOptions struct {
	Timeout time.Duration
	Context context.Context
}

// CreateCollection creates a new collection on the bucket.
func (cm *CollectionManager) CreateCollection(scopeName, collectionName string, opts *CreateCollectionOptions) error {
	if collectionName == "" {
		return invalidArgumentsError{
			message: "collection name cannot be empty",
		}
	}

	if opts == nil {
		opts = &CreateCollectionOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		cancel()
	}

	posts := url.Values{}
	posts.Add("name", collectionName)

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        fmt.Sprintf("/pools/default/buckets/%s/collections/%s", cm.bucketName, scopeName),
		Method:      "POST",
		Body:        []byte(posts.Encode()),
		ContentType: "application/x-www-form-urlencoded",
		Context:     ctx,
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
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
	Timeout time.Duration
	Context context.Context
}

// DropCollection removes a collection.
func (cm *CollectionManager) DropCollection(scopeName, collectionName string, opts *DropCollectionOptions) error {
	if opts == nil {
		opts = &DropCollectionOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", cm.bucketName, scopeName, collectionName),
		Method:  "DELETE",
		Context: ctx,
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
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
	Timeout time.Duration
	Context context.Context
}

// CreateScope creates a new scope on the bucket.
func (cm *CollectionManager) CreateScope(scopeName string, opts *CreateScopeOptions) error {
	if scopeName == "" {
		return invalidArgumentsError{
			message: "scope name cannot be empty",
		}
	}

	if opts == nil {
		opts = &CreateScopeOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		cancel()
	}

	posts := url.Values{}
	posts.Add("name", scopeName)

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        fmt.Sprintf("/pools/default/buckets/%s/collections", cm.bucketName),
		Method:      "POST",
		Body:        []byte(posts.Encode()),
		ContentType: "application/x-www-form-urlencoded",
		Context:     ctx,
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
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
	Timeout time.Duration
	Context context.Context
}

// DropScope removes a scope.
func (cm *CollectionManager) DropScope(scopeName string, opts *DropScopeOptions) error {
	if opts == nil {
		opts = &DropScopeOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s", cm.bucketName, scopeName),
		Method:  "DELETE",
		Context: ctx,
	}

	resp, err := cm.httpClient.DoHttpRequest(req)
	if err != nil {
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
