package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pkg/errors"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// ViewIndexManager provides methods for performing View management.
// Volatile: This API is subject to change at any time.
type ViewIndexManager struct {
	bucketName string
	httpClient httpProvider
}

// View represents a Couchbase view within a design document.
type View struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

func (v View) hasReduce() bool {
	return v.Reduce != ""
}

// DesignDocument represents a Couchbase design document containing multiple views.
type DesignDocument struct {
	Name  string          `json:"-"`
	Views map[string]View `json:"views,omitempty"`
}

// GetViewIndexOptions is the set of options available to the ViewIndexManager Get operation.
type GetViewIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IsProduction bool
}

func (vm *ViewIndexManager) ddocName(name string, isProd bool) string {
	if isProd {
		if strings.HasPrefix(name, "dev_") {
			name = strings.TrimLeft(name, "dev_")
		}
	} else {
		if !strings.HasPrefix(name, "dev_") {
			name = "dev_" + name
		}
	}

	return name
}

// Get retrieves a single design document for the given bucket.
func (vm *ViewIndexManager) Get(name string, opts *GetViewIndexOptions) (*DesignDocument, error) {
	if opts == nil {
		opts = &GetViewIndexOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	name = vm.ddocName(name, opts.IsProduction)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/_design/%s", name),
		Method:  "GET",
		Context: ctx,
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}

		return nil, viewIndexError{
			statusCode:   resp.StatusCode,
			message:      string(data),
			indexMissing: resp.StatusCode == 404,
		}
	}

	ddocObj := DesignDocument{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&ddocObj)
	if err != nil {
		return nil, err
	}

	ddocObj.Name = name
	return &ddocObj, nil
}

// GetAllViewIndexOptions is the set of options available to the ViewIndexManager GetAll operation.
type GetAllViewIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetAll will retrieve all design documents for the given bucket.
func (vm *ViewIndexManager) GetAll(opts *GetAllViewIndexOptions) ([]*DesignDocument, error) {
	if opts == nil {
		opts = &GetAllViewIndexOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s/ddocs", vm.bucketName),
		Method:  "GET",
		Context: ctx,
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, viewIndexError{statusCode: resp.StatusCode, message: string(data)}
	}

	var ddocsObj struct {
		Rows []struct {
			Doc struct {
				Meta struct {
					Id string
				}
				Json DesignDocument
			}
		}
	}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&ddocsObj)
	if err != nil {
		return nil, err
	}

	var ddocs []*DesignDocument
	for index, ddocData := range ddocsObj.Rows {
		ddoc := &ddocsObj.Rows[index].Doc.Json
		ddoc.Name = ddocData.Doc.Meta.Id[8:]
		ddocs = append(ddocs, ddoc)
	}

	return ddocs, nil
}

// CreateViewIndexOptions is the set of options available to the ViewIndexManager Create operation.
type CreateViewIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IsProduction bool
}

// Create inserts a design document to the given bucket.
func (vm *ViewIndexManager) Create(ddoc DesignDocument, opts *CreateViewIndexOptions) error {
	if opts == nil {
		opts = &CreateViewIndexOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	_, err := vm.Get(ddoc.Name, &GetViewIndexOptions{
		Context:      ctx,
		IsProduction: opts.IsProduction,
	})
	if err == nil {
		// If there's no error then design doc exists.
		return viewIndexError{message: "Design document already exists", indexExists: true}
	}
	indexErr, ok := err.(viewIndexError)
	if ok {
		if !indexErr.indexMissing {
			// If the error isn't index missing then it's an actual error.
			return viewIndexError{message: "Design document already exists", indexExists: true}
		}
	} else {
		// If the error isn't a view index error then it's an actual error.
		return err
	}

	return vm.Upsert(ddoc, &UpsertViewIndexOptions{
		Context:      ctx,
		IsProduction: opts.IsProduction,
	})
}

// UpsertViewIndexOptions is the set of options available to the ViewIndexManager Upsert operation.
type UpsertViewIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IsProduction bool
}

// Upsert will insert a design document to the given bucket, or update
// an existing design document with the same name.
func (vm *ViewIndexManager) Upsert(ddoc DesignDocument, opts *UpsertViewIndexOptions) error {
	if opts == nil {
		opts = &UpsertViewIndexOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	data, err := json.Marshal(&ddoc)
	if err != nil {
		return err
	}

	ddoc.Name = vm.ddocName(ddoc.Name, opts.IsProduction)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/_design/%s", ddoc.Name),
		Method:  "PUT",
		Body:    data,
		Context: ctx,
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 201 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return viewIndexError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}

// DropViewIndexOptions is the set of options available to the ViewIndexManager Upsert operation.
type DropViewIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IsProduction bool
}

// Drop will remove a design document from the given bucket.
func (vm *ViewIndexManager) Drop(name string, opts *DropViewIndexOptions) error {
	if opts == nil {
		opts = &DropViewIndexOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	name = vm.ddocName(name, opts.IsProduction)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/_design/%s", name),
		Method:  "DELETE",
		Context: ctx,
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
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
		return viewIndexError{
			statusCode:   resp.StatusCode,
			message:      string(data),
			indexMissing: resp.StatusCode == 404,
		}
	}

	return nil
}

// PublishViewIndexOptions is the set of options available to the ViewIndexManager Publish operation.
type PublishViewIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Publish publishes a design document to the given bucket.
func (vm *ViewIndexManager) Publish(name string, opts *PublishViewIndexOptions) error {
	if opts == nil {
		opts = &PublishViewIndexOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	devdoc, err := vm.Get(name, &GetViewIndexOptions{
		Context:      ctx,
		IsProduction: false,
	})
	if err != nil {
		indexErr, ok := err.(viewIndexError)
		if ok {
			if indexErr.indexMissing {
				return viewIndexError{message: "Development design document does not exist", indexMissing: true}
			}
		}
		return err
	}

	err = vm.Upsert(*devdoc, &UpsertViewIndexOptions{
		Context:      ctx,
		IsProduction: true,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create ")
	}

	err = vm.Drop(devdoc.Name, &DropViewIndexOptions{
		Context:      ctx,
		IsProduction: false,
	})
	if err != nil {
		return viewIndexError{message: fmt.Sprintf("failed to drop development index: %v", err), publishDropFail: true}
	}

	return nil
}
