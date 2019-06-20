package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/opentracing/opentracing-go"
)

// UserManager provides methods for performing Couchbase user management.
type UserManager struct {
	httpClient httpProvider
}

// UserRole represents a role for a particular user on the server.
type UserRole struct {
	Role       string
	BucketName string
}

// User represents a user which was retrieved from the server.
type User struct {
	ID    string
	Name  string
	Type  string
	Roles []UserRole
}

// AuthDomain specifies the user domain of a specific user
type AuthDomain string

const (
	// LocalDomain specifies users that are locally stored in Couchbase.
	LocalDomain AuthDomain = "local"

	// ExternalDomain specifies users that are externally stored
	// (in LDAP for instance).
	ExternalDomain = "external"
)

type userRoleJson struct {
	Role       string `json:"role"`
	BucketName string `json:"bucket_name"`
}

type userJson struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Type  string         `json:"type"`
	Roles []userRoleJson `json:"roles"`
}

func transformUserJson(userData *userJson) User {
	var user User
	user.ID = userData.ID
	user.Name = userData.Name
	user.Type = userData.Type
	for _, roleData := range userData.Roles {
		user.Roles = append(user.Roles, UserRole{
			Role:       roleData.Role,
			BucketName: roleData.BucketName,
		})
	}
	return user
}

// GetAllUsersOptions is the set of options available to the user manager GetAll operation.
type GetAllUsersOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	DomainName string
}

// GetAll returns a list of all the users from the cluster.
func (um *UserManager) GetAll(opts *GetAllUsersOptions) ([]*User, error) {
	if opts == nil {
		opts = &GetAllUsersOptions{}
	}

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("GetAll",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("GetAll",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

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
		Method:  "GET",
		Path:    fmt.Sprintf("/settings/rbac/users/%s", opts.DomainName),
		Context: ctx,
	}

	resp, err := um.httpClient.DoHttpRequest(req)
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
		return nil, clientError{message: string(data)} // TODO: proper error
	}

	var usersData []*userJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&usersData)
	if err != nil {
		return nil, err
	}

	var users []*User
	for _, userData := range usersData {
		user := transformUserJson(userData)
		users = append(users, &user)
	}

	return users, nil
}

// GetUserOptions is the set of options available to the user manager Get operation.
type GetUserOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	DomainName string
}

// Get returns the data for a particular user
func (um *UserManager) Get(name string, opts *GetUserOptions) (*User, error) {
	if opts == nil {
		opts = &GetUserOptions{}
	}

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("Get",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("Get",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

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
		Method:  "GET",
		Path:    fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, name),
		Context: ctx,
	}

	resp, err := um.httpClient.DoHttpRequest(req)
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
		return nil, clientError{message: string(data)} // TODO: proper error
	}

	var userData userJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&userData)
	if err != nil {
		return nil, err
	}

	user := transformUserJson(&userData)
	return &user, nil
}

// UpsertUserOptions is the set of options available to the user manager Upsert operation.
type UpsertUserOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	DomainName string
}

// Upsert updates a built-in RBAC user on the cluster.
func (um *UserManager) Upsert(name string, password string, roles []UserRole, opts *UpsertUserOptions) error {
	if opts == nil {
		opts = &UpsertUserOptions{}
	}

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("Upsert",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("Upsert",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	var reqRoleStrs []string
	for _, roleData := range roles {
		reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s[%s]", roleData.Role, roleData.BucketName))
	}

	reqForm := make(url.Values)
	reqForm.Add("name", name)
	reqForm.Add("password", password)
	reqForm.Add("roles", strings.Join(reqRoleStrs, ","))

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Method:      "PUT",
		Path:        fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, name),
		Body:        []byte(reqForm.Encode()),
		ContentType: "application/x-www-form-urlencoded",
		Context:     ctx,
	}

	resp, err := um.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return clientError{message: string(data)} // TODO: proper error
	}

	return nil
}

// RemoveUserOptions is the set of options available to the user manager Remove operation.
type RemoveUserOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context

	DomainName string
}

// Remove removes a built-in RBAC user on the cluster.
func (um *UserManager) Remove(name string, opts *RemoveUserOptions) error {
	if opts == nil {
		opts = &RemoveUserOptions{}
	}

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("Remove",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("Remove",
			opentracing.Tag{Key: "couchbase.service", Value: "usermgr"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

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
		Method:  "DELETE",
		Path:    fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, name),
		Context: ctx,
	}

	resp, err := um.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return clientError{message: string(data)} // TODO: proper error
	}

	return nil
}
