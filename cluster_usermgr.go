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
	Timeout time.Duration
	Context context.Context

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

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
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
		return nil, userManagerError{statusCode: resp.StatusCode, message: string(data)}
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
	Timeout time.Duration
	Context context.Context

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

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
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
		return nil, userManagerError{statusCode: resp.StatusCode, message: string(data)}
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
	Timeout time.Duration
	Context context.Context

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

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
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
		return userManagerError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}

// DropUserOptions is the set of options available to the user manager Drop operation.
type DropUserOptions struct {
	Timeout time.Duration
	Context context.Context

	DomainName string
}

// Drop removes a built-in RBAC user on the cluster.
func (um *UserManager) Drop(name string, opts *DropUserOptions) error {
	if opts == nil {
		opts = &DropUserOptions{}
	}

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
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
		return userManagerError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}
