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
// Volatile: This API is subject to change at any time.
type UserManager struct {
	httpClient httpProvider
}

// UserRole represents a role for a particular user on the server.
type UserRole struct {
	Role       string `json:"role"`
	BucketName string `json:"bucket_name"`
}

// User represents a user which was retrieved from the server.
type User struct {
	ID    string
	Name  string
	Type  string
	Roles []UserRole
}

// Group represents a user group on the server.
type Group struct {
	Name               string     `json:"id"`
	Description        string     `json:"description"`
	Roles              []UserRole `json:"roles"`
	LDAPGroupReference string     `json:"ldap_group_ref"`
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

// GetAllUsers returns a list of all the users from the cluster.
func (um *UserManager) GetAllUsers(opts *GetAllUsersOptions) ([]*User, error) {
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

// GetUser returns the data for a particular user
func (um *UserManager) GetUser(name string, opts *GetUserOptions) (*User, error) {
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

// UpsertUser updates a built-in RBAC user on the cluster.
func (um *UserManager) UpsertUser(name string, password string, roles []UserRole, opts *UpsertUserOptions) error {
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

// DropUser removes a built-in RBAC user on the cluster.
func (um *UserManager) DropUser(name string, opts *DropUserOptions) error {
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

// GetGroupOptions is the set of options available to the group manager Get operation.
type GetGroupOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetGroup fetches a single group from the server.
func (um *UserManager) GetGroup(groupName string, opts *GetGroupOptions) (*Group, error) {
	if groupName == "" {
		return nil, invalidArgumentsError{message: "groupName cannot be empty"}
	}
	if opts == nil {
		opts = &GetGroupOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "GET",
		Path:    fmt.Sprintf("/settings/rbac/groups/%s", groupName),
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

	var group Group
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&group)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

// GetAllGroupsOptions is the set of options available to the group manager GetAll operation.
type GetAllGroupsOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetAllGroups fetches all groups from the server.
func (um *UserManager) GetAllGroups(opts *GetAllGroupsOptions) ([]Group, error) {
	if opts == nil {
		opts = &GetAllGroupsOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "GET",
		Path:    "/settings/rbac/groups",
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

	var groups []Group
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&groups)
	if err != nil {
		return nil, err
	}

	return groups, nil
}

// UpsertGroupOptions is the set of options available to the group manager Upsert operation.
type UpsertGroupOptions struct {
	Timeout time.Duration
	Context context.Context
}

// UpsertGroup creates, or updates, a group on the server.
func (um *UserManager) UpsertGroup(group Group, opts *UpsertGroupOptions) error {
	if group.Name == "" {
		return invalidArgumentsError{message: "group name cannot be empty"}
	}
	if opts == nil {
		opts = &UpsertGroupOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	var reqRoleStrs []string
	for _, roleData := range group.Roles {
		if roleData.BucketName == "" {
			reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s", roleData.Role))
		} else {
			reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s[%s]", roleData.Role, roleData.BucketName))
		}
	}

	reqForm := make(url.Values)
	reqForm.Add("description", group.Description)
	reqForm.Add("ldap_group_ref", group.LDAPGroupReference)
	reqForm.Add("roles", strings.Join(reqRoleStrs, ","))

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Method:      "PUT",
		Path:        fmt.Sprintf("/settings/rbac/groups/%s", group.Name),
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

// DropGroupOptions is the set of options available to the group manager Drop operation.
type DropGroupOptions struct {
	Timeout time.Duration
	Context context.Context
}

// DropGroup removes a group from the server.
func (um *UserManager) DropGroup(groupName string, opts *DropGroupOptions) error {
	if groupName == "" {
		return invalidArgumentsError{message: "groupName cannot be empty"}
	}

	if opts == nil {
		opts = &DropGroupOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "DELETE",
		Path:    fmt.Sprintf("/settings/rbac/groups/%s", groupName),
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
