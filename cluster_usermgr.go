package gocb

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// AuthDomain specifies the user domain of a specific user
type AuthDomain string

const (
	// LocalDomain specifies users that are locally stored in Couchbase.
	LocalDomain AuthDomain = "local"

	// ExternalDomain specifies users that are externally stored
	// (in LDAP for instance).
	ExternalDomain AuthDomain = "external"
)

type jsonOrigin struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type jsonRole struct {
	RoleName   string `json:"role"`
	BucketName string `json:"bucket_name"`
}

type jsonRoleDescription struct {
	jsonRole

	Name        string `json:"string"`
	Description string `json:"desc"`
}

type jsonRoleOrigins struct {
	jsonRole

	Origins []jsonOrigin
}

type jsonUserMetadata struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Roles           []jsonRoleOrigins `json:"roles"`
	Groups          []string          `json:"groups"`
	Domain          AuthDomain        `json:"domain"`
	ExternalGroups  []string          `json:"external_groups"`
	PasswordChanged time.Time         `json:"password_change_date"`
}

type jsonGroup struct {
	Name               string     `json:"id"`
	Description        string     `json:"description"`
	Roles              []jsonRole `json:"roles"`
	LDAPGroupReference string     `json:"ldap_group_ref"`
}

// Role represents a specific permission.
type Role struct {
	Name   string `json:"role"`
	Bucket string `json:"bucket_name"`
}

func (ro *Role) fromData(data jsonRole) error {
	ro.Name = data.RoleName
	ro.Bucket = data.BucketName

	return nil
}

// RoleAndDescription represents a role with its display name and description.
type RoleAndDescription struct {
	Role

	DisplayName string
	Description string
}

func (rd *RoleAndDescription) fromData(data jsonRoleDescription) error {
	err := rd.Role.fromData(data.jsonRole)
	if err != nil {
		return err
	}

	rd.DisplayName = data.Name
	rd.Description = data.Description

	return nil
}

// Origin indicates why a user has a specific role. Is the Origin Type is "user" then the role is assigned
// directly to the user. If the type is "group" then it means that the role has been inherited from the group
// identified by the Name field.
type Origin struct {
	Type string
	Name string
}

func (o *Origin) fromData(data jsonOrigin) error {
	o.Type = data.Type
	o.Name = data.Name

	return nil
}

// RoleAndOrigins associates a role with its origins.
type RoleAndOrigins struct {
	Role

	Origins []Origin
}

func (ro *RoleAndOrigins) fromData(data jsonRoleOrigins) error {
	err := ro.Role.fromData(data.jsonRole)
	if err != nil {
		return err
	}

	origins := make([]Origin, len(data.Origins))
	for _, originData := range data.Origins {
		var origin Origin
		err := origin.fromData(originData)
		if err != nil {
			return err
		}

		origins = append(origins, origin)
	}
	ro.Origins = origins

	return nil
}

// User represents a user which was retrieved from the server.
type User struct {
	Username    string
	DisplayName string
	// Roles are the roles assigned to the user that are of type "user".
	Roles    []Role
	Groups   []string
	Password string
}

// UserAndMetadata represents a user and user meta-data from the server.
type UserAndMetadata struct {
	User

	Domain AuthDomain
	// EffectiveRoles are all of the user's roles and the origins.
	EffectiveRoles  []RoleAndOrigins
	ExternalGroups  []string
	PasswordChanged time.Time
}

func (um *UserAndMetadata) fromData(data jsonUserMetadata) error {
	um.User.Username = data.ID
	um.User.DisplayName = data.Name
	um.User.Groups = data.Groups

	um.ExternalGroups = data.ExternalGroups
	um.Domain = data.Domain
	um.PasswordChanged = data.PasswordChanged

	var roles []Role
	var effectiveRoles []RoleAndOrigins
	for _, roleData := range data.Roles {
		var effectiveRole RoleAndOrigins
		err := effectiveRole.fromData(roleData)
		if err != nil {
			return err
		}

		effectiveRoles = append(effectiveRoles, effectiveRole)

		role := effectiveRole.Role
		if effectiveRole.Origins == nil {
			roles = append(roles, role)
		} else {
			for _, origin := range effectiveRole.Origins {
				if origin.Type == "user" {
					roles = append(roles, role)
					break
				}
			}
		}
	}
	um.EffectiveRoles = effectiveRoles
	um.User.Roles = roles

	return nil
}

// Group represents a user group on the server.
type Group struct {
	Name               string
	Description        string
	Roles              []Role
	LDAPGroupReference string
}

func (g *Group) fromData(data jsonGroup) error {
	g.Name = data.Name
	g.Description = data.Description
	g.LDAPGroupReference = data.LDAPGroupReference

	roles := make([]Role, len(data.Roles))
	for roleIdx, roleData := range data.Roles {
		err := roles[roleIdx].fromData(roleData)
		if err != nil {
			return err
		}
	}
	g.Roles = roles

	return nil
}

// UserManager provides methods for performing Couchbase user management.
// Volatile: This API is subject to change at any time.
type UserManager struct {
	httpClient           httpProvider
	globalTimeout        time.Duration
	defaultRetryStrategy *retryStrategyWrapper
	tracer               requestTracer
}

// GetAllUsersOptions is the set of options available to the user manager GetAll operation.
type GetAllUsersOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy

	DomainName string
}

// GetAllUsers returns a list of all the users from the cluster.
func (um *UserManager) GetAllUsers(opts *GetAllUsersOptions) ([]UserAndMetadata, error) {
	if opts == nil {
		opts = &GetAllUsersOptions{}
	}

	span := um.tracer.StartSpan("GetAllUsers", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "GET",
		Path:          fmt.Sprintf("/settings/rbac/users/%s", opts.DomainName),
		IsIdempotent:  true,
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, makeHTTPBadStatusError("failed to get all users", req, resp)
	}

	var usersData []jsonUserMetadata
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&usersData)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	users := make([]UserAndMetadata, len(usersData))
	for userIdx, userData := range usersData {
		err := users[userIdx].fromData(userData)
		if err != nil {
			return nil, err
		}
	}

	return users, nil
}

// GetUserOptions is the set of options available to the user manager Get operation.
type GetUserOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy

	DomainName string
}

// GetUser returns the data for a particular user
func (um *UserManager) GetUser(name string, opts *GetUserOptions) (*UserAndMetadata, error) {
	if opts == nil {
		opts = &GetUserOptions{}
	}

	span := um.tracer.StartSpan("GetUser", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "GET",
		Path:          fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, name),
		IsIdempotent:  true,
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, makeHTTPBadStatusError("failed to get user", req, resp)
	}

	var userData jsonUserMetadata
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&userData)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	var user UserAndMetadata
	err = user.fromData(userData)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

// UpsertUserOptions is the set of options available to the user manager Upsert operation.
type UpsertUserOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy

	DomainName string
}

// UpsertUser updates a built-in RBAC user on the cluster.
func (um *UserManager) UpsertUser(user User, opts *UpsertUserOptions) error {
	if opts == nil {
		opts = &UpsertUserOptions{}
	}

	span := um.tracer.StartSpan("UpsertUser", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	var reqRoleStrs []string
	for _, roleData := range user.Roles {
		reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s[%s]", roleData.Name, roleData.Bucket))
	}

	reqForm := make(url.Values)
	reqForm.Add("name", user.DisplayName)
	if user.Password != "" {
		reqForm.Add("password", user.Password)
	}
	if len(user.Groups) > 0 {
		reqForm.Add("groups", strings.Join(user.Groups, ","))
	}
	reqForm.Add("roles", strings.Join(reqRoleStrs, ","))

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "PUT",
		Path:          fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, user.Username),
		Body:          []byte(reqForm.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return makeHTTPBadStatusError("failed to upsert user", req, resp)
	}

	return nil
}

// DropUserOptions is the set of options available to the user manager Drop operation.
type DropUserOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy

	DomainName string
}

// DropUser removes a built-in RBAC user on the cluster.
func (um *UserManager) DropUser(name string, opts *DropUserOptions) error {
	if opts == nil {
		opts = &DropUserOptions{}
	}

	span := um.tracer.StartSpan("DropUser", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	if opts.DomainName == "" {
		opts.DomainName = string(LocalDomain)
	}

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "DELETE",
		Path:          fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, name),
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return makeHTTPBadStatusError("failed to drop user", req, resp)
	}

	return nil
}

// GetRolesOptions is the set of options available to the user manager GetRoles operation.
type GetRolesOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetRoles lists the roles supported by the cluster.
func (um *UserManager) GetRoles(opts *GetRolesOptions) ([]RoleAndDescription, error) {
	if opts == nil {
		opts = &GetRolesOptions{}
	}

	span := um.tracer.StartSpan("GetRoles", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "GET",
		Path:          "/settings/rbac/roles",
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, makeHTTPBadStatusError("failed to get roles", req, resp)
	}

	var roleDatas []jsonRoleDescription
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&roleDatas)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	roles := make([]RoleAndDescription, len(roleDatas))
	for roleIdx, roleData := range roleDatas {
		err := roles[roleIdx].fromData(roleData)
		if err != nil {
			return nil, err
		}
	}

	return roles, nil
}

// GetGroupOptions is the set of options available to the group manager Get operation.
type GetGroupOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetGroup fetches a single group from the server.
func (um *UserManager) GetGroup(groupName string, opts *GetGroupOptions) (*Group, error) {
	if groupName == "" {
		return nil, makeInvalidArgumentsError("groupName cannot be empty")
	}
	if opts == nil {
		opts = &GetGroupOptions{}
	}

	span := um.tracer.StartSpan("GetGroup", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "GET",
		Path:          fmt.Sprintf("/settings/rbac/groups/%s", groupName),
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, makeHTTPBadStatusError("failed to get group", req, resp)
	}

	var groupData jsonGroup
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&groupData)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	var group Group
	err = group.fromData(groupData)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

// GetAllGroupsOptions is the set of options available to the group manager GetAll operation.
type GetAllGroupsOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAllGroups fetches all groups from the server.
func (um *UserManager) GetAllGroups(opts *GetAllGroupsOptions) ([]Group, error) {
	if opts == nil {
		opts = &GetAllGroupsOptions{}
	}

	span := um.tracer.StartSpan("GetAllGroups", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "GET",
		Path:          "/settings/rbac/groups",
		RetryStrategy: retryStrategy,
		IsIdempotent:  true,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return nil, makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, makeHTTPBadStatusError("failed to get all groups", req, resp)
	}

	var groupDatas []jsonGroup
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&groupDatas)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	groups := make([]Group, len(groupDatas))
	for groupIdx, groupData := range groupDatas {
		err = groups[groupIdx].fromData(groupData)
		if err != nil {
			return nil, err
		}
	}

	return groups, nil
}

// UpsertGroupOptions is the set of options available to the group manager Upsert operation.
type UpsertGroupOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// UpsertGroup creates, or updates, a group on the server.
func (um *UserManager) UpsertGroup(group Group, opts *UpsertGroupOptions) error {
	if group.Name == "" {
		return makeInvalidArgumentsError("group name cannot be empty")
	}
	if opts == nil {
		opts = &UpsertGroupOptions{}
	}

	span := um.tracer.StartSpan("UpsertGroup", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	var reqRoleStrs []string
	for _, roleData := range group.Roles {
		if roleData.Bucket == "" {
			reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s", roleData.Name))
		} else {
			reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s[%s]", roleData.Name, roleData.Bucket))
		}
	}

	reqForm := make(url.Values)
	reqForm.Add("description", group.Description)
	reqForm.Add("ldap_group_ref", group.LDAPGroupReference)
	reqForm.Add("roles", strings.Join(reqRoleStrs, ","))

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "PUT",
		Path:          fmt.Sprintf("/settings/rbac/groups/%s", group.Name),
		Body:          []byte(reqForm.Encode()),
		ContentType:   "application/x-www-form-urlencoded",
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return makeHTTPBadStatusError("failed to upsert group", req, resp)
	}

	return nil
}

// DropGroupOptions is the set of options available to the group manager Drop operation.
type DropGroupOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// DropGroup removes a group from the server.
func (um *UserManager) DropGroup(groupName string, opts *DropGroupOptions) error {
	if groupName == "" {
		return makeInvalidArgumentsError("groupName cannot be empty")
	}

	if opts == nil {
		opts = &DropGroupOptions{}
	}

	span := um.tracer.StartSpan("DropGroup", nil).
		SetTag("couchbase.service", "mgmt")
	defer span.Finish()

	retryStrategy := um.defaultRetryStrategy
	if opts.RetryStrategy == nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	req := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(MgmtService),
		Method:        "DELETE",
		Path:          fmt.Sprintf("/settings/rbac/groups/%s", groupName),
		RetryStrategy: retryStrategy,
		UniqueID:      uuid.New().String(),
	}

	dspan := um.tracer.StartSpan("dispatch", span.Context())
	resp, err := um.httpClient.DoHTTPRequest(req)
	dspan.Finish()
	if err != nil {
		return makeGenericHTTPError(err, req, resp)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return makeHTTPBadStatusError("failed to drop group", req, resp)
	}

	return nil
}
