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

// Role represents a specific permission.
type Role struct {
	Name   string `json:"role"`
	Bucket string `json:"bucket_name"`
}

// RoleAndDescription represents a role with its display name and description.
type RoleAndDescription struct {
	Role        Role
	DisplayName string
	Description string
}

// Origin indicates why a user has a specific role. Is the Origin Type is "user" then the role is assigned
// directly to the user. If the type is "group" then it means that the role has been inherited from the group
// identified by the Name field.
type Origin struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// RoleAndOrigins associates a role with its origins.
type RoleAndOrigins struct {
	Role    Role
	Origins []Origin
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

// UserAndMetadata represents a user and user metadata from the server.
type UserAndMetadata struct {
	Domain AuthDomain
	User   User
	// EffectiveRoles are all of the user's roles, regardless of origin.
	EffectiveRoles []Role
	// EffectiveRolesAndOrigins is the same as EffectiveRoles but with origin information included.
	EffectiveRolesAndOrigins []RoleAndOrigins
	ExternalGroups           []string
	PasswordChanged          time.Time
}

// Group represents a user group on the server.
type Group struct {
	Name               string `json:"id"`
	Description        string `json:"description"`
	Roles              []Role `json:"roles"`
	LDAPGroupReference string `json:"ldap_group_ref"`
}

// AuthDomain specifies the user domain of a specific user
type AuthDomain string

const (
	// LocalDomain specifies users that are locally stored in Couchbase.
	LocalDomain AuthDomain = "local"

	// ExternalDomain specifies users that are externally stored
	// (in LDAP for instance).
	ExternalDomain AuthDomain = "external"
)

type roleDescriptionsJson struct {
	Role        string `json:"role"`
	BucketName  string `json:"bucket_name"`
	Name        string `json:"string"`
	Description string `json:"desc"`
}

type roleOriginsJson struct {
	RoleName   string `json:"role"`
	BucketName string `json:"bucket_name"`
	Origins    []Origin
}

type userMetadataJson struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Roles           []roleOriginsJson `json:"roles"`
	Groups          []string          `json:"groups"`
	Domain          AuthDomain        `json:"domain"`
	ExternalGroups  []string          `json:"external_groups"`
	PasswordChanged time.Time         `json:"password_change_date"`
}

func transformUserMetadataJson(userData *userMetadataJson) UserAndMetadata {
	var user UserAndMetadata
	user.User.Username = userData.ID
	user.User.DisplayName = userData.Name
	user.User.Groups = userData.Groups

	user.ExternalGroups = userData.ExternalGroups
	user.Domain = userData.Domain
	user.PasswordChanged = userData.PasswordChanged

	var roles []Role
	var effectiveRoles []Role
	var effectiveRolesAndOrigins []RoleAndOrigins
	for _, roleData := range userData.Roles {
		role := Role{
			Name:   roleData.RoleName,
			Bucket: roleData.BucketName,
		}
		effectiveRoles = append(effectiveRoles, role)
		effectiveRolesAndOrigins = append(effectiveRolesAndOrigins, RoleAndOrigins{
			Role:    role,
			Origins: roleData.Origins,
		})
		if roleData.Origins == nil {
			roles = append(roles, role)
		}
		for _, origin := range roleData.Origins {
			if origin.Type == "user" {
				roles = append(roles, role)
				break
			}
		}
	}
	user.EffectiveRoles = effectiveRoles
	user.EffectiveRolesAndOrigins = effectiveRolesAndOrigins
	user.User.Roles = roles

	return user
}

// GetAllUsersOptions is the set of options available to the user manager GetAll operation.
type GetAllUsersOptions struct {
	Timeout time.Duration
	Context context.Context

	DomainName string
}

// GetAllUsers returns a list of all the users from the cluster.
func (um *UserManager) GetAllUsers(opts *GetAllUsersOptions) ([]UserAndMetadata, error) {
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

	var usersData []userMetadataJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&usersData)
	if err != nil {
		return nil, err
	}

	var users []UserAndMetadata
	for _, userData := range usersData {
		user := transformUserMetadataJson(&userData)
		users = append(users, user)
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
func (um *UserManager) GetUser(name string, opts *GetUserOptions) (*UserAndMetadata, error) {
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

	var userData userMetadataJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&userData)
	if err != nil {
		return nil, err
	}

	user := transformUserMetadataJson(&userData)
	return &user, nil
}

// UpsertUserOptions is the set of options available to the user manager Upsert operation.
type UpsertUserOptions struct {
	Timeout time.Duration
	Context context.Context

	DomainName string
}

// UpsertUser updates a built-in RBAC user on the cluster.
func (um *UserManager) UpsertUser(user User, opts *UpsertUserOptions) error {
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

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Method:      "PUT",
		Path:        fmt.Sprintf("/settings/rbac/users/%s/%s", opts.DomainName, user.Username),
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

// AvailableRolesOptions is the set of options available to the user manager AvailableRoles operation.
type AvailableRolesOptions struct {
	Timeout time.Duration
	Context context.Context
}

// AvailableRoles lists the roles supported by the cluster.
func (um *UserManager) AvailableRoles(opts *AvailableRolesOptions) ([]RoleAndDescription, error) {
	if opts == nil {
		opts = &AvailableRolesOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "GET",
		Path:    "/settings/rbac/roles",
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

	var roleDatas []roleDescriptionsJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&roleDatas)
	if err != nil {
		return nil, err
	}

	var roles []RoleAndDescription
	for _, roleData := range roleDatas {
		role := RoleAndDescription{
			Role: Role{
				Name:   roleData.Role,
				Bucket: roleData.BucketName,
			},
			DisplayName: roleData.Name,
			Description: roleData.Description,
		}

		roles = append(roles, role)
	}

	return roles, nil
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
