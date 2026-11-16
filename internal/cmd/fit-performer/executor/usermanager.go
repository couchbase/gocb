package executor

import (
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	protoUsers "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/usermanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *Executor) handleUserManagerCommand(
	op *protoUsers.Command,
	sender sender.ResultSender,
	returnResult bool,
) (bool, error) {
	mgr := e.conn.Cluster().Users()
	switch cmd := op.GetCommand().(type) {
	case *protoUsers.Command_GetUser:
		o := cmd.GetUser.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.GetUserOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		if o != nil && o.DomainName != nil {
			domain, err := authDomainFromProto(o.GetDomainName())
			if err != nil {
				return false, err
			}
			opts.DomainName = domain
		}
		return runUserManagerOp(e, sender, returnResult, getUserResult, func() (*gocb.UserAndMetadata, error) {
			return mgr.GetUser(cmd.GetUser.GetUsername(), opts)
		})
	case *protoUsers.Command_GetAllUsers:
		o := cmd.GetAllUsers.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.GetAllUsersOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		if o != nil && o.DomainName != nil {
			domain, err := authDomainFromProto(o.GetDomainName())
			if err != nil {
				return false, err
			}
			opts.DomainName = domain
		}
		return runUserManagerOp(e, sender, returnResult, getAllUsersResult, func() ([]gocb.UserAndMetadata, error) {
			return mgr.GetAllUsers(opts)
		})
	case *protoUsers.Command_UpsertUser:
		o := cmd.UpsertUser.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.UpsertUserOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		if o != nil && o.DomainName != nil {
			domain, err := authDomainFromProto(o.GetDomainName())
			if err != nil {
				return false, err
			}
			opts.DomainName = domain
		}
		user := userFromProto(cmd.UpsertUser.GetUser())
		return runUserManagerOp(e, sender, returnResult, nil, func() (struct{}, error) {
			return struct{}{}, mgr.UpsertUser(user, opts)
		})
	case *protoUsers.Command_DropUser:
		o := cmd.DropUser.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.DropUserOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		if o != nil && o.DomainName != nil {
			domain, err := authDomainFromProto(o.GetDomainName())
			if err != nil {
				return false, err
			}
			opts.DomainName = domain
		}
		return runUserManagerOp(e, sender, returnResult, nil, func() (struct{}, error) {
			return struct{}{}, mgr.DropUser(cmd.DropUser.GetUsername(), opts)
		})
	case *protoUsers.Command_GetRoles:
		o := cmd.GetRoles.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.GetRolesOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		return runUserManagerOp(e, sender, returnResult, getRolesResult, func() ([]gocb.RoleAndDescription, error) {
			return mgr.GetRoles(opts)
		})
	case *protoUsers.Command_GetGroup:
		o := cmd.GetGroup.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.GetGroupOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		return runUserManagerOp(e, sender, returnResult, getGroupResult, func() (*gocb.Group, error) {
			return mgr.GetGroup(cmd.GetGroup.GetGroupName(), opts)
		})
	case *protoUsers.Command_GetAllGroups:
		o := cmd.GetAllGroups.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.GetAllGroupsOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		return runUserManagerOp(e, sender, returnResult, getAllGroupsResult, func() ([]gocb.Group, error) {
			return mgr.GetAllGroups(opts)
		})
	case *protoUsers.Command_UpsertGroup:
		o := cmd.UpsertGroup.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.UpsertGroupOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		group := groupFromProto(cmd.UpsertGroup.GetGroup())
		return runUserManagerOp(e, sender, returnResult, nil, func() (struct{}, error) {
			return struct{}{}, mgr.UpsertGroup(group, opts)
		})
	case *protoUsers.Command_DropGroup:
		o := cmd.DropGroup.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.DropGroupOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		return runUserManagerOp(e, sender, returnResult, nil, func() (struct{}, error) {
			return struct{}{}, mgr.DropGroup(cmd.DropGroup.GetGroupName(), opts)
		})
	case *protoUsers.Command_ChangePassword:
		o := cmd.ChangePassword.GetOptions()
		parentSpan, err := e.parentSpan(o.GetParentSpanId())
		if err != nil {
			return false, err
		}
		opts := &gocb.ChangePasswordOptions{
			ParentSpan: parentSpan,
		}
		if o.GetTimeout() != nil {
			opts.Timeout = o.GetTimeout().AsDuration()
		}
		return runUserManagerOp(e, sender, returnResult, nil, func() (struct{}, error) {
			return struct{}{}, mgr.ChangePassword(cmd.ChangePassword.GetNewPassword(), opts)
		})
	default:
		return false, status.Errorf(codes.Unimplemented, "unknown user manager command: %T", op.GetCommand())
	}
}

func runUserManagerOp[R any](
	e *Executor,
	sender sender.ResultSender,
	returnResult bool,
	toProto func(R) (*protoUsers.Result, error),
	call func() (R, error),
) (bool, error) {
	result := &run.Result{
		Initiated: timestamppb.Now(),
	}

	start := time.Now()
	res, err := call()
	result.ElapsedNanos = time.Since(start).Nanoseconds()
	if err != nil {
		e.sendSDKError(err, sender, start)
		return false, nil
	}

	if returnResult && toProto != nil {
		protoRes, err := toProto(res)
		if err != nil {
			return false, err
		}
		result.Result = &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_UserManagerResult{UserManagerResult: protoRes},
			},
		}
	} else {
		result.Result = e.makeSuccessResult()
	}

	sender.Send(result)
	return true, nil
}

func authDomainFromProto(domain protoUsers.AuthDomain) (string, error) {
	switch domain {
	case protoUsers.AuthDomain_LOCAL:
		return string(gocb.LocalDomain), nil
	case protoUsers.AuthDomain_EXTERNAL:
		return string(gocb.ExternalDomain), nil
	default:
		return "", status.Errorf(codes.Unimplemented, "unknown auth domain: %s", domain)
	}
}

func authDomainToProto(domain gocb.AuthDomain) (protoUsers.AuthDomain, error) {
	switch domain {
	case gocb.LocalDomain:
		return protoUsers.AuthDomain_LOCAL, nil
	case gocb.ExternalDomain:
		return protoUsers.AuthDomain_EXTERNAL, nil
	default:
		return 0, status.Errorf(codes.Unimplemented, "unknown auth domain: %s", domain)
	}
}

func roleFromProto(role *protoUsers.Role) gocb.Role {
	return gocb.Role{
		Name:       role.GetName(),
		Bucket:     role.GetBucket(),
		Scope:      role.GetScope(),
		Collection: role.GetCollection(),
	}
}

func roleToProto(role gocb.Role) *protoUsers.Role {
	res := &protoUsers.Role{
		Name: role.Name,
	}
	if role.Bucket != "" {
		res.Bucket = &role.Bucket
	}
	if role.Scope != "" {
		res.Scope = &role.Scope
	}
	if role.Collection != "" {
		res.Collection = &role.Collection
	}
	return res
}

func rolesFromProto(roles []*protoUsers.Role) []gocb.Role {
	out := make([]gocb.Role, 0, len(roles))
	for _, role := range roles {
		out = append(out, roleFromProto(role))
	}
	return out
}

func rolesToProto(roles []gocb.Role) []*protoUsers.Role {
	out := make([]*protoUsers.Role, 0, len(roles))
	for _, role := range roles {
		out = append(out, roleToProto(role))
	}
	return out
}

func userFromProto(user *protoUsers.User) gocb.User {
	return gocb.User{
		Username:    user.GetUsername(),
		DisplayName: user.GetDisplayName(),
		Roles:       rolesFromProto(user.GetRoles()),
		Groups:      user.GetGroups(),
		// gocb only sends the password when it is non-empty, so an unset password leaves the existing one unchanged.
		Password: user.GetPassword(),
	}
}

func userAndMetadataToProto(user *gocb.UserAndMetadata) (*protoUsers.UserAndMetadata, error) {
	domain, err := authDomainToProto(user.Domain)
	if err != nil {
		return nil, err
	}

	effectiveRoles := make([]*protoUsers.RoleAndOrigins, 0, len(user.EffectiveRoles))
	for _, role := range user.EffectiveRoles {
		origins := make([]*protoUsers.Origin, 0, len(role.Origins))
		for _, origin := range role.Origins {
			protoOrigin := &protoUsers.Origin{
				Type: origin.Type,
			}
			if origin.Name != "" {
				protoOrigin.Name = &origin.Name
			}
			origins = append(origins, protoOrigin)
		}
		effectiveRoles = append(effectiveRoles, &protoUsers.RoleAndOrigins{
			Role:    roleToProto(role.Role),
			Origins: origins,
		})
	}

	res := &protoUsers.UserAndMetadata{
		Domain: domain,
		User: &protoUsers.User{
			Username:    user.Username,
			DisplayName: user.DisplayName,
			Groups:      user.Groups,
			Roles:       rolesToProto(user.Roles),
		},
		EffectiveRoles: effectiveRoles,
		ExternalGroups: user.ExternalGroups,
	}
	if !user.PasswordChanged.IsZero() {
		res.PasswordChanged = timestamppb.New(user.PasswordChanged)
	}
	return res, nil
}

func groupFromProto(group *protoUsers.Group) gocb.Group {
	return gocb.Group{
		Name:               group.GetName(),
		Description:        group.GetDescription(),
		Roles:              rolesFromProto(group.GetRoles()),
		LDAPGroupReference: group.GetLdapGroupReference(),
	}
}

func groupToProto(group *gocb.Group) *protoUsers.Group {
	res := &protoUsers.Group{
		Name:  group.Name,
		Roles: rolesToProto(group.Roles),
	}
	if group.Description != "" {
		res.Description = &group.Description
	}
	if group.LDAPGroupReference != "" {
		res.LdapGroupReference = &group.LDAPGroupReference
	}
	return res
}

func getUserResult(user *gocb.UserAndMetadata) (*protoUsers.Result, error) {
	protoUser, err := userAndMetadataToProto(user)
	if err != nil {
		return nil, err
	}
	return &protoUsers.Result{
		Result: &protoUsers.Result_GetUserResult{
			GetUserResult: &protoUsers.GetUserResult{User: protoUser},
		},
	}, nil
}

func getAllUsersResult(users []gocb.UserAndMetadata) (*protoUsers.Result, error) {
	protoUsersList := make([]*protoUsers.UserAndMetadata, 0, len(users))
	for i := range users {
		protoUser, err := userAndMetadataToProto(&users[i])
		if err != nil {
			return nil, err
		}
		protoUsersList = append(protoUsersList, protoUser)
	}
	return &protoUsers.Result{
		Result: &protoUsers.Result_GetAllUsersResult{
			GetAllUsersResult: &protoUsers.GetAllUsersResult{Users: protoUsersList},
		},
	}, nil
}

func getRolesResult(roles []gocb.RoleAndDescription) (*protoUsers.Result, error) {
	protoRoles := make([]*protoUsers.RoleAndDescription, 0, len(roles))
	for _, role := range roles {
		protoRoles = append(protoRoles, &protoUsers.RoleAndDescription{
			Role:        roleToProto(role.Role),
			DisplayName: role.DisplayName,
			Description: role.Description,
		})
	}
	return &protoUsers.Result{
		Result: &protoUsers.Result_GetRolesResult{
			GetRolesResult: &protoUsers.GetRolesResult{Roles: protoRoles},
		},
	}, nil
}

func getGroupResult(group *gocb.Group) (*protoUsers.Result, error) {
	return &protoUsers.Result{
		Result: &protoUsers.Result_GetGroupResult{
			GetGroupResult: &protoUsers.GetGroupResult{Group: groupToProto(group)},
		},
	}, nil
}

func getAllGroupsResult(groups []gocb.Group) (*protoUsers.Result, error) {
	protoGroups := make([]*protoUsers.Group, 0, len(groups))
	for i := range groups {
		protoGroups = append(protoGroups, groupToProto(&groups[i]))
	}
	return &protoUsers.Result{
		Result: &protoUsers.Result_GetAllGroupsResult{
			GetAllGroupsResult: &protoUsers.GetAllGroupsResult{Groups: protoGroups},
		},
	}, nil
}
