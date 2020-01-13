package gocb

import (
	"errors"
	"testing"
)

func TestUserManagerGroupCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(UserGroupFeature) {
		t.Skip("Skipping test as groups not supported.")
	}

	mgr := globalCluster.Users()

	err := mgr.UpsertGroup(Group{
		Name:        "test",
		Description: "this is a test",
		Roles: []Role{
			{
				Name:   "replication_target",
				Bucket: globalBucket.Name(),
			},
			{
				Name: "security_admin",
			},
		},
		LDAPGroupReference: "asda=price",
	}, nil)
	if err != nil {
		t.Fatalf("Expected Upsert to not error: %v", err)
	}

	group, err := mgr.GetGroup("test", nil)
	if err != nil {
		t.Fatalf("Expected Get to not error: %v", err)
	}
	group.Description = "this is still a test"
	group.Roles = append(group.Roles, Role{Name: "query_system_catalog"})

	err = mgr.UpsertGroup(*group, nil)
	if err != nil {
		t.Fatalf("Expected Upsert to not error: %v", err)
	}

	group.Name = "test2"
	err = mgr.UpsertGroup(*group, nil)
	if err != nil {
		t.Fatalf("Expected Upsert to not error: %v", err)
	}

	groups, err := mgr.GetAllGroups(nil)
	if err != nil {
		t.Fatalf("Expected GetAll to not error: %v", err)
	}

	if len(groups) < 2 {
		t.Fatalf("Expected groups to contain at least 2 groups, was %v", groups)
	}

	err = mgr.DropGroup("test", nil)
	if err != nil {
		t.Fatalf("Expected Drop to not error: %v", err)
	}

	err = mgr.DropGroup("test2", nil)
	if err != nil {
		t.Fatalf("Expected Drop to not error: %v", err)
	}
}

func TestUserManagerWithGroupsCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(UserGroupFeature) {
		t.Skip("Skipping test as groups not supported.")
	}

	mgr := globalCluster.Users()

	err := mgr.UpsertGroup(Group{
		Name:        "test",
		Description: "this is a test",
		Roles: []Role{
			{
				Name:   "replication_target",
				Bucket: globalBucket.Name(),
			},
			{
				Name: "security_admin",
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Expected UpsertGroup to not error: %v", err)
	}

	expectedUser := User{
		Username:    "barry",
		DisplayName: "sheen",
		Password:    "bangbang!",
		Roles: []Role{
			{
				Name:   "bucket_admin",
				Bucket: globalBucket.Name(),
			},
		},
		Groups: []string{"test"},
	}

	err = mgr.UpsertUser(expectedUser, nil)
	if err != nil {
		t.Fatalf("Expected UpsertUser to not error: %v", err)
	}

	user, err := mgr.GetUser("barry", nil)
	if err != nil {
		t.Fatalf("Expected GetUser to not error: %v", err)
	}

	expectedUserAndMeta := &UserAndMetadata{
		Domain: "local",
		User:   expectedUser,
		EffectiveRoles: []RoleAndOrigins{
			{
				Role: Role{
					Name:   "bucket_admin",
					Bucket: globalBucket.Name(),
				},
				Origins: []Origin{
					{
						Type: "user",
					},
				},
			},
			{
				Role: Role{
					Name:   "replication_target",
					Bucket: globalBucket.Name(),
				},
				Origins: []Origin{
					{
						Type: "group",
						Name: "test",
					},
				},
			},
			{
				Role: Role{
					Name: "security_admin",
				},
				Origins: []Origin{
					{
						Type: "group",
						Name: "test",
					},
				},
			},
		},
	}
	assertUser(t, user, expectedUserAndMeta)

	user.User.DisplayName = "barries"
	err = mgr.UpsertUser(user.User, nil)
	if err != nil {
		t.Fatalf("Expected UpsertUser to not error: %v", err)
	}

	expectedUserAndMeta.User.DisplayName = "barries"
	assertUser(t, user, expectedUserAndMeta)

	users, err := mgr.GetAllUsers(nil)
	if err != nil {
		t.Fatalf("Expected GetAllUsers to not error: %v", err)
	}

	if len(users) == 0 {
		t.Fatalf("Expected users length to be greater than 0")
	}

	err = mgr.DropUser("barry", nil)
	if err != nil {
		t.Fatalf("Expected DropUser to not error: %v", err)
	}

	err = mgr.DropGroup("test", nil)
	if err != nil {
		t.Fatalf("Expected DropGroup to not error: %v", err)
	}

	_, err = mgr.GetUser("barry", nil)
	if err == nil {
		t.Fatalf("Expected GetUser to error")
	}

	if !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("Expected error to be user not found but was %v", err)
	}
}

func TestUserManagerCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(UserManagerFeature) {
		t.Skip("Skipping test as rbac not supported.")
	}

	mgr := globalCluster.Users()

	expectedUser := User{
		Username:    "barry",
		DisplayName: "sheen",
		Password:    "bangbang!",
		Roles: []Role{
			{
				Name:   "bucket_admin",
				Bucket: globalBucket.Name(),
			},
		},
	}
	err := mgr.UpsertUser(expectedUser, nil)
	if err != nil {
		t.Fatalf("Expected UpsertUser to not error: %v", err)
	}

	user, err := mgr.GetUser("barry", nil)
	if err != nil {
		t.Fatalf("Expected GetUser to not error: %v", err)
	}

	expectedUserAndMeta := &UserAndMetadata{
		Domain: "local",
		User:   expectedUser,
		EffectiveRoles: []RoleAndOrigins{
			{
				Role: Role{
					Name:   "bucket_admin",
					Bucket: globalBucket.Name(),
				},
			},
		},
	}
	assertUser(t, user, expectedUserAndMeta)

	user.User.DisplayName = "barries"
	err = mgr.UpsertUser(user.User, nil)
	if err != nil {
		t.Fatalf("Expected UpsertUser to not error: %v", err)
	}

	users, err := mgr.GetAllUsers(nil)
	if err != nil {
		t.Fatalf("Expected GetAllUsers to not error: %v", err)
	}

	if len(users) == 0 {
		t.Fatalf("Expected users length to be greater than 0")
	}

	user, err = mgr.GetUser("barry", nil)
	if err != nil {
		t.Fatalf("Expected GetUser to not error: %v", err)
	}
	expectedUserAndMeta.User.DisplayName = "barries"
	assertUser(t, user, expectedUserAndMeta)

	err = mgr.DropUser("barry", nil)
	if err != nil {
		t.Fatalf("Expected DropUser to not error: %v", err)
	}

	_, err = mgr.GetUser("barry", nil)
	if err == nil {
		t.Fatalf("Expected GetUser to error")
	}

	if !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("Expected error to be user not found but was %v", err)
	}
}

func TestUserManagerAvailableRoles(t *testing.T) {
	if !globalCluster.SupportsFeature(UserManagerFeature) {
		t.Skip("Skipping test as rbac not supported.")
	}

	mgr := globalCluster.Users()

	roles, err := mgr.GetRoles(nil)
	if err != nil {
		t.Fatalf("Expected GetRoles to not error %v", err)
	}

	if len(roles) == 0 {
		t.Fatalf("Expected roles to have entries")
	}
}

func assertUser(t *testing.T, user *UserAndMetadata, expected *UserAndMetadata) {
	if user.User.Username != expected.User.Username {
		t.Fatalf("Expected user Username to be %s but was %s", expected.User.Username, user.User.Username)
	}

	if len(user.User.Groups) != len(expected.User.Groups) {
		t.Fatalf("Expected user Groups to be length %v but was %v", expected.User.Groups, user.User.Groups)
	}

	for i, group := range user.User.Groups {
		if group != expected.User.Groups[i] {
			t.Fatalf("Expected user Groups 0 to be %s but was %s", expected.User.Groups[i], group)
		}
	}

	if len(user.User.Roles) != len(expected.User.Roles) {
		t.Fatalf("Expected user Roles to be length %v but was %v", expected.User.Roles, user.User.Roles)
	}

	if user.User.DisplayName != expected.User.DisplayName {
		t.Fatalf("Expected user DisplayName to be %s but was %s", expected.User.DisplayName, user.User.DisplayName)
	}

	if user.Domain != expected.Domain {
		t.Fatalf("Expected user Domain to be %s but was %s", expected.Domain, user.Domain)
	}

	if len(user.EffectiveRoles) != len(expected.EffectiveRoles) {
		t.Fatalf("Expected user EffectiveRoles to be length %v but was %v", expected.EffectiveRoles, user.EffectiveRoles)
	}
}
