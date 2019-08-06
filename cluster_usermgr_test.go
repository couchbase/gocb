package gocb

import (
	"testing"
)

func TestUserManagerGroupCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(UserGroupFeature) {
		t.Skip("Skipping test as groups not supported.")
	}

	mgr, err := globalCluster.Users()
	if err != nil {
		t.Fatalf("Expected Groups to not error: %v", err)
	}

	err = mgr.UpsertGroup(Group{
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

func TestUserManagerCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(UserManagerFeature) {
		t.Skip("Skipping test as rbac not supported.")
	}

	mgr, err := globalCluster.Users()
	if err != nil {
		t.Fatalf("Expected Users to not error: %v", err)
	}

	err = mgr.UpsertGroup(Group{
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

	err = mgr.UpsertUser(User{
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
	}, nil)
	if err != nil {
		t.Fatalf("Expected UpsertUser to not error: %v", err)
	}

	user, err := mgr.GetUser("barry", nil)
	if err != nil {
		t.Fatalf("Expected GetUser to not error: %v", err)
	}

	if user.User.Username != "barry" {
		t.Fatalf("Expected user Username to be barry but was %s", user.User.Username)
	}

	if len(user.User.Groups) != 1 {
		t.Fatalf("Expected user Groups to be length 1 but was %v", user.User.Groups)
	}

	if user.User.Groups[0] != "test" {
		t.Fatalf("Expected user Groups 0 to be test but was %s", user.User.Groups[0])
	}

	if len(user.User.Roles) != 1 {
		t.Fatalf("Expected user Roles to be length 1 but was %v", user.User.Roles)
	}

	if user.User.DisplayName != "sheen" {
		t.Fatalf("Expected user DisplayName to be sheen but was %s", user.User.DisplayName)
	}

	if user.Domain != "local" {
		t.Fatalf("Expected user Domain to be local but was %s", user.Domain)
	}

	if len(user.EffectiveRoles) != 3 {
		t.Fatalf("Expected user EffectiveRoles to be length 3 but was %v", user.EffectiveRoles)
	}

	if len(user.EffectiveRolesAndOrigins) != 3 {
		t.Fatalf("Expected user EffectiveRolesAndOrigins to be length 3 but was %v", user.EffectiveRolesAndOrigins)
	}

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

	if !IsUserNotFoundError(err) {
		t.Fatalf("Expected error to be user not found but was %v", err)
	}
}

func TestUserManagerAvailableRoles(t *testing.T) {
	if !globalCluster.SupportsFeature(UserManagerFeature) {
		t.Skip("Skipping test as rbac not supported.")
	}

	mgr, err := globalCluster.Users()
	if err != nil {
		t.Fatalf("Expected Users to not error: %v", err)
	}

	roles, err := mgr.AvailableRoles(nil)
	if err != nil {
		t.Fatalf("Expected AvailableRoles to not error %v", err)
	}

	if len(roles) == 0 {
		t.Fatalf("Expected roles to have entries")
	}
}
