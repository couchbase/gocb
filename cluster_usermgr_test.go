package gocb

import "testing"

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
		Roles: []UserRole{
			{
				Role:       "replication_target",
				BucketName: globalBucket.Name(),
			},
			{
				Role: "security_admin",
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
	group.Roles = append(group.Roles, UserRole{Role: "query_system_catalog"})

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
