package gocb

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestUserManagerGroupCrud() {
	suite.skipIfUnsupported(UserGroupFeature)

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
				Name: "replication_admin",
			},
		},
		LDAPGroupReference: "asda=price",
	}, nil)
	suite.Require().NoError(err)

	suite.EnsureUserGroupOnAllNodes(time.Now().Add(20*time.Second), "test", nil)

	group, err := mgr.GetGroup("test", nil)
	suite.Require().NoError(err)

	group.Description = "this is still a test"
	group.Roles = append(group.Roles, Role{Name: "query_system_catalog"})

	err = mgr.UpsertGroup(*group, nil)
	suite.Require().NoError(err)

	suite.EnsureUserGroupOnAllNodes(time.Now().Add(20*time.Second), "test", func(g jsonGroup) bool {
		return g.Description == group.Description
	})

	group.Name = "test2"
	err = mgr.UpsertGroup(*group, nil)
	suite.Require().NoError(err)

	suite.EnsureUserGroupOnAllNodes(time.Now().Add(20*time.Second), "test2", nil)

	groups, err := mgr.GetAllGroups(nil)
	suite.Require().NoError(err)

	suite.Assert().Len(groups, 2)

	roles, err := mgr.GetRoles(nil)
	suite.Require().NoError(err)

	suite.Assert().Greater(len(roles), 0)

	err = mgr.DropGroup("test", nil)
	suite.Require().NoError(err)

	err = mgr.DropGroup("test2", nil)
	suite.Require().NoError(err)

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_upsert_group"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_get_group"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_get_all_groups"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_get_roles"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_drop_group"), 2, false)
}

func (suite *IntegrationTestSuite) TestUserManagerWithGroupsCrud() {
	suite.skipIfUnsupported(UserGroupFeature)

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
				Name: "replication_admin",
			},
		},
	}, nil)
	suite.Require().NoError(err)

	suite.EnsureUserGroupOnAllNodes(time.Now().Add(20*time.Second), "test", nil)

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
	suite.Require().NoError(err)

	suite.EnsureUserOnAllNodes(time.Now().Add(20*time.Second), expectedUser.Username, nil)

	user, err := mgr.GetUser("barry", nil)
	suite.Require().NoError(err)

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
					Name: "replication_admin",
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
	assertUserAndMetadata(suite.T(), user, expectedUserAndMeta)

	user.User.DisplayName = "barries"
	err = mgr.UpsertUser(user.User, nil)
	suite.Require().NoError(err)

	suite.EnsureUserOnAllNodes(time.Now().Add(20*time.Second), expectedUser.Username, func(u jsonUserMetadata) bool {
		return u.Name == user.User.DisplayName
	})

	user, err = mgr.GetUser("barry", nil)
	suite.Require().NoError(err)

	expectedUserAndMeta.User.DisplayName = "barries"
	assertUserAndMetadata(suite.T(), user, expectedUserAndMeta)

	users, err := mgr.GetAllUsers(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllUsers to not error: %v", err)
	}

	suite.Assert().Greater(len(users), 0, "Expected users length to be greater than 0")

	err = mgr.DropUser("barry", nil)
	suite.Require().NoError(err)

	suite.EnsureUserDroppedOnAllNodes(time.Now().Add(30*time.Second), expectedUser.Username)

	err = mgr.DropGroup("test", nil)
	suite.Require().NoError(err)

	_, err = mgr.GetUser("barry", nil)
	suite.Require().ErrorIs(err, ErrUserNotFound)
}

func (suite *IntegrationTestSuite) TestUserManagerCrud() {
	suite.skipIfUnsupported(UserManagerFeature)

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
			{
				Name: "replication_admin",
			},
			{
				Name:   "replication_target",
				Bucket: globalBucket.Name(),
			},
			{
				Name: "cluster_admin",
			},
		},
	}
	err := mgr.UpsertUser(expectedUser, nil)
	suite.Require().NoError(err, "Expected UpsertUser to not error")

	suite.EnsureUserOnAllNodes(time.Now().Add(20*time.Second), "barry", nil)

	user, err := mgr.GetUser("barry", nil)
	suite.Require().NoError(err, "Expected GetUser to not error")

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
			{
				Role: Role{
					Name: "replication_admin",
				},
			},
			{
				Role: Role{
					Name:   "replication_target",
					Bucket: globalBucket.Name(),
				},
			},
			{
				Role: Role{
					Name: "cluster_admin",
				},
			},
		},
	}
	assertUserAndMetadata(suite.T(), user, expectedUserAndMeta)

	user.User.DisplayName = "barries"

	err = mgr.UpsertUser(user.User, nil)
	suite.Require().NoError(err, "Expected UpsertUser to not error")

	suite.EnsureUserOnAllNodes(time.Now().Add(20*time.Second), expectedUser.Username, func(u jsonUserMetadata) bool {
		return u.Name == user.User.DisplayName
	})

	users, err := mgr.GetAllUsers(nil)
	suite.Require().NoError(err, "Expected GetAllUsers to not error")

	suite.Assert().Greater(len(users), 0, "Expected users length to be greater than 0")

	user, err = mgr.GetUser("barry", nil)
	suite.Require().NoError(err, "Expected GetUser to not error")

	expectedUserAndMeta.User.DisplayName = "barries"
	assertUserAndMetadata(suite.T(), user, expectedUserAndMeta)

	err = mgr.DropUser("barry", nil)
	suite.Require().NoError(err, "Expected DropUser to not error")

	suite.EnsureUserDroppedOnAllNodes(time.Now().Add(30*time.Second), expectedUser.Username)

	_, err = mgr.GetUser("barry", nil)
	suite.Assert().ErrorIs(err, ErrUserNotFound)

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_upsert_user"), 2, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_get_user"), 3, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_get_all_users"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_users_drop_user"), 1, false)
}

func (suite *IntegrationTestSuite) TestUserManagerAvailableRoles() {
	suite.skipIfUnsupported(UserManagerFeature)

	mgr := globalCluster.Users()

	roles, err := mgr.GetRoles(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetRoles to not error %v", err)
	}

	if len(roles) == 0 {
		suite.T().Fatalf("Expected roles to have entries")
	}
}

func (suite *IntegrationTestSuite) TestUserManagerCollectionsRoles() {
	suite.skipIfUnsupported(UserManagerFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	mgr := globalCluster.Users()

	type testCase struct {
		user User
	}

	testCases := []testCase{
		{
			user: User{
				Username:    "collectionsUserBucket",
				DisplayName: "collections user bucket",
				Password:    "password",
				Roles: []Role{
					{
						Name:   "data_reader",
						Bucket: globalBucket.Name(),
					},
				},
			},
		},
		{
			user: User{
				Username:    "collectionsUserScope",
				DisplayName: "collections user scope",
				Password:    "password",
				Roles: []Role{
					{
						Name:   "data_reader",
						Bucket: globalBucket.Name(),
						Scope:  globalScope.Name(),
					},
				},
			},
		},
		{
			user: User{
				Username:    "collectionsUserCollection",
				DisplayName: "collections user collection",
				Password:    "password",
				Roles: []Role{
					{
						Name:       "data_reader",
						Bucket:     globalBucket.Name(),
						Scope:      globalScope.Name(),
						Collection: globalCollection.Name(),
					},
				},
			},
		},
	}
	for _, tCase := range testCases {
		suite.T().Run(tCase.user.Username, func(te *testing.T) {
			err := mgr.UpsertUser(tCase.user, nil)
			if err != nil {
				te.Logf("Expected err to be nil but was %v", err)
				te.Fail()
				return
			}

			suite.EnsureUserOnAllNodes(time.Now().Add(20*time.Second), tCase.user.Username, nil)

			user, err := mgr.GetUser(tCase.user.Username, nil)
			if err != nil {
				te.Logf("Expected err to be nil but was %v", err)
				return
			}

			assertUser(te, &user.User, &tCase.user)

		})
	}
}

func (suite *IntegrationTestSuite) TestUserManagerChangePassword() {
	suite.skipIfUnsupported(UserManagerFeature)
	suite.skipIfUnsupported(UserManagerChangePasswordFeature)

	username := uuid.NewString()
	password := "password"
	mgr := globalCluster.Users()
	err := mgr.UpsertUser(User{
		Username: username,
		Password: password,
		Roles: []Role{
			{
				Name:   "data_reader",
				Bucket: globalBucket.Name(),
			},
		},
	}, nil)
	suite.Require().Nil(err, err)

	suite.EnsureUserOnAllNodes(time.Now().Add(20*time.Second), username, nil)

	c, err := Connect(globalConfig.connstr, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	suite.Require().Nil(err, err)
	closed := false
	defer func() {
		if !closed {
			c.Close(nil)
		}
	}()

	if globalCluster.SupportsFeature(WaitUntilReadyClusterFeature) {
		err = c.WaitUntilReady(20*time.Second, nil)
		suite.Require().Nil(err, err)
	} else {
		err = c.Bucket(globalConfig.Bucket).WaitUntilReady(20*time.Second, nil)
		suite.Require().Nil(err, err)
	}

	mgr = c.Users()
	newPassword := "newpassword"
	err = mgr.ChangePassword(newPassword, nil)
	suite.Require().Nil(err, err)

	err = c.Close(nil)
	closed = true
	suite.Require().Nil(err, err)

	c, err = Connect(globalConfig.connstr, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: newPassword,
	}})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	if globalCluster.SupportsFeature(WaitUntilReadyClusterFeature) {
		err = c.WaitUntilReady(20*time.Second, nil)
		suite.Require().Nil(err, err)
	} else {
		err = c.Bucket(globalConfig.Bucket).WaitUntilReady(20*time.Second, nil)
		suite.Require().Nil(err, err)
	}

	c, err = Connect(globalConfig.connstr, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	suite.Require().Nil(err, err)
	defer c.Close(nil)

	if globalCluster.SupportsFeature(WaitUntilReadyClusterFeature) {
		err = c.WaitUntilReady(10*time.Second, nil)
		suite.Require().NotNil(err, err)
	} else {
		err = c.Bucket(globalConfig.Bucket).WaitUntilReady(10*time.Second, nil)
		suite.Require().NotNil(err, err)
	}
}

func assertUser(t *testing.T, user *User, expected *User) {
	if user.Username != expected.Username {
		t.Logf("Expected user Username to be %s but was %s", expected.Username, user.Username)
		t.Fail()
	}

	if user.DisplayName != expected.DisplayName {
		t.Logf("Expected user DisplayName to be %s but was %s", expected.DisplayName, user.DisplayName)
		t.Fail()
	}

	if len(user.Groups) != len(expected.Groups) {
		t.Fatalf("Expected user Groups to be length %v but was %v", expected.Groups, user.Groups)
	}
	for i, group := range user.Groups {
		if group != expected.Groups[i] {
			t.Logf("Expected user group to be %s but was %s", expected.Groups[i], group)
			t.Fail()
		}
	}

	if len(user.Roles) != len(expected.Roles) {
		t.Fatalf("Expected user Roles to be length %v but was %v", expected.Roles, user.Roles)
	}
	for i, role := range user.Roles {
		if role != expected.Roles[i] {
			t.Logf("Expected user role to be %s but was %s", expected.Roles[i], role)
			t.Fail()
		}
	}
}

func assertUserAndMetadata(t *testing.T, user *UserAndMetadata, expected *UserAndMetadata) {
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

func (suite *UnitTestSuite) userManager(runFn func(args mock.Arguments), args ...interface{}) *UserManager {
	mockProvider := new(mockMgmtProvider)
	call := mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Return(args...)

	if runFn != nil {
		call.Run(runFn)
	}

	provider := &userManagerProviderCore{
		provider: mockProvider,
		tracer:   &NoopTracer{},
		meter:    &meterWrapper{meter: &NoopMeter{}, isNoopMeter: true},
	}

	usrMgr := &UserManager{
		controller: &providerController[userManagerProvider]{
			get: func() (userManagerProvider, error) {
				return provider, nil
			},
			opController: mockOpController{},
		},
	}

	return usrMgr
}

func (suite *UnitTestSuite) TestUserManagerGetUserDoesntExist() {
	retErr := `Unknown user.`
	resp := &mgmtResponse{
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	username := "larry"
	usrMgr := suite.userManager(func(args mock.Arguments) {
		req := args.Get(1).(mgmtRequest)

		suite.Assert().Equal("/settings/rbac/users/local/"+username, req.Path)
		suite.Assert().True(req.IsIdempotent)
		suite.Assert().Equal(1*time.Second, req.Timeout)
		suite.Assert().Equal("GET", req.Method)
	}, resp, nil)

	_, err := usrMgr.GetUser(username, &GetUserOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrUserNotFound) {
		suite.T().Fatalf("Expected user not found error, %s", err)
	}
}

func (suite *UnitTestSuite) TestUserManagerDropUserDoesntExist() {
	retErr := `User was not found.`
	resp := &mgmtResponse{
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	username := "larry"
	usrMgr := suite.userManager(func(args mock.Arguments) {
		req := args.Get(1).(mgmtRequest)

		suite.Assert().Equal("/settings/rbac/users/local/"+username, req.Path)
		suite.Assert().False(req.IsIdempotent)
		suite.Assert().Equal(1*time.Second, req.Timeout)
		suite.Assert().Equal("DELETE", req.Method)
	}, resp, nil)

	err := usrMgr.DropUser(username, &DropUserOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrUserNotFound) {
		suite.T().Fatalf("Expected user not found error, %s", err)
	}
}

func (suite *UnitTestSuite) TestUserManagerGetGroupDoesntExist() {
	retErr := `Unknown group.`
	resp := &mgmtResponse{
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	name := "g"

	usrMgr := suite.userManager(func(args mock.Arguments) {
		req := args.Get(1).(mgmtRequest)

		suite.Assert().Equal("/settings/rbac/groups/"+name, req.Path)
		suite.Assert().True(req.IsIdempotent)
		suite.Assert().Equal(1*time.Second, req.Timeout)
		suite.Assert().Equal("GET", req.Method)
	}, resp, nil)

	_, err := usrMgr.GetGroup(name, &GetGroupOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrGroupNotFound) {
		suite.T().Fatalf("Expected user not found error, %s", err)
	}
}

func (suite *UnitTestSuite) TestUserManagerDropGroupDoesntExist() {
	retErr := `Group was not found.`
	resp := &mgmtResponse{
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	name := "g"
	usrMgr := suite.userManager(func(args mock.Arguments) {
		req := args.Get(1).(mgmtRequest)

		suite.Assert().Equal("/settings/rbac/groups/"+name, req.Path)
		suite.Assert().False(req.IsIdempotent)
		suite.Assert().Equal(1*time.Second, req.Timeout)
		suite.Assert().Equal("DELETE", req.Method)
	}, resp, nil)

	err := usrMgr.DropGroup(name, &DropGroupOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrGroupNotFound) {
		suite.T().Fatalf("Expected user not found error, %s", err)
	}
}
