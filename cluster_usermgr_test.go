package gocb

import (
	"bytes"
	"errors"
	"io/ioutil"
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
	if err != nil {
		suite.T().Fatalf("Expected Upsert to not error: %v", err)
	}

	var group *Group
	suite.Require().Eventually(func() bool {
		group, err = mgr.GetGroup("test", nil)
		if err != nil {
			suite.T().Logf("Get errored: %v", err)
			return false
		}

		return true
	}, 5*time.Second, 100*time.Millisecond)
	group.Description = "this is still a test"
	group.Roles = append(group.Roles, Role{Name: "query_system_catalog"})

	err = mgr.UpsertGroup(*group, nil)
	if err != nil {
		suite.T().Fatalf("Expected Upsert to not error: %v", err)
	}

	group.Name = "test2"
	err = mgr.UpsertGroup(*group, nil)
	if err != nil {
		suite.T().Fatalf("Expected Upsert to not error: %v", err)
	}

	groups, err := mgr.GetAllGroups(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAll to not error: %v", err)
	}

	if len(groups) < 2 {
		suite.T().Fatalf("Expected groups to contain at least 2 groups, was %v", groups)
	}

	roles, err := mgr.GetRoles(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllClusterRoles to not error: %v", err)
	}

	suite.Assert().Greater(len(roles), 0)

	err = mgr.DropGroup("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected Drop to not error: %v", err)
	}

	err = mgr.DropGroup("test2", nil)
	if err != nil {
		suite.T().Fatalf("Expected Drop to not error: %v", err)
	}

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
	if err != nil {
		suite.T().Fatalf("Expected UpsertGroup to not error: %v", err)
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
		suite.T().Fatalf("Expected UpsertUser to not error: %v", err)
	}

	var user *UserAndMetadata
	suite.Eventuallyf(func() bool {
		user, err = mgr.GetUser("barry", nil)
		if err != nil {
			suite.T().Logf("GetUser failed: %v", err)
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond, "GetUser failed to successfully return user")

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
	if err != nil {
		suite.T().Fatalf("Expected UpsertUser to not error: %v", err)
	}

	expectedUserAndMeta.User.DisplayName = "barries"
	assertUserAndMetadata(suite.T(), user, expectedUserAndMeta)

	users, err := mgr.GetAllUsers(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllUsers to not error: %v", err)
	}

	if len(users) == 0 {
		suite.T().Fatalf("Expected users length to be greater than 0")
	}

	err = mgr.DropUser("barry", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropUser to not error: %v", err)
	}

	err = mgr.DropGroup("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropGroup to not error: %v", err)
	}

	_, err = mgr.GetUser("barry", nil)
	if !errors.Is(err, ErrUserNotFound) {
		suite.T().Fatalf("Expected error to be user not found but was %v", err)
	}
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
	if err != nil {
		suite.T().Fatalf("Expected UpsertUser to not error: %v", err)
	}

	var user *UserAndMetadata
	success := suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		user, err = mgr.GetUser("barry", nil)
		if err != nil {
			suite.T().Logf("GetUser request errored with %s", err)
			return false
		}
		return true
	})

	if !success {
		suite.T().Fatal("Wait time for get user expired")
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

	success = suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		err = mgr.UpsertUser(user.User, nil)
		if err != nil {
			suite.T().Logf("UpsertUser errored with %s", err)
			return false
		}

		return true
	})

	if !success {
		suite.T().Fatal("Wait time for upsert user expired")
	}

	users, err := mgr.GetAllUsers(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllUsers to not error: %v", err)
	}

	if len(users) == 0 {
		suite.T().Fatalf("Expected users length to be greater than 0")
	}

	success = suite.tryUntil(time.Now().Add(5*time.Second), 50*time.Millisecond, func() bool {
		user, err = mgr.GetUser("barry", nil)
		if err != nil {
			suite.T().Logf("GetUser errored with %s", err)
			return false
		}
		return true
	})

	if !success {
		suite.T().Fatal("Wait time for get user expired")
	}

	expectedUserAndMeta.User.DisplayName = "barries"
	assertUserAndMetadata(suite.T(), user, expectedUserAndMeta)

	err = mgr.DropUser("barry", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropUser to not error: %v", err)
	}

	suite.Require().Eventually(func() bool {
		_, err = mgr.GetUser("barry", nil)
		if !errors.Is(err, ErrUserNotFound) {
			suite.T().Logf("Expected error to be user not found but was %v", err)
			return false
		}

		return true
	}, 5*time.Second, 50*time.Millisecond)
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

			found := suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
				user, err := mgr.GetUser(tCase.user.Username, nil)
				if err != nil {
					te.Logf("Expected err to be nil but was %v", err)
					return false
				}

				assertUser(te, &user.User, &tCase.user)
				return true
			})

			if !found {
				te.Logf("User was not found")
			}
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

func (suite *UnitTestSuite) TestUserManagerGetUserDoesntExist() {
	retErr := `Unknown user.`
	resp := &mgmtResponse{
		StatusCode: 404,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	username := "larry"
	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/settings/rbac/users/local/"+username, req.Path)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		provider: mockProvider,
		tracer:   &NoopTracer{},
		meter:    &meterWrapper{meter: &NoopMeter{}},
	}
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
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	username := "larry"
	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/settings/rbac/users/local/"+username, req.Path)
			suite.Assert().False(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("DELETE", req.Method)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		provider: mockProvider,
		tracer:   &NoopTracer{},
		meter:    &meterWrapper{meter: &NoopMeter{}},
	}
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
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	name := "g"
	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/settings/rbac/groups/"+name, req.Path)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		provider: mockProvider,
		tracer:   &NoopTracer{},
		meter:    &meterWrapper{meter: &NoopMeter{}},
	}
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
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	name := "g"
	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/settings/rbac/groups/"+name, req.Path)
			suite.Assert().False(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("DELETE", req.Method)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		provider: mockProvider,
		tracer:   &NoopTracer{},
		meter:    &meterWrapper{meter: &NoopMeter{}},
	}
	err := usrMgr.DropGroup(name, &DropGroupOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrGroupNotFound) {
		suite.T().Fatalf("Expected user not found error, %s", err)
	}
}
