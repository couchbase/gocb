package gocb

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"

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
				Name: "security_admin",
			},
		},
		LDAPGroupReference: "asda=price",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected Upsert to not error: %v", err)
	}

	group, err := mgr.GetGroup("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected Get to not error: %v", err)
	}
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
		suite.T().Fatalf("Expected GetAllRoles to not error: %v", err)
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
				Name: "security_admin",
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

	user, err := mgr.GetUser("barry", nil)
	if err != nil {
		suite.T().Fatalf("Expected GetUser to not error: %v", err)
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
	assertUser(suite.T(), user, expectedUserAndMeta)

	user.User.DisplayName = "barries"
	err = mgr.UpsertUser(user.User, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertUser to not error: %v", err)
	}

	expectedUserAndMeta.User.DisplayName = "barries"
	assertUser(suite.T(), user, expectedUserAndMeta)

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
			suite.T().Logf("GetUser failed with %s", err)
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
		},
	}
	assertUser(suite.T(), user, expectedUserAndMeta)

	user.User.DisplayName = "barries"
	err = mgr.UpsertUser(user.User, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertUser to not error: %v", err)
	}

	users, err := mgr.GetAllUsers(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllUsers to not error: %v", err)
	}

	if len(users) == 0 {
		suite.T().Fatalf("Expected users length to be greater than 0")
	}

	user, err = mgr.GetUser("barry", nil)
	if err != nil {
		suite.T().Fatalf("Expected GetUser to not error: %v", err)
	}
	expectedUserAndMeta.User.DisplayName = "barries"
	assertUser(suite.T(), user, expectedUserAndMeta)

	err = mgr.DropUser("barry", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropUser to not error: %v", err)
	}

	_, err = mgr.GetUser("barry", nil)
	if !errors.Is(err, ErrUserNotFound) {
		suite.T().Fatalf("Expected error to be user not found but was %v", err)
	}
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

func (suite *UnitTestSuite) TestUserManagerGetUserDoesntExist() {
	retErr := `Unknown user.`
	resp := &gocbcore.HTTPResponse{
		StatusCode: 404,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	username := "larry"
	mockProvider := new(mockHttpProvider)
	mockProvider.
		On("DoHTTPRequest", mock.AnythingOfType("*gocbcore.HTTPRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)

			suite.Assert().Equal("/settings/rbac/users/local/"+username, req.Path)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
			suite.Assert().NotNil(req.RetryStrategy)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		httpClient:           mockProvider,
		globalTimeout:        10 * time.Second,
		defaultRetryStrategy: newRetryStrategyWrapper(newFailFastRetryStrategy()),
		tracer:               &noopTracer{},
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
	resp := &gocbcore.HTTPResponse{
		StatusCode: 404,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	username := "larry"
	mockProvider := new(mockHttpProvider)
	mockProvider.
		On("DoHTTPRequest", mock.AnythingOfType("*gocbcore.HTTPRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)

			suite.Assert().Equal("/settings/rbac/users/local/"+username, req.Path)
			suite.Assert().False(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("DELETE", req.Method)
			suite.Assert().NotNil(req.RetryStrategy)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		httpClient:           mockProvider,
		globalTimeout:        10 * time.Second,
		defaultRetryStrategy: newRetryStrategyWrapper(newFailFastRetryStrategy()),
		tracer:               &noopTracer{},
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
	resp := &gocbcore.HTTPResponse{
		StatusCode: 404,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	name := "g"
	mockProvider := new(mockHttpProvider)
	mockProvider.
		On("DoHTTPRequest", mock.AnythingOfType("*gocbcore.HTTPRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)

			suite.Assert().Equal("/settings/rbac/groups/"+name, req.Path)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
			suite.Assert().NotNil(req.RetryStrategy)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		httpClient:           mockProvider,
		globalTimeout:        10 * time.Second,
		defaultRetryStrategy: newRetryStrategyWrapper(newFailFastRetryStrategy()),
		tracer:               &noopTracer{},
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
	resp := &gocbcore.HTTPResponse{
		StatusCode: 404,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	name := "g"
	mockProvider := new(mockHttpProvider)
	mockProvider.
		On("DoHTTPRequest", mock.AnythingOfType("*gocbcore.HTTPRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)

			suite.Assert().Equal("/settings/rbac/groups/"+name, req.Path)
			suite.Assert().False(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("DELETE", req.Method)
			suite.Assert().NotNil(req.RetryStrategy)
		}).
		Return(resp, nil)

	usrMgr := &UserManager{
		httpClient:           mockProvider,
		globalTimeout:        10 * time.Second,
		defaultRetryStrategy: newRetryStrategyWrapper(newFailFastRetryStrategy()),
		tracer:               &noopTracer{},
	}
	err := usrMgr.DropGroup(name, &DropGroupOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrGroupNotFound) {
		suite.T().Fatalf("Expected user not found error, %s", err)
	}
}
