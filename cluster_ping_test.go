package gocb

import (
	"bytes"
	"time"

	"github.com/stretchr/testify/mock"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

func (suite *IntegrationTestSuite) TestClusterPingAll() {
	if !globalCluster.SupportsFeature(PingFeature) {
		suite.T().Skip("Skipping test as ping not supported")
	}
	report, err := globalCluster.Ping(nil)
	suite.Require().Nil(err)

	suite.Assert().NotEmpty(report.ID)

	numServices := 1
	if globalCluster.SupportsFeature(SearchFeature) {
		numServices++
	}
	if globalCluster.SupportsFeature(AnalyticsFeature) {
		numServices++
	}

	suite.Assert().Len(report.Services, numServices)

	for serviceType, services := range report.Services {
		for _, service := range services {
			switch serviceType {
			case ServiceTypeQuery:
				suite.Assert().NotEmpty(service.Remote)
				suite.Assert().Equal(PingStateOk, service.State)
				suite.Assert().NotZero(int64(service.Latency))
			case ServiceTypeSearch:
				suite.Assert().NotEmpty(service.Remote)
				suite.Assert().Equal(PingStateOk, service.State)
				suite.Assert().NotZero(int64(service.Latency))
			case ServiceTypeAnalytics:
				suite.Assert().NotEmpty(service.Remote)
				suite.Assert().Equal(PingStateOk, service.State)
				suite.Assert().NotZero(int64(service.Latency))
			default:
				suite.T().Fatalf("Unexpected service type: %d", serviceType)
			}
		}
	}
}

func (suite *UnitTestSuite) TestClusterPingAll() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	httpProvider := new(mockHttpProvider)
	httpProvider.
		On("DoHTTPRequest", mock.MatchedBy(func(req *gocbcore.HTTPRequest) bool {
			return req.Service == gocbcore.N1qlService
		})).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)
			req.Endpoint = "http://localhost:8093"
		}).
		After(50*time.Millisecond).
		Return(&gocbcore.HTTPResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBufferString(""), nil},
		}, nil)

	httpProvider.
		On("DoHTTPRequest", mock.MatchedBy(func(req *gocbcore.HTTPRequest) bool {
			return req.Service == gocbcore.FtsService
		})).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)
			req.Endpoint = "http://localhost:8094"
		}).
		Return(nil, errors.New("some error occurred"))

	httpProvider.
		On("DoHTTPRequest", mock.MatchedBy(func(req *gocbcore.HTTPRequest) bool {
			return req.Service == gocbcore.CbasService
		})).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)
			req.Endpoint = "http://localhost:8095"
		}).
		After(20*time.Millisecond).
		Return(&gocbcore.HTTPResponse{
			Endpoint:   "http://localhost:8095",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBufferString(""), nil},
		}, nil)

	cli := new(mockClient)
	cli.On("supportsGCCCP").Return(true)
	cli.On("getHTTPProvider").Return(httpProvider, nil)

	c := &Cluster{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			KvTimeout:        1000 * time.Second,
			AnalyticsTimeout: 1000 * time.Second,
			QueryTimeout:     1000 * time.Second,
			SearchTimeout:    1000 * time.Second,
		},
		clusterClient: cli,
	}

	report, err := c.Ping(nil)
	suite.Require().Nil(err)

	suite.Assert().NotEmpty(report.ID)

	suite.Assert().Len(report.Services, 3)

	for serviceType, services := range report.Services {
		for _, service := range services {
			switch serviceType {
			case ServiceTypeQuery:
				suite.Assert().Equal("http://localhost:8093", service.Remote)
				suite.Assert().Equal(PingStateOk, service.State)
				suite.Assert().Less(int64(50*time.Millisecond), int64(service.Latency))
			case ServiceTypeSearch:
				suite.Assert().Equal("http://localhost:8094", service.Remote)
				suite.Assert().Equal(PingStateError, service.State)
				suite.Assert().Equal(time.Duration(0), service.Latency)
			case ServiceTypeAnalytics:
				suite.Assert().Equal("http://localhost:8095", service.Remote)
				suite.Assert().Equal(PingStateOk, service.State)
				suite.Assert().Less(int64(20*time.Millisecond), int64(service.Latency))
			default:
				suite.T().Fatalf("Unexpected service type: %d", serviceType)
			}
		}
	}
}

func (suite *UnitTestSuite) TestClusterPingOne() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	httpProvider := new(mockHttpProvider)
	httpProvider.
		On("DoHTTPRequest", mock.MatchedBy(func(req *gocbcore.HTTPRequest) bool {
			return req.Service == gocbcore.N1qlService
		})).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(*gocbcore.HTTPRequest)
			req.Endpoint = "http://localhost:8093"
		}).
		After(50*time.Millisecond).
		Return(&gocbcore.HTTPResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBufferString(""), nil},
		}, nil)

	cli := new(mockClient)
	cli.On("supportsGCCCP").Return(true)
	cli.On("getHTTPProvider").Return(httpProvider, nil)

	c := &Cluster{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			KvTimeout:        1000 * time.Second,
			AnalyticsTimeout: 1000 * time.Second,
			QueryTimeout:     1000 * time.Second,
			SearchTimeout:    1000 * time.Second,
		},
		clusterClient: cli,
	}

	reportID := "myreportid"
	report, err := c.Ping(&PingOptions{
		ServiceTypes: []ServiceType{ServiceTypeQuery},
		ReportID:     reportID,
	})
	suite.Require().Nil(err)

	suite.Assert().Equal(reportID, report.ID)

	suite.Assert().Len(report.Services, 1)

	suite.Assert().Contains(report.Services, ServiceTypeQuery)
	queryServices := report.Services[ServiceTypeQuery]
	suite.Assert().Len(queryServices, 1)

	service := queryServices[0]
	suite.Assert().Equal("http://localhost:8093", service.Remote)
	suite.Assert().Equal(PingStateOk, service.State)
	suite.Assert().Less(int64(50*time.Millisecond), int64(service.Latency))
}
