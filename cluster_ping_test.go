package gocb

import (
	"errors"
	"time"

	"github.com/stretchr/testify/mock"

	gocbcore "github.com/couchbase/gocbcore/v10"
)

func (suite *IntegrationTestSuite) TestClusterPingAll() {
	suite.skipIfUnsupported(PingFeature)

	report, err := globalCluster.Ping(nil)
	suite.Require().Nil(err)

	suite.Assert().NotEmpty(report.ID)

	numServices := 3
	if globalCluster.SupportsFeature(PingAnalyticsFeature) {
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
			case ServiceTypeManagement:
				suite.Assert().NotEmpty(service.Remote)
				suite.Assert().Equal(PingStateOk, service.State)
				suite.Assert().NotZero(int64(service.Latency))
			case ServiceTypeAnalytics:
				if globalCluster.SupportsFeature(PingAnalyticsFeature) {
					suite.Assert().NotEmpty(service.Remote)
					suite.Assert().Equal(PingStateOk, service.State)
					suite.Assert().NotZero(int64(service.Latency))
				}
			default:
				suite.T().Fatalf("Unexpected service type: %d", serviceType)
			}
		}
	}
}

func (suite *UnitTestSuite) pingCluster(runFn func(args mock.Arguments), args ...interface{}) *Cluster {
	pingProviderCoreProvider := new(mockDiagnosticsProviderCoreProvider)
	call := pingProviderCoreProvider.
		On("Ping", nil, mock.AnythingOfType("gocbcore.PingOptions")).
		Return(args...)

	if runFn != nil {
		call.Run(runFn)
	}

	pingProvider := &diagnosticsProviderCore{
		provider: pingProviderCoreProvider,
	}

	cli := new(mockConnectionManager)
	cli.On("getDiagnosticsProvider", mock.AnythingOfType("string")).Return(pingProvider, nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	c := &Cluster{
		timeoutsConfig: TimeoutsConfig{
			KVTimeout:        1000 * time.Second,
			AnalyticsTimeout: 1000 * time.Second,
			QueryTimeout:     1000 * time.Second,
			SearchTimeout:    1000 * time.Second,
		},
		connectionManager: cli,
	}

	pingProvider.tracer = newTracerWrapper(&NoopTracer{})
	pingProvider.timeouts = c.timeoutsConfig

	return c
}

func (suite *UnitTestSuite) TestClusterPingAll() {
	expectedResults := map[gocbcore.ServiceType][]gocbcore.EndpointPingResult{
		gocbcore.N1qlService: {
			{
				Endpoint: "server1",
				Latency:  50 * time.Millisecond,
				Scope:    "default",
				State:    gocbcore.PingStateOK,
			},
			{
				Endpoint: "server2",
				Latency:  34 * time.Millisecond,
				Error:    errors.New("something"),
				Scope:    "default",
				State:    gocbcore.PingStateError,
			},
		},
		gocbcore.CbasService: {
			{
				Endpoint: "server1",
				Latency:  50 * time.Millisecond,
				Scope:    "default",
				State:    gocbcore.PingStateOK,
			},
		},
		gocbcore.FtsService: {
			{
				Endpoint: "server3",
				Latency:  20 * time.Millisecond,
				Scope:    "default",
				State:    gocbcore.PingStateOK,
			},
		},
	}
	pingResult := &gocbcore.PingResult{
		ConfigRev: 64,
		Services:  expectedResults,
	}

	c := suite.pingCluster(func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.PingOptions)

		if len(opts.ServiceTypes) != 0 {
			suite.T().Errorf("Expected service types to be len 0 but was %v", opts.ServiceTypes)
		}
	}, pingResult, nil)

	report, err := c.Ping(nil)
	if err != nil {
		suite.T().Fatalf("Expected ping to not return error but was %v", err)
	}

	if report.ID == "" {
		suite.T().Fatalf("Report ID was empty")
	}

	if len(report.Services) != 3 {
		suite.T().Fatalf("Expected services length to be 3 but was %d", len(report.Services))
	}

	for serviceType, services := range report.Services {
		expectedServices, ok := expectedResults[gocbcore.ServiceType(serviceType)]
		if !ok {
			suite.T().Errorf("Unexpected service type in result: %v", serviceType)
			continue
		}
		for i, service := range services {
			expectedService := expectedServices[i]

			suite.Assert().Equal(expectedService.Latency, service.Latency)
			suite.Assert().Equal(expectedService.Scope, service.Namespace)
			if expectedService.Error == nil {
				suite.Assert().Empty(service.Error)
			} else {
				suite.Assert().Equal(expectedService.Error.Error(), service.Error)
			}
			suite.Assert().Equal(PingState(expectedService.State), service.State)
			suite.Assert().Equal(expectedService.Endpoint, service.Remote)
			suite.Assert().Equal(expectedService.ID, service.ID)
		}
	}
}

func (suite *UnitTestSuite) TestClusterPingOne() {
	expectedResults := map[gocbcore.ServiceType][]gocbcore.EndpointPingResult{
		gocbcore.N1qlService: {
			{
				Endpoint: "server1",
				Latency:  50 * time.Millisecond,
				Scope:    "default",
				State:    gocbcore.PingStateOK,
			},
			{
				Endpoint: "server2",
				Latency:  34 * time.Millisecond,
				Error:    errors.New("something"),
				Scope:    "default",
				State:    gocbcore.PingStateError,
			},
		},
	}
	pingResult := &gocbcore.PingResult{
		ConfigRev: 64,
		Services:  expectedResults,
	}

	c := suite.pingCluster(func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.PingOptions)

		if len(opts.ServiceTypes) != 1 {
			suite.T().Errorf("Expected service types to be len 1 but was %v", opts.ServiceTypes)
		}
	}, pingResult, nil)

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
	suite.Assert().Len(queryServices, 2)

	for i, service := range queryServices {
		expectedService := expectedResults[gocbcore.N1qlService][i]

		suite.Assert().Equal(expectedService.Latency, service.Latency)
		suite.Assert().Equal(expectedService.Scope, service.Namespace)
		if expectedService.Error == nil {
			suite.Assert().Empty(service.Error)
		} else {
			suite.Assert().Equal(expectedService.Error.Error(), service.Error)
		}
		suite.Assert().Equal(PingState(expectedService.State), service.State)
		suite.Assert().Equal(expectedService.Endpoint, service.Remote)
		suite.Assert().Equal(expectedService.ID, service.ID)
	}
}
