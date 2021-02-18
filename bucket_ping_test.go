package gocb

import (
	"time"

	"github.com/stretchr/testify/mock"

	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/pkg/errors"
)

func (suite *UnitTestSuite) TestPingAll() {
	expectedResults := map[gocbcore.ServiceType][]gocbcore.EndpointPingResult{
		gocbcore.MemdService: {
			{
				Endpoint: "server1",
				Latency:  25 * time.Millisecond,
				Scope:    "default",
				State:    gocbcore.PingStateOK,
			},
			{
				Endpoint: "server2",
				Latency:  42 * time.Millisecond,
				Error:    errors.New("something"),
				Scope:    "default",
				State:    gocbcore.PingStateError,
			},
			{
				Endpoint: "server3",
				Latency:  100 * time.Millisecond,
				Error:    gocbcore.ErrUnambiguousTimeout,
				Scope:    "default",
				State:    gocbcore.PingStateTimeout,
			},
		},
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
		gocbcore.CapiService: {
			{
				Endpoint: "server2",
				Latency:  30 * time.Millisecond,
				Error:    gocbcore.ErrUnambiguousTimeout,
				Scope:    "default",
				State:    gocbcore.PingStateTimeout,
			},
		},
	}
	pingResult := &gocbcore.PingResult{
		ConfigRev: 64,
		Services:  expectedResults,
	}

	pingProvider := new(mockDiagnosticsProvider)
	pingProvider.
		On("Ping", nil, mock.AnythingOfType("gocbcore.PingOptions")).
		Run(func(args mock.Arguments) {
			if len(args) != 2 {
				suite.T().Fatalf("Expected options to contain two arguments, was: %v", args)
			}
			opts := args.Get(1).(gocbcore.PingOptions)

			if len(opts.ServiceTypes) != 0 {
				suite.T().Errorf("Expected service types to be len 0 but was %v", opts.ServiceTypes)
			}
		}).
		Return(pingResult, nil)

	cli := new(mockConnectionManager)
	cli.On("getDiagnosticsProvider", "mock").Return(pingProvider, nil)

	b := suite.bucket("mock", suite.defaultTimeoutConfig(), cli)

	report, err := b.Ping(nil)
	if err != nil {
		suite.T().Fatalf("Expected ping to not return error but was %v", err)
	}

	if report.ID == "" {
		suite.T().Fatalf("Report ID was empty")
	}

	if len(report.Services) != 5 {
		suite.T().Fatalf("Expected services length to be 5 but was %d", len(report.Services))
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
