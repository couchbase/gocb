package gocb

import (
	"bytes"
	"time"

	"github.com/stretchr/testify/mock"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

func (suite *UnitTestSuite) TestPingAll() {
	results := map[string]gocbcore.PingResult{
		"server1": {
			Endpoint: "server1",
			Latency:  25 * time.Millisecond,
			Scope:    "default",
		},
		"server2": {
			Endpoint: "server2",
			Latency:  42 * time.Millisecond,
			Error:    errors.New("something"),
			Scope:    "default",
		},
		"server3": {
			Endpoint: "server3",
			Latency:  100 * time.Millisecond,
			Error:    gocbcore.ErrRequestCanceled,
			Scope:    "default",
		},
	}
	pingResult := &gocbcore.PingKvResult{
		ConfigRev: 64,
		Services: []gocbcore.PingResult{
			results["server1"],
			results["server2"],
			results["server3"],
		},
	}

	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	kvProvider := new(mockKvProvider)
	kvProvider.
		On("PingKvEx", gocbcore.PingKvOptions{}, mock.AnythingOfType("gocbcore.PingKvExCallback")).
		Run(func(args mock.Arguments) {
			cb := args.Get(1).(gocbcore.PingKvExCallback)
			cb(pingResult, nil)
		}).
		Return(pendingOp, nil)

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
	cli.On("getKvProvider").Return(kvProvider, nil)
	cli.On("getHTTPProvider").Return(httpProvider, nil)

	b := &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			KvTimeout:        1000 * time.Second,
			AnalyticsTimeout: 1000 * time.Second,
			QueryTimeout:     1000 * time.Second,
			SearchTimeout:    1000 * time.Second,
			cachedClient:     cli,
		},
	}

	report, err := b.Ping(nil)
	if err != nil {
		suite.T().Fatalf("Expected ping to not return error but was %v", err)
	}

	if report.ID == "" {
		suite.T().Fatalf("Report ID was empty")
	}

	if len(report.Services) != 4 {
		suite.T().Fatalf("Expected services length to be 4 but was %d", len(report.Services))
	}

	for serviceType, services := range report.Services {
		for _, service := range services {
			switch serviceType {
			case ServiceTypeQuery:
				if service.Remote != "http://localhost:8093" {
					suite.T().Fatalf("Expected service RemoteAddr to be http://localhost:8093 but was %s", service.Remote)
				}

				if service.State != PingStateOk {
					suite.T().Fatalf("Expected service state to be ok but was %d", service.State)
				}

				if service.Latency < 50*time.Millisecond {
					suite.T().Fatalf("Expected service latency to be over 50ms but was %d", service.Latency)
				}
			case ServiceTypeSearch:
				if service.Remote != "http://localhost:8094" {
					suite.T().Fatalf("Expected service RemoteAddr to be http://localhost:8094 but was %s", service.Remote)
				}

				if service.State != PingStateError {
					suite.T().Fatalf("Expected service State to be error but was %d", service.State)
				}

				if service.Latency != 0 {
					suite.T().Fatalf("Expected service latency to be 0 but was %d", service.Latency)
				}
			case ServiceTypeAnalytics:
				if service.Remote != "http://localhost:8095" {
					suite.T().Fatalf("Expected service RemoteAddr to be http://localhost:8095 but was %s", service.Remote)
				}

				if service.State != PingStateOk {
					suite.T().Fatalf("Expected service state to be ok but was %d", service.State)
				}

				if service.Latency < 20*time.Millisecond {
					suite.T().Fatalf("Expected service latency to be over 20ms but was %d", service.Latency)
				}
			case ServiceTypeKeyValue:
				expected, ok := results[service.Remote]
				if !ok {
					suite.T().Fatalf("Unexpected service endpoint: %s", service.Remote)
				}
				if service.Latency != expected.Latency {
					suite.T().Fatalf("Expected service Latency to be %s but was %s", expected.Latency, service.Latency)
				}

				if expected.Error != nil {
					if service.State != PingStateError {
						suite.T().Fatalf("Service success should have been error, was %d", service.State)
					}
				} else {
					if service.State != PingStateOk {
						suite.T().Fatalf("Service success should have been ok, was %d", service.State)
					}
				}
			default:
				suite.T().Fatalf("Unexpected service type: %d", serviceType)
			}
		}
	}
}
