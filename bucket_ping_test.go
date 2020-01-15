package gocb

import (
	"bytes"
	"testing"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

func TestPingAll(t *testing.T) {
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

	doHTTP := func(req *gocbcore.HTTPRequest) (*gocbcore.HTTPResponse, error) {
		var endpoint string
		switch req.Service {
		case gocbcore.N1qlService:
			<-time.After(50 * time.Millisecond)
			endpoint = "http://localhost:8093"

			req.Endpoint = endpoint

			return &gocbcore.HTTPResponse{
				Endpoint:   endpoint,
				StatusCode: 200,
				Body:       &testReadCloser{bytes.NewBufferString(""), nil},
			}, nil
		case gocbcore.FtsService:
			req.Endpoint = "http://localhost:8094"
			return nil, errors.New("some error occurred")
		case gocbcore.CbasService:
			<-time.After(20 * time.Millisecond)
			endpoint = "http://localhost:8095"

			req.Endpoint = endpoint

			return &gocbcore.HTTPResponse{
				Endpoint:   endpoint,
				StatusCode: 200,
				Body:       &testReadCloser{bytes.NewBufferString(""), nil},
			}, nil
		default:
			return nil, errors.New("unexpected service type")
		}
	}

	kvProvider := &mockKvProvider{
		value: pingResult,
	}

	httpProvider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	clients := make(map[string]client)
	cli := &mockClient{
		bucketName:        "mock",
		collectionID:      0,
		scopeID:           0,
		useMutationTokens: false,
		mockKvProvider:    kvProvider,
		mockHTTPProvider:  httpProvider,
	}
	clients["mock"] = cli
	c := &Cluster{
		connections: clients,
		sb: stateBlock{
			KvTimeout: 1000 * time.Millisecond,
		},
	}

	b := &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			KvTimeout:        c.sb.KvTimeout,
			AnalyticsTimeout: c.sb.AnalyticsTimeout,
			QueryTimeout:     c.sb.QueryTimeout,
			SearchTimeout:    c.sb.SearchTimeout,
			cachedClient:     cli,
		},
	}

	report, err := b.Ping(nil)
	if err != nil {
		t.Fatalf("Expected ping to not return error but was %v", err)
	}

	if report.ID == "" {
		t.Fatalf("Report ID was empty")
	}

	if len(report.Services) != 4 {
		t.Fatalf("Expected services length to be 6 but was %d", len(report.Services))
	}

	for serviceType, services := range report.Services {
		for _, service := range services {
			switch serviceType {
			case ServiceTypeQuery:
				if service.Remote != "http://localhost:8093" {
					t.Fatalf("Expected service RemoteAddr to be http://localhost:8093 but was %s", service.Remote)
				}

				if service.State != PingStateOk {
					t.Fatalf("Expected service state to be ok but was %d", service.State)
				}

				if service.Latency < 50*time.Millisecond {
					t.Fatalf("Expected service latency to be over 50ms but was %d", service.Latency)
				}
			case ServiceTypeSearch:
				if service.Remote != "http://localhost:8094" {
					t.Fatalf("Expected service RemoteAddr to be http://localhost:8094 but was %s", service.Remote)
				}

				if service.State != PingStateError {
					t.Fatalf("Expected service State to be error but was %d", service.State)
				}

				if service.Latency != 0 {
					t.Fatalf("Expected service latency to be 0 but was %d", service.Latency)
				}
			case ServiceTypeAnalytics:
				if service.Remote != "http://localhost:8095" {
					t.Fatalf("Expected service RemoteAddr to be http://localhost:8095 but was %s", service.Remote)
				}

				if service.State != PingStateOk {
					t.Fatalf("Expected service state to be ok but was %d", service.State)
				}

				if service.Latency < 20*time.Millisecond {
					t.Fatalf("Expected service latency to be over 20ms but was %d", service.Latency)
				}
			case ServiceTypeKeyValue:
				expected, ok := results[service.Remote]
				if !ok {
					t.Fatalf("Unexpected service endpoint: %s", service.Remote)
				}
				if service.Latency != expected.Latency {
					t.Fatalf("Expected service Latency to be %s but was %s", expected.Latency, service.Latency)
				}

				if expected.Error != nil {
					if service.State != PingStateError {
						t.Fatalf("Service success should have been error, was %d", service.State)
					}
				} else {
					if service.State != PingStateOk {
						t.Fatalf("Service success should have been ok, was %d", service.State)
					}
				}
			default:
				t.Fatalf("Unexpected service type: %d", serviceType)
			}
		}
	}
}
