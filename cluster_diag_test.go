package gocb

import (
	"encoding/json"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocbcore/v9"
)

func (suite *UnitTestSuite) TestDiagnostics() {
	layout := "2006-01-02T15:04:05.000Z"
	date1, err := time.Parse(layout, "2014-11-12T11:45:26.371Z")
	if err != nil {
		suite.T().Fatalf("Failed to parse date: %v", err)
	}
	date2, err := time.Parse(layout, "2017-11-12T11:45:26.371Z")
	if err != nil {
		suite.T().Fatalf("Failed to parse date: %v", err)
	}

	info := &gocbcore.DiagnosticInfo{
		ConfigRev: 1,
		MemdConns: []gocbcore.MemdConnInfo{
			{
				LastActivity: date1,
				LocalAddr:    "10.112.191.101",
				RemoteAddr:   "10.112.191.102",
				Scope:        "bucket",
				State:        gocbcore.EndpointStateConnected,
				ID:           "0xc000094120",
			},
			{
				LastActivity: date2,
				LocalAddr:    "",
				RemoteAddr:   "",
				ID:           "",
				State:        gocbcore.EndpointStateDisconnected,
			},
		},
	}

	provider := new(mockDiagnosticsProvider)
	provider.
		On("Diagnostics", mock.AnythingOfType("gocbcore.DiagnosticsOptions")).
		Return(info, nil)

	cli := new(mockConnectionManager)
	cli.On("getDiagnosticsProvider", "").Return(provider, nil)

	c := &Cluster{
		connectionManager: cli,
	}

	report, err := c.Diagnostics(nil)
	if err != nil {
		suite.T().Fatalf("Expected error to be nil but was %v", err)
	}

	if report.ID == "" {
		suite.T().Fatalf("Report ID should have been not empty")
	}

	services, ok := report.Services["kv"]
	if !ok {
		suite.T().Fatalf("Report missing kv service")
	}

	if len(services) != len(info.MemdConns) {
		suite.T().Fatalf("Expected Services length to be %d but was %d", len(info.MemdConns), len(report.Services))
	}

	for i, service := range services {
		if service.Type != ServiceTypeKeyValue {
			suite.T().Fatalf("Expected service to be KeyValueService but was %d", service.Type)
		}

		expected := info.MemdConns[i]
		if service.Remote != expected.RemoteAddr {
			suite.T().Fatalf("Expected service Remote to be %s but was %s", expected.RemoteAddr, service.Remote)
		}
		if service.Local != expected.LocalAddr {
			suite.T().Fatalf("Expected service Local to be %s but was %s", expected.LocalAddr, service.Local)
		}
		if service.LastActivity != expected.LastActivity {
			suite.T().Fatalf("Expected service LastActivity to be %s but was %s", expected.LastActivity, service.LastActivity)
		}
		if service.Namespace != expected.Scope {
			suite.T().Fatalf("Expected service Scope to be %s but was %s", expected.Scope, service.Namespace)
		}
		if service.ID != expected.ID {
			suite.T().Fatalf("Expected service ID to be %s but was %s", expected.ID, service.ID)
		}
		if service.State != EndpointState(expected.State) {
			suite.T().Fatalf("Expected service state to be %s but was %s", endpointStateToString(EndpointState(expected.State)),
				endpointStateToString(service.State))
		}
	}

	marshaled, err := json.Marshal(report)
	if err != nil {
		suite.T().Fatalf("Failed to Marshal report: %v", err)
	}

	var jsonReport jsonDiagnosticReport
	err = json.Unmarshal(marshaled, &jsonReport)
	if err != nil {
		suite.T().Fatalf("Failed to Unmarshal report: %v", err)
	}

	if jsonReport.ID != report.ID {
		suite.T().Fatalf("Expected json report ID to be %s but was %s", report.ID, jsonReport.ID)
	}

	if jsonReport.Version != 2 {
		suite.T().Fatalf("Expected json report Version to be 1 but was %d", jsonReport.Version)
	}

	if jsonReport.SDK != Identifier() {
		suite.T().Fatalf("Expected json report SDK to be %s but was %s", Identifier(), jsonReport.SDK)
	}

	if len(jsonReport.Services) != 1 {
		suite.T().Fatalf("Expected json report Services to be of length 1 but was %d", len(jsonReport.Services))
	}

	jsonServices, ok := jsonReport.Services["kv"]
	if !ok {
		suite.T().Fatalf("Expected json report services to contain kv but didn't")
	}

	if len(services) != len(jsonServices) {
		suite.T().Fatalf("Expected json report Services length to be %d but was %d", len(report.Services), len(services))
	}

	for i, service := range jsonServices {
		expected := services[i]
		if service.Remote != expected.Remote {
			suite.T().Fatalf("Expected service Remote to be %s but was %s", expected.Remote, service.Remote)
		}
		if service.Local != expected.Local {
			suite.T().Fatalf("Expected service Local to be %s but was %s", expected.Local, service.Local)
		}
		if service.LastActivityUs == 0 {
			suite.T().Fatalf("Expected service LastActivityUs to be non zero but was %d", service.LastActivityUs)
		}
		if service.Namespace != expected.Namespace {
			suite.T().Fatalf("Expected service Scope to be %s but was %s", expected.Namespace, service.Namespace)
		}
		if service.ID != expected.ID {
			suite.T().Fatalf("Expected service Scope to be %s but was %s", expected.ID, service.ID)
		}

		if service.State != endpointStateToString(expected.State) {
			suite.T().Fatalf("Expected service state to be %s but was %s", endpointStateToString(expected.State),
				service.State)
		}
	}
}

func (suite *UnitTestSuite) TestDiagnosticsWithID() {
	provider := new(mockDiagnosticsProvider)
	provider.
		On("Diagnostics", mock.AnythingOfType("gocbcore.DiagnosticsOptions")).
		Return(&gocbcore.DiagnosticInfo{
			ConfigRev: 1,
		}, nil)

	cli := new(mockConnectionManager)
	cli.On("getDiagnosticsProvider", "").Return(provider, nil)

	c := &Cluster{
		connectionManager: cli,
	}

	report, err := c.Diagnostics(&DiagnosticsOptions{ReportID: "myreportid"})
	if err != nil {
		suite.T().Fatalf("Expected error to be nil but was %v", err)
	}

	if report.ID != "myreportid" {
		suite.T().Fatalf("Report ID should have been myreportid but was %s", report.ID)
	}
}
