package gocb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

type mockDiagnosticsProvider struct {
	info *gocbcore.DiagnosticInfo
	err  error
}

func (provider *mockDiagnosticsProvider) Diagnostics() (*gocbcore.DiagnosticInfo, error) {
	return provider.info, provider.err
}

func TestDiagnostics(t *testing.T) {
	layout := "2006-01-02T15:04:05.000Z"
	date1, err := time.Parse(layout, "2014-11-12T11:45:26.371Z")
	if err != nil {
		t.Fatalf("Failed to parse date: %v", err)
	}
	date2, err := time.Parse(layout, "2017-11-12T11:45:26.371Z")
	if err != nil {
		t.Fatalf("Failed to parse date: %v", err)
	}

	info := &gocbcore.DiagnosticInfo{
		ConfigRev: 1,
		MemdConns: []gocbcore.MemdConnInfo{
			{
				LastActivity: date1,
				LocalAddr:    "10.112.191.101",
				RemoteAddr:   "10.112.191.102",
				Scope:        "bucket",
				ID:           "0xc000094120",
			},
			{
				LastActivity: date2,
				LocalAddr:    "",
				RemoteAddr:   "",
				ID:           "",
			},
		},
	}

	provider := &mockDiagnosticsProvider{
		info: info,
	}
	cli := &mockClient{
		mockDiagnosticsProvider: provider,
		bucketName:              "mock",
		collectionID:            0,
		scopeID:                 0,
		useMutationTokens:       false,
	}

	clients := make(map[string]client)
	clients["mock-false"] = cli

	c := &Cluster{
		connections: clients,
	}

	report, err := c.Diagnostics(nil)
	if err != nil {
		t.Fatalf("Expected error to be nil but was %v", err)
	}

	if report.ID == "" {
		t.Fatalf("Report ID should have been not empty")
	}

	if report.Version != info.ConfigRev {
		t.Fatalf("Report Version should have been %d but was %d", info.ConfigRev, report.Version)
	}

	services, ok := report.Services["kv"]
	if !ok {
		t.Fatalf("Report missing kv service")
	}

	if len(services) != len(info.MemdConns) {
		t.Fatalf("Expected Services length to be %d but was %d", len(info.MemdConns), len(report.Services))
	}

	for i, service := range services {
		if service.Type != ServiceTypeKeyValue {
			t.Fatalf("Expected service to be KeyValueService but was %d", service.Type)
		}

		expected := info.MemdConns[i]
		if service.Remote != expected.RemoteAddr {
			t.Fatalf("Expected service Remote to be %s but was %s", expected.RemoteAddr, service.Remote)
		}
		if service.Local != expected.LocalAddr {
			t.Fatalf("Expected service Local to be %s but was %s", expected.LocalAddr, service.Local)
		}
		if service.LastActivity != expected.LastActivity {
			t.Fatalf("Expected service LastActivity to be %s but was %s", expected.LastActivity, service.LastActivity)
		}
		if service.Scope != expected.Scope {
			t.Fatalf("Expected service Scope to be %s but was %s", expected.Scope, service.Scope)
		}
		if service.ID != expected.ID {
			t.Fatalf("Expected service ID to be %s but was %s", expected.ID, service.ID)
		}

		if expected.LocalAddr == "" {
			if service.State != DiagStateDisconnected {
				t.Fatalf("Expected service state to be disconnected but was %d", service.State)
			}
		} else {
			if service.State != DiagStateOk {
				t.Fatalf("Expected service state to be ok but was %d", service.State)
			}
		}
	}

	marshaled, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Failed to Marshal report: %v", err)
	}

	var jsonReport jsonDiagnosticReport
	err = json.Unmarshal(marshaled, &jsonReport)
	if err != nil {
		t.Fatalf("Failed to Unmarshal report: %v", err)
	}

	if jsonReport.ID != report.ID {
		t.Fatalf("Expected json report ID to be %s but was %s", report.ID, jsonReport.ID)
	}

	if jsonReport.Version != report.Version {
		t.Fatalf("Expected json report Version to be %d but was %d", report.Version, jsonReport.Version)
	}

	if jsonReport.Version != 1 {
		t.Fatalf("Expected json report Version to be 1 but was %d", jsonReport.Version)
	}

	if jsonReport.SDK != Identifier() {
		t.Fatalf("Expected json report SDK to be %s but was %s", Identifier(), jsonReport.SDK)
	}

	if len(jsonReport.Services) != 1 {
		t.Fatalf("Expected json report Services to be of length 1 but was %d", len(jsonReport.Services))
	}

	jsonServices, ok := jsonReport.Services["kv"]
	if !ok {
		t.Fatalf("Expected json report services to contain kv but didn't")
	}

	if len(services) != len(jsonServices) {
		t.Fatalf("Expected json report Services length to be %d but was %d", len(report.Services), len(services))
	}

	for i, service := range jsonServices {
		expected := services[i]
		if service.Remote != expected.Remote {
			t.Fatalf("Expected service Remote to be %s but was %s", expected.Remote, service.Remote)
		}
		if service.Local != expected.Local {
			t.Fatalf("Expected service Local to be %s but was %s", expected.Local, service.Local)
		}
		if service.LastActivityUs == 0 {
			t.Fatalf("Expected service LastActivityUs to be non zero but was %d", service.LastActivityUs)
		}
		if service.Scope != expected.Scope {
			t.Fatalf("Expected service Scope to be %s but was %s", expected.Scope, service.Scope)
		}
		if service.ID != expected.ID {
			t.Fatalf("Expected service Scope to be %s but was %s", expected.ID, service.ID)
		}

		if expected.Local == "" {
			if service.State != "disconnected" {
				t.Fatalf("Expected service state to be disconnected but was %s", service.State)
			}
		} else {
			if service.State != "ok" {
				t.Fatalf("Expected service state to be ok but was %s", service.State)
			}
		}
	}
}

func TestDiagnosticsWithID(t *testing.T) {
	provider := &mockDiagnosticsProvider{
		info: &gocbcore.DiagnosticInfo{
			ConfigRev: 1,
		},
	}
	cli := &mockClient{
		mockDiagnosticsProvider: provider,
		bucketName:              "mock",
		collectionID:            0,
		scopeID:                 0,
		useMutationTokens:       false,
	}

	clients := make(map[string]client)
	clients["mock-false"] = cli

	c := &Cluster{
		connections: clients,
	}

	report, err := c.Diagnostics(&DiagnosticsOptions{ReportID: "myreportid"})
	if err != nil {
		t.Fatalf("Expected error to be nil but was %v", err)
	}

	if report.ID != "myreportid" {
		t.Fatalf("Report ID should have been myreportid but was %s", report.ID)
	}
}
