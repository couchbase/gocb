package gocb

import (
	"testing"
)

func TestDiagnostics(t *testing.T) {
	report, err := globalBucket.Diagnostics()
	if err != nil {
		t.Fatalf("Failed to fetch diagnostics: %s", err)
	}

	if len(report.Services) == 0 {
		t.Fatalf("Diagnostics report contained no services")
	}

	for _, service := range report.Services {
		if service.RemoteAddr == "" {
			t.Fatalf("Diagnostic report contained invalid entry")
		}
	}
}

func TestPing(t *testing.T) {
	// We only test the main services, which are the ones support
	// by our mock at the moment.
	report, err := globalBucket.Ping([]ServiceType{
		MemdService,
		CapiService,
		N1qlService,
	})
	if err != nil {
		t.Fatalf("Failed to perform ping: %s", err)
	}

	if len(report.Services) == 0 {
		t.Fatalf("Ping report contained no services")
	}

	logDebugf("%+v", report)

	for _, service := range report.Services {
		if service.Latency == 0 {
			t.Fatalf("Ping report contained invalid entry")
		}
	}
}
