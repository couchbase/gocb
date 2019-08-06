package gocb

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v8"
)

type diagnosticsProvider interface {
	Diagnostics() (*gocbcore.DiagnosticInfo, error)
}

func diagServiceString(service ServiceType) string {
	switch service {
	case MemdService:
		return "kv"
	case CapiService:
		return "view"
	case MgmtService:
		return "mgmt"
	case QueryService:
		return "n1ql"
	case SearchService:
		return "fts"
	case AnalyticsService:
		return "cbas"
	}
	return ""
}

func diagStringService(service string) ServiceType {
	switch service {
	case "kv":
		return MemdService
	case "view":
		return CapiService
	case "mgmt":
		return MgmtService
	case "n1ql":
		return QueryService
	case "fts":
		return SearchService
	case "cbas":
		return AnalyticsService
	}
	return ServiceType(0)
}

// DiagConnState represents the state of a connection in a diagnostics report.
type DiagConnState int

const (
	// DiagStateOk indicates that the connection state is ok.
	DiagStateOk = DiagConnState(0)

	// DiagStateDisconnected indicates that the connection is disconnected.
	DiagStateDisconnected = DiagConnState(1)
)

func diagStateString(state DiagConnState) string {
	switch state {
	case DiagStateOk:
		return "ok"
	case DiagStateDisconnected:
		return "disconnected"
	}
	return ""
}

// DiagnosticEntry represents a single entry in a diagnostics report.
type DiagnosticEntry struct {
	Service      ServiceType
	State        DiagConnState
	LocalAddr    string
	RemoteAddr   string
	LastActivity time.Time
}

// DiagnosticsResult encapsulates the results of a Diagnostics operation.
type DiagnosticsResult struct {
	ID        string
	ConfigRev int64
	SDK       string
	Services  []DiagnosticEntry
}

type jsonDiagnosticEntry struct {
	State          string `json:"state"`
	Remote         string `json:"remote"`
	Local          string `json:"local"`
	LastActivityUs uint64 `json:"last_activity_us"`
}

type jsonDiagnosticReport struct {
	Version   int                              `json:"version"`
	ID        string                           `json:"id"`
	ConfigRev int64                            `json:"config_rev"`
	SDK       string                           `json:"sdk"`
	Services  map[string][]jsonDiagnosticEntry `json:"services"`
}

// MarshalJSON generates a JSON representation of this diagnostics report.
func (report *DiagnosticsResult) MarshalJSON() ([]byte, error) {
	jsonReport := jsonDiagnosticReport{
		Version:   1,
		ID:        report.ID,
		Services:  make(map[string][]jsonDiagnosticEntry),
		SDK:       report.SDK,
		ConfigRev: report.ConfigRev,
	}

	for _, service := range report.Services {
		serviceStr := diagServiceString(service.Service)
		if serviceStr == "" {
			serviceStr = "unknown"
		}

		stateStr := diagStateString(service.State)
		if stateStr == "" {
			stateStr = "unknown"
		}

		jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonDiagnosticEntry{
			State:          stateStr,
			Remote:         service.RemoteAddr,
			Local:          service.LocalAddr,
			LastActivityUs: uint64(time.Now().Sub(service.LastActivity).Nanoseconds()),
		})
	}

	return json.Marshal(&jsonReport)
}

// DiagnosticsOptions are the options that are available for use with the Diagnostics operation.
type DiagnosticsOptions struct {
	ReportID string
}

// Diagnostics returns information about the internal state of the SDK.
//
// Volatile: This API is subject to change at any time.
func (c *Cluster) Diagnostics(opts *DiagnosticsOptions) (*DiagnosticsResult, error) {
	if opts == nil {
		opts = &DiagnosticsOptions{}
	}

	if opts.ReportID == "" {
		opts.ReportID = uuid.New().String()
	}

	cli, err := c.randomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getDiagnosticsProvider()
	if err != nil {
		return nil, err
	}

	agentReport, err := provider.Diagnostics()
	if err != nil {
		return nil, err
	}

	report := &DiagnosticsResult{
		ID:        opts.ReportID,
		ConfigRev: agentReport.ConfigRev,
		SDK:       Identifier(),
	}

	for _, conn := range agentReport.MemdConns {
		state := DiagStateDisconnected
		if conn.LocalAddr != "" {
			state = DiagStateOk
		}

		report.Services = append(report.Services, DiagnosticEntry{
			Service:      MemdService,
			State:        state,
			LocalAddr:    conn.LocalAddr,
			RemoteAddr:   conn.RemoteAddr,
			LastActivity: conn.LastActivity,
		})
	}

	return report, nil
}
