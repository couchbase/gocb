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
	case MgmtService:
		return "mgmt"
	case KeyValueService:
		return "kv"
	case CapiService:
		return "views"
	case QueryService:
		return "query"
	case SearchService:
		return "search"
	case AnalyticsService:
		return "analytics"
	}
	return ""
}

func diagStringService(service string) ServiceType {
	switch service {
	case "mgmt":
		return MgmtService
	case "kv":
		return KeyValueService
	case "views":
		return CapiService
	case "query":
		return QueryService
	case "search":
		return SearchService
	case "analytics":
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

// EndPointDiagnostics represents a single entry in a diagnostics report.
type EndPointDiagnostics struct {
	Type         ServiceType
	State        DiagConnState
	Local        string
	Remote       string
	LastActivity time.Time
	Scope        string
	ID           string
}

// DiagnosticsResult encapsulates the results of a Diagnostics operation.
type DiagnosticsResult struct {
	ID       string
	Version  int64
	SDK      string
	Services map[string][]EndPointDiagnostics
}

type jsonDiagnosticEntry struct {
	State          string `json:"state"`
	Remote         string `json:"remote"`
	Local          string `json:"local"`
	LastActivityUs uint64 `json:"last_activity_us"`
	Scope          string `json:"scope,omitempty"`
	ID             string `json:"id"`
}

type jsonDiagnosticReport struct {
	Version  int64                            `json:"version"`
	ID       string                           `json:"id"`
	SDK      string                           `json:"sdk"`
	Services map[string][]jsonDiagnosticEntry `json:"services"`
}

// MarshalJSON generates a JSON representation of this diagnostics report.
func (report *DiagnosticsResult) MarshalJSON() ([]byte, error) {
	jsonReport := jsonDiagnosticReport{
		Version:  1,
		ID:       report.ID,
		Services: make(map[string][]jsonDiagnosticEntry),
		SDK:      report.SDK,
	}

	for _, serviceType := range report.Services {
		for _, service := range serviceType {
			serviceStr := diagServiceString(service.Type)
			if serviceStr == "" {
				serviceStr = "unknown"
			}

			stateStr := diagStateString(service.State)
			if stateStr == "" {
				stateStr = "unknown"
			}

			jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonDiagnosticEntry{
				State:          stateStr,
				Remote:         service.Remote,
				Local:          service.Local,
				LastActivityUs: uint64(time.Now().Sub(service.LastActivity).Nanoseconds()),
				Scope:          service.Scope,
				ID:             service.ID,
			})
		}
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

	provider, err := c.getDiagnosticsProvider()
	if err != nil {
		return nil, err
	}

	agentReport, err := provider.Diagnostics()
	if err != nil {
		return nil, err
	}

	report := &DiagnosticsResult{
		ID:       opts.ReportID,
		Version:  agentReport.ConfigRev,
		SDK:      Identifier(),
		Services: make(map[string][]EndPointDiagnostics),
	}

	report.Services["kv"] = make([]EndPointDiagnostics, 0)

	for _, conn := range agentReport.MemdConns {
		state := DiagStateDisconnected
		if conn.LocalAddr != "" {
			state = DiagStateOk
		}

		report.Services["kv"] = append(report.Services["kv"], EndPointDiagnostics{
			Type:         KeyValueService,
			State:        state,
			Local:        conn.LocalAddr,
			Remote:       conn.RemoteAddr,
			LastActivity: conn.LastActivity,
			Scope:        conn.Scope,
			ID:           conn.ID,
		})
	}

	return report, nil
}
