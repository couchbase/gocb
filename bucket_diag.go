package gocb

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type DiagConnState int

const (
	DiagStateOk           = DiagConnState(0)
	DiagStateDisconnected = DiagConnState(1)
)

func diagStateString(state DiagConnState) string {
	switch state {
	case DiagStateOk:
		return "ok"
	case DiagStateDisconnected:
		return "disconnected"
	}
	return "?"
}

type DiagnosticEntry struct {
	Service      ServiceType
	State        DiagConnState
	LocalAddr    string
	RemoteAddr   string
	LastActivity time.Time
}

type DiagnosticReport struct {
	services []DiagnosticEntry
}

type jsonDiagnosticEntry struct {
	State          string `json:"state"`
	Remote         string `json:"remote"`
	Local          string `json:"local"`
	LastActivityUs uint64 `json:"last_activity_us"`
}

type jsonDiagnosticReport struct {
	Version   int                              `json:"version"`
	Id        string                           `json:"id"`
	ConfigRev int                              `json:"config_rev"`
	Sdk       string                           `json:"sdk"`
	Services  map[string][]jsonDiagnosticEntry `json:"services"`
}

func (report *DiagnosticReport) MarshalJSON() ([]byte, error) {
	jsonReport := jsonDiagnosticReport{
		Version:  1,
		Id:       uuid.New().String(),
		Services: make(map[string][]jsonDiagnosticEntry),
	}

	for _, service := range report.services {
		serviceStr := diagServiceString(service.Service)
		stateStr := diagStateString(service.State)

		jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonDiagnosticEntry{
			State:          stateStr,
			Remote:         service.RemoteAddr,
			Local:          service.LocalAddr,
			LastActivityUs: uint64(time.Now().Sub(service.LastActivity).Nanoseconds()),
		})
	}

	return json.Marshal(&jsonReport)
}
