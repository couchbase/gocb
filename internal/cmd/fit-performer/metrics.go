package main

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"time"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol"
	metricspb "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/metrics"
	runpb "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MetricsReporterDelay = 1 * time.Second
)

type Metrics struct {
	MemHeapUsedMB      uint64  `json:"memHeapUsedMB"`
	ProcessCPU         float64 `json:"processCpu"`
	SystemCPU          float64 `json:"systemCpu"`
	MachineThreadCount int32   `json:"machineThreadCount"`
	GoroutineCount     int32   `json:"threadCount"`
}

type MetricsReporter struct {
	runID    string
	server   protocol.PerformerService_RunServer
	logger   *logrus.Logger
	process  *process.Process
	ctx      context.Context
	cancelFn context.CancelFunc

	successCount uint64
	errorCount   uint64
}

func NewMetricsReporter(runID string, server protocol.PerformerService_RunServer, logger *logrus.Logger) (*MetricsReporter, error) {
	return NewMetricsReporterWithContext(context.Background(), runID, server, logger)
}

func NewMetricsReporterWithContext(ctx context.Context, runID string, server protocol.PerformerService_RunServer, logger *logrus.Logger) (*MetricsReporter, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}

	ctx, cancelFn := context.WithCancel(ctx)

	return &MetricsReporter{
		runID:    runID,
		server:   server,
		logger:   logger,
		process:  p,
		ctx:      ctx,
		cancelFn: cancelFn,
	}, nil
}

func (r *MetricsReporter) Start() {
	go r.report()
}

func (r *MetricsReporter) Stop() {
	r.logger.Infof("Stopping metrics reporter. PID=%d. Run ID=%s.", r.process.Pid, r.runID)
	r.cancelFn()
}

func (r *MetricsReporter) report() {
	r.logger.Infof("Starting metrics reporter. PID=%d. Run ID=%s.", r.process.Pid, r.runID)
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Infof("Stopped metrics reporter. Reported metrics %d times (%d errors). PID=%d. Run ID=%s.", r.successCount, r.errorCount, r.process.Pid, r.runID)
			return
		case <-time.After(MetricsReporterDelay):
			metrics, err := r.collectMetrics()
			if err != nil {
				r.logger.Warnf("Failed to collect metrics: %v.", err)
				r.errorCount++
				continue
			}
			encoded, err := json.Marshal(metrics)
			if err != nil {
				r.logger.Warnf("Failed to encode metrics: %v.", err)
				r.errorCount++
				continue
			}

			err = r.server.Send(&runpb.Result{
				Result: &runpb.Result_Metrics{
					Metrics: &metricspb.Result{
						Metrics: string(encoded),
					},
				},
			})
			if err != nil {
				grpcCode := status.Code(err)
				if grpcCode == codes.Unavailable {
					r.logger.Warnf("Server is unavailable. Stop trying to report metrics. Run ID=%s.", r.runID)
					r.errorCount++
					r.cancelFn()
					continue
				}
				r.logger.Warnf("Failed to report metrics: %v. Run ID=%s.", err, r.runID)
				r.errorCount++
				continue
			}

			r.successCount++
		}
	}
}

func (r *MetricsReporter) collectMetrics() (*Metrics, error) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	processCPU, err := r.process.CPUPercentWithContext(r.ctx)
	if err != nil {
		return nil, err
	}
	systemCPU, err := cpu.PercentWithContext(r.ctx, 0, false)
	if err != nil {
		return nil, err
	}
	machineThreadCount, err := r.process.NumThreadsWithContext(r.ctx)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		MemHeapUsedMB:      memStats.HeapInuse / 1e6,
		ProcessCPU:         processCPU,
		SystemCPU:          systemCPU[0],
		MachineThreadCount: machineThreadCount,
		GoroutineCount:     int32(runtime.NumGoroutine()),
	}, nil
}
