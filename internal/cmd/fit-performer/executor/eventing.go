package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	protoEventing "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/eventingfunctionmanager"
	protoScopeEventing "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/scope/eventingfunctionmanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type protoEventingOptions interface {
	GetTimeout() *durationpb.Duration
	GetParentSpanId() string
}

type sdkEventingOptions interface {
	~struct {
		Timeout       time.Duration
		RetryStrategy gocb.RetryStrategy
		ParentSpan    gocb.RequestSpan
		Context       context.Context
	}
}

func (e *Executor) handleClusterLevelEventingManagerCommand(
	cluster *gocb.Cluster,
	op *protoEventing.Command,
	sender sender.ResultSender,
	returnResult bool,
) (bool, error) {
	mgr := cluster.EventingFunctions()
	switch cmd := op.GetCommand().(type) {
	case *protoEventing.Command_UpsertFunction:
		function, err := eventingFunctionFromProto(cmd.UpsertFunction.GetFunction())
		if err != nil {
			return false, err
		}
		return runEventingOp(e, sender, returnResult, cmd.UpsertFunction.GetOptions(), nil,
			func(opts *gocb.UpsertEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.UpsertFunction(function, opts)
			})
	case *protoEventing.Command_DropFunction:
		return runEventingOp(e, sender, returnResult, cmd.DropFunction.GetOptions(), nil,
			func(opts *gocb.DropEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.DropFunction(cmd.DropFunction.GetName(), opts)
			})
	case *protoEventing.Command_DeployFunction:
		return runEventingOp(e, sender, returnResult, cmd.DeployFunction.GetOptions(), nil,
			func(opts *gocb.DeployEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.DeployFunction(cmd.DeployFunction.GetName(), opts)
			})
	case *protoEventing.Command_UndeployFunction:
		return runEventingOp(e, sender, returnResult, cmd.UndeployFunction.GetOptions(), nil,
			func(opts *gocb.UndeployEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.UndeployFunction(cmd.UndeployFunction.GetName(), opts)
			})
	case *protoEventing.Command_PauseFunction:
		return runEventingOp(e, sender, returnResult, cmd.PauseFunction.GetOptions(), nil,
			func(opts *gocb.PauseEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.PauseFunction(cmd.PauseFunction.GetName(), opts)
			})
	case *protoEventing.Command_ResumeFunction:
		return runEventingOp(e, sender, returnResult, cmd.ResumeFunction.GetOptions(), nil,
			func(opts *gocb.ResumeEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.ResumeFunction(cmd.ResumeFunction.GetName(), opts)
			})
	case *protoEventing.Command_GetFunction:
		return runEventingOp(e, sender, returnResult, cmd.GetFunction.GetOptions(), eventingFunctionResult,
			func(opts *gocb.GetEventingFunctionOptions) (*gocb.EventingFunction, error) {
				return mgr.GetFunction(cmd.GetFunction.GetName(), opts)
			})
	case *protoEventing.Command_GetAllFunctions:
		return runEventingOp(e, sender, returnResult, cmd.GetAllFunctions.GetOptions(), eventingFunctionListResult,
			func(opts *gocb.GetAllEventingFunctionsOptions) ([]gocb.EventingFunction, error) {
				return mgr.GetAllFunctions(opts)
			})
	case *protoEventing.Command_FunctionsStatus:
		return runEventingOp(e, sender, returnResult, cmd.FunctionsStatus.GetOptions(), eventingStatusResult,
			func(opts *gocb.EventingFunctionsStatusOptions) (*gocb.EventingStatus, error) {
				return mgr.FunctionsStatus(opts)
			})
	default:
		return false, fmt.Errorf("unknown eventing function manager command: %T", op.GetCommand())
	}
}

func (e *Executor) handleScopeLevelEventingManagerCommand(
	scope *gocb.Scope,
	op *protoScopeEventing.Command,
	sender sender.ResultSender,
	returnResult bool,
) (bool, error) {
	mgr := scope.EventingFunctions()
	switch cmd := op.GetCommand().(type) {
	case *protoScopeEventing.Command_UpsertFunction:
		function, err := eventingFunctionFromProto(cmd.UpsertFunction.GetFunction())
		if err != nil {
			return false, err
		}
		return runEventingOp(e, sender, returnResult, cmd.UpsertFunction.GetOptions(), nil,
			func(opts *gocb.UpsertEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.UpsertFunction(function, opts)
			})
	case *protoScopeEventing.Command_DropFunction:
		return runEventingOp(e, sender, returnResult, cmd.DropFunction.GetOptions(), nil,
			func(opts *gocb.DropEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.DropFunction(cmd.DropFunction.GetName(), opts)
			})
	case *protoScopeEventing.Command_DeployFunction:
		return runEventingOp(e, sender, returnResult, cmd.DeployFunction.GetOptions(), nil,
			func(opts *gocb.DeployEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.DeployFunction(cmd.DeployFunction.GetName(), opts)
			})
	case *protoScopeEventing.Command_UndeployFunction:
		return runEventingOp(e, sender, returnResult, cmd.UndeployFunction.GetOptions(), nil,
			func(opts *gocb.UndeployEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.UndeployFunction(cmd.UndeployFunction.GetName(), opts)
			})
	case *protoScopeEventing.Command_PauseFunction:
		return runEventingOp(e, sender, returnResult, cmd.PauseFunction.GetOptions(), nil,
			func(opts *gocb.PauseEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.PauseFunction(cmd.PauseFunction.GetName(), opts)
			})
	case *protoScopeEventing.Command_ResumeFunction:
		return runEventingOp(e, sender, returnResult, cmd.ResumeFunction.GetOptions(), nil,
			func(opts *gocb.ResumeEventingFunctionOptions) (struct{}, error) {
				return struct{}{}, mgr.ResumeFunction(cmd.ResumeFunction.GetName(), opts)
			})
	case *protoScopeEventing.Command_GetFunction:
		return runEventingOp(e, sender, returnResult, cmd.GetFunction.GetOptions(), eventingFunctionResult,
			func(opts *gocb.GetEventingFunctionOptions) (*gocb.EventingFunction, error) {
				return mgr.GetFunction(cmd.GetFunction.GetName(), opts)
			})
	case *protoScopeEventing.Command_GetAllFunctions:
		return runEventingOp(e, sender, returnResult, cmd.GetAllFunctions.GetOptions(), eventingFunctionListResult,
			func(opts *gocb.GetAllEventingFunctionsOptions) ([]gocb.EventingFunction, error) {
				return mgr.GetAllFunctions(opts)
			})
	case *protoScopeEventing.Command_FunctionsStatus:
		return runEventingOp(e, sender, returnResult, cmd.FunctionsStatus.GetOptions(), eventingStatusResult,
			func(opts *gocb.EventingFunctionsStatusOptions) (*gocb.EventingStatus, error) {
				return mgr.FunctionsStatus(opts)
			})
	default:
		return false, fmt.Errorf("unknown scope eventing function manager command: %T", op.GetCommand())
	}
}

func runEventingOp[O sdkEventingOptions, R any](
	e *Executor,
	sender sender.ResultSender,
	returnResult bool,
	protoOpts protoEventingOptions,
	toProto func(R) (*protoEventing.Result, error),
	call func(*O) (R, error),
) (bool, error) {
	opts, err := createEventingOptions[O](e, protoOpts)
	if err != nil {
		return false, err
	}

	result := &run.Result{
		Initiated: timestamppb.Now(),
	}

	start := time.Now()
	res, err := call(opts)
	result.ElapsedNanos = time.Since(start).Nanoseconds()
	if err != nil {
		e.sendSDKError(err, sender, start)
		return false, nil
	}

	if returnResult && toProto != nil {
		protoRes, err := toProto(res)
		if err != nil {
			return false, err
		}
		result.Result = &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_EventingFunctionManagerResult{EventingFunctionManagerResult: protoRes},
			},
		}
	} else {
		result.Result = e.makeSuccessResult()
	}

	sender.Send(result)
	return true, nil
}

func createEventingOptions[T sdkEventingOptions](e *Executor, protoOpts protoEventingOptions) (*T, error) {
	var parentSpan gocb.RequestSpan
	if id := protoOpts.GetParentSpanId(); id != "" {
		span, ok := e.spanOwner.GetSpan(id)
		if !ok {
			return nil, fmt.Errorf("unknown parent span %s", id)
		}
		parentSpan = span
	}
	return &T{
		Timeout:    protoOpts.GetTimeout().AsDuration(),
		ParentSpan: parentSpan,
	}, nil
}
