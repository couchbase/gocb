package executor

import (
	"fmt"

	"github.com/couchbase/gocb/v2"
	protoEventing "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/cluster/eventingfunctionmanager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func eventingFunctionResult(function *gocb.EventingFunction) (*protoEventing.Result, error) {
	protoFunction, err := eventingFunctionToProto(*function)
	if err != nil {
		return nil, err
	}
	return &protoEventing.Result{Result: &protoEventing.Result_EventingFunction{EventingFunction: protoFunction}}, nil
}

func eventingFunctionListResult(functions []gocb.EventingFunction) (*protoEventing.Result, error) {
	protoFunctions := make([]*protoEventing.EventingFunction, 0, len(functions))
	for _, function := range functions {
		protoFunction, err := eventingFunctionToProto(function)
		if err != nil {
			return nil, err
		}
		protoFunctions = append(protoFunctions, protoFunction)
	}
	return &protoEventing.Result{
		Result: &protoEventing.Result_EventingFunctionList{
			EventingFunctionList: &protoEventing.EventingFunctionList{Functions: protoFunctions},
		},
	}, nil
}

func eventingStatusResult(status *gocb.EventingStatus) (*protoEventing.Result, error) {
	protoStates := make([]*protoEventing.EventingFunctionState, 0, len(status.Functions))
	for _, state := range status.Functions {
		protoStates = append(protoStates, &protoEventing.EventingFunctionState{
			Name:                  state.Name,
			Status:                eventingFunctionStatusToProto(state.Status),
			NumBootstrappingNodes: int32(state.NumBootstrappingNodes),
			NumDeployedNodes:      int32(state.NumDeployedNodes),
			DeploymentStatus:      eventingDeploymentStatusToProto(state.DeploymentStatus),
			ProcessingStatus:      eventingProcessingStatusToProto(state.ProcessingStatus),
		})
	}
	return &protoEventing.Result{
		Result: &protoEventing.Result_EventingStatus{
			EventingStatus: &protoEventing.EventingStatus{
				Functions:        protoStates,
				NumEventingNodes: int32(status.NumEventingNodes),
			},
		},
	}, nil
}

// Proto -> SDK

func eventingFunctionFromProto(f *protoEventing.EventingFunction) (gocb.EventingFunction, error) {
	if f == nil {
		return gocb.EventingFunction{}, status.Error(codes.InvalidArgument, "expected an eventing function to be provided")
	}

	function := gocb.EventingFunction{
		Name:               f.GetName(),
		Code:               f.GetCode(),
		Version:            f.GetVersion(),
		EnforceSchema:      f.GetEnforceSchema(),
		HandlerUUID:        int(f.GetHandlerUuid()),
		FunctionInstanceID: f.GetFunctionInstanceId(),
		MetadataKeyspace:   eventingKeyspaceFromProto(f.GetMetadataKeyspace()),
		SourceKeyspace:     eventingKeyspaceFromProto(f.GetSourceKeyspace()),
	}

	if f.Settings != nil {
		settings, err := eventingSettingsFromProto(f.GetSettings())
		if err != nil {
			return gocb.EventingFunction{}, err
		}
		function.Settings = settings
	}

	for _, b := range f.GetBucketBindings() {
		access, err := eventingBucketAccessFromProto(b.GetAccess())
		if err != nil {
			return gocb.EventingFunction{}, err
		}
		function.BucketBindings = append(function.BucketBindings, gocb.EventingFunctionBucketBinding{
			Alias:  b.GetAlias(),
			Name:   eventingKeyspaceFromProto(b.GetName()),
			Access: access,
		})
	}

	for _, b := range f.GetUrlBindings() {
		auth, err := eventingURLAuthFromProto(b.GetAuth())
		if err != nil {
			return gocb.EventingFunction{}, err
		}
		function.UrlBindings = append(function.UrlBindings, gocb.EventingFunctionUrlBinding{
			Hostname:               b.GetHostname(),
			Alias:                  b.GetAlias(),
			Auth:                   auth,
			AllowCookies:           b.GetAllowCookies(),
			ValidateSSLCertificate: b.GetValidateSslCertificate(),
		})
	}

	for _, b := range f.GetConstantBindings() {
		function.ConstantBindings = append(function.ConstantBindings, gocb.EventingFunctionConstantBinding{
			Alias:   b.GetAlias(),
			Literal: b.GetLiteral(),
		})
	}

	return function, nil
}

func eventingKeyspaceFromProto(k *protoEventing.EventingFunctionKeyspace) gocb.EventingFunctionKeyspace {
	return gocb.EventingFunctionKeyspace{
		Bucket:     k.GetBucket(),
		Scope:      k.GetScope(),
		Collection: k.GetCollection(),
	}
}

func eventingURLAuthFromProto(a *protoEventing.EventingFunctionUrlAuth) (gocb.EventingFunctionUrlAuth, error) {
	switch auth := a.GetAuth().(type) {
	case nil, *protoEventing.EventingFunctionUrlAuth_NoAuth:
		return gocb.EventingFunctionUrlNoAuth{}, nil
	case *protoEventing.EventingFunctionUrlAuth_BasicAuth:
		return gocb.EventingFunctionUrlAuthBasic{
			User: auth.BasicAuth.GetUsername(),
			Pass: auth.BasicAuth.GetPassword(),
		}, nil
	case *protoEventing.EventingFunctionUrlAuth_DigestAuth:
		return gocb.EventingFunctionUrlAuthDigest{
			User: auth.DigestAuth.GetUsername(),
			Pass: auth.DigestAuth.GetPassword(),
		}, nil
	case *protoEventing.EventingFunctionUrlAuth_BearerAuth:
		return gocb.EventingFunctionUrlAuthBearer{BearerKey: auth.BearerAuth.GetKey()}, nil
	default:
		return nil, fmt.Errorf("unknown eventing url auth: %T", auth)
	}
}

func eventingSettingsFromProto(s *protoEventing.EventingFunctionSettings) (gocb.EventingFunctionSettings, error) {
	settings := gocb.EventingFunctionSettings{
		CPPWorkerThreadCount:   int(s.GetCppWorkerThreadCount()),
		Description:            s.GetDescription(),
		ExecutionTimeout:       s.GetExecutionTimeout().AsDuration(),
		LCBInstCapacity:        int(s.GetLcbInstCapacity()),
		LCBRetryCount:          int(s.GetLcbRetryCount()),
		LCBTimeout:             s.GetLcbTimeout().AsDuration(),
		NumTimerPartitions:     int(s.GetNumTimerPartitions()),
		SockBatchSize:          int(s.GetSockBatchSize()),
		TickDuration:           s.GetTickDuration().AsDuration(),
		TimerContextSize:       int(s.GetTimerContextSize()),
		UserPrefix:             s.GetUserPrefix(),
		BucketCacheSize:        int(s.GetBucketCacheSize()),
		BucketCacheAge:         int(s.GetBucketCacheAge()),
		CurlMaxAllowedRespSize: int(s.GetCurlMaxAllowedRespSize()),
		QueryPrepareAll:        s.GetQueryPrepareAll(),
		WorkerCount:            int(s.GetWorkerCount()),
		HandlerHeaders:         s.GetHandlerHeaders(),
		HandlerFooters:         s.GetHandlerFooters(),
		EnableAppLogRotation:   s.GetEnableAppLogRotation(),
		AppLogDir:              s.GetAppLogDir(),
		AppLogMaxSize:          int(s.GetAppLogMaxSize()),
		AppLogMaxFiles:         int(s.GetAppLogMaxFiles()),
		CheckpointInterval:     s.GetCheckpointInterval().AsDuration(),
	}

	if s.DcpStreamBoundary != nil {
		switch s.GetDcpStreamBoundary() {
		case protoEventing.EventingFunctionDcpBoundary_EVERYTHING:
			settings.DCPStreamBoundary = gocb.EventingFunctionDCPBoundaryEverything
		case protoEventing.EventingFunctionDcpBoundary_FROM_NOW:
			settings.DCPStreamBoundary = gocb.EventingFunctionDCPBoundaryFromNow
		default:
			return settings, fmt.Errorf("unknown dcp stream boundary: %v", s.GetDcpStreamBoundary())
		}
	}

	if s.DeploymentStatus != nil {
		switch s.GetDeploymentStatus() {
		case protoEventing.EventingFunctionDeploymentStatus_DEPLOYMENT_STATUS_DEPLOYED:
			settings.DeploymentStatus = gocb.EventingFunctionDeploymentStatusDeployed
		case protoEventing.EventingFunctionDeploymentStatus_DEPLOYMENT_STATUS_UNDEPLOYED:
			settings.DeploymentStatus = gocb.EventingFunctionDeploymentStatusUndeployed
		default:
			return settings, fmt.Errorf("unknown deployment status: %v", s.GetDeploymentStatus())
		}
	}

	if s.ProcessingStatus != nil {
		switch s.GetProcessingStatus() {
		case protoEventing.EventingFunctionProcessingStatus_PROCESSING_STATUS_RUNNING:
			settings.ProcessingStatus = gocb.EventingFunctionProcessingStatusRunning
		case protoEventing.EventingFunctionProcessingStatus_PROCESSING_STATUS_PAUSED:
			settings.ProcessingStatus = gocb.EventingFunctionProcessingStatusPaused
		default:
			return settings, fmt.Errorf("unknown processing status: %v", s.GetProcessingStatus())
		}
	}

	if s.LanguageCompatibility != nil {
		switch s.GetLanguageCompatibility() {
		case protoEventing.EventingFunctionLanguageCompatibility_VERSION_6_0_0:
			settings.LanguageCompatibility = gocb.EventingFunctionLanguageCompatibilityVersion600
		case protoEventing.EventingFunctionLanguageCompatibility_VERSION_6_5_0:
			settings.LanguageCompatibility = gocb.EventingFunctionLanguageCompatibilityVersion650
		case protoEventing.EventingFunctionLanguageCompatibility_VERSION_6_6_2:
			settings.LanguageCompatibility = gocb.EventingFunctionLanguageCompatibilityVersion662
		case protoEventing.EventingFunctionLanguageCompatibility_VERSION_7_2_0:
			settings.LanguageCompatibility = gocb.EventingFunctionLanguageCompatibilityVersion720
		default:
			return settings, fmt.Errorf("unknown language compatibility: %v", s.GetLanguageCompatibility())
		}
	}

	if s.LogLevel != nil {
		switch s.GetLogLevel() {
		case protoEventing.EventingFunctionLogLevel_INFO:
			settings.LogLevel = gocb.EventingFunctionLogLevelInfo
		case protoEventing.EventingFunctionLogLevel_ERROR:
			settings.LogLevel = gocb.EventingFunctionLogLevelError
		case protoEventing.EventingFunctionLogLevel_WARNING:
			settings.LogLevel = gocb.EventingFunctionLogLevelWarning
		case protoEventing.EventingFunctionLogLevel_DEBUG:
			settings.LogLevel = gocb.EventingFunctionLogLevelDebug
		case protoEventing.EventingFunctionLogLevel_TRACE:
			settings.LogLevel = gocb.EventingFunctionLogLevelTrace
		default:
			return settings, fmt.Errorf("unknown log level: %v", s.GetLogLevel())
		}
	}

	if s.QueryConsistency != nil {
		switch s.GetQueryConsistency() {
		case shared.ScanConsistency_NOT_BOUNDED:
			settings.QueryConsistency = gocb.QueryScanConsistencyNotBounded
		case shared.ScanConsistency_REQUEST_PLUS:
			settings.QueryConsistency = gocb.QueryScanConsistencyRequestPlus
		default:
			return settings, fmt.Errorf("unknown query consistency: %v", s.GetQueryConsistency())
		}
	}

	return settings, nil
}

func eventingBucketAccessFromProto(a protoEventing.EventingFunctionBucketAccess) (gocb.EventingFunctionBucketAccess, error) {
	switch a {
	case protoEventing.EventingFunctionBucketAccess_READ_ONLY:
		return gocb.EventingFunctionBucketAccessReadOnly, nil
	case protoEventing.EventingFunctionBucketAccess_READ_WRITE:
		return gocb.EventingFunctionBucketAccessReadWrite, nil
	default:
		return "", fmt.Errorf("unknown bucket access: %v", a)
	}
}

// SDK -> Proto

func eventingFunctionToProto(f gocb.EventingFunction) (*protoEventing.EventingFunction, error) {
	settings, err := eventingSettingsToProto(f.Settings)
	if err != nil {
		return nil, err
	}

	protoFunction := &protoEventing.EventingFunction{
		Name:               f.Name,
		Code:               f.Code,
		Version:            proto.String(f.Version),
		EnforceSchema:      proto.Bool(f.EnforceSchema),
		HandlerUuid:        proto.Int64(int64(f.HandlerUUID)),
		FunctionInstanceId: proto.String(f.FunctionInstanceID),
		MetadataKeyspace:   eventingKeyspaceToProto(f.MetadataKeyspace),
		SourceKeyspace:     eventingKeyspaceToProto(f.SourceKeyspace),
		Settings:           settings,
	}

	for _, b := range f.BucketBindings {
		access, err := eventingBucketAccessToProto(b.Access)
		if err != nil {
			return nil, err
		}
		protoFunction.BucketBindings = append(protoFunction.BucketBindings, &protoEventing.EventingFunctionBucketBinding{
			Alias:  b.Alias,
			Name:   eventingKeyspaceToProto(b.Name),
			Access: access,
		})
	}

	for _, b := range f.UrlBindings {
		auth, err := eventingURLAuthToProto(b.Auth)
		if err != nil {
			return nil, err
		}
		protoFunction.UrlBindings = append(protoFunction.UrlBindings, &protoEventing.EventingFunctionUrlBinding{
			Alias:                  b.Alias,
			AllowCookies:           b.AllowCookies,
			Auth:                   auth,
			Hostname:               b.Hostname,
			ValidateSslCertificate: b.ValidateSSLCertificate,
		})
	}

	for _, b := range f.ConstantBindings {
		protoFunction.ConstantBindings = append(protoFunction.ConstantBindings, &protoEventing.EventingFunctionConstantBinding{
			Alias:   b.Alias,
			Literal: b.Literal,
		})
	}

	return protoFunction, nil
}

func eventingKeyspaceToProto(k gocb.EventingFunctionKeyspace) *protoEventing.EventingFunctionKeyspace {
	keyspace := &protoEventing.EventingFunctionKeyspace{Bucket: k.Bucket}
	if k.Scope != "" {
		keyspace.Scope = proto.String(k.Scope)
	}
	if k.Collection != "" {
		keyspace.Collection = proto.String(k.Collection)
	}
	return keyspace
}

func eventingURLAuthToProto(a gocb.EventingFunctionUrlAuth) (*protoEventing.EventingFunctionUrlAuth, error) {
	switch auth := a.(type) {
	case nil, gocb.EventingFunctionUrlNoAuth:
		return &protoEventing.EventingFunctionUrlAuth{
			Auth: &protoEventing.EventingFunctionUrlAuth_NoAuth{NoAuth: &protoEventing.EventingFunctionUrlNoAuth{}},
		}, nil
	case gocb.EventingFunctionUrlAuthBasic:
		return &protoEventing.EventingFunctionUrlAuth{
			Auth: &protoEventing.EventingFunctionUrlAuth_BasicAuth{
				BasicAuth: &protoEventing.EventingFunctionUrlAuthBasic{Username: auth.User, Password: auth.Pass},
			},
		}, nil
	case gocb.EventingFunctionUrlAuthDigest:
		return &protoEventing.EventingFunctionUrlAuth{
			Auth: &protoEventing.EventingFunctionUrlAuth_DigestAuth{
				DigestAuth: &protoEventing.EventingFunctionUrlAuthDigest{Username: auth.User, Password: auth.Pass},
			},
		}, nil
	case gocb.EventingFunctionUrlAuthBearer:
		return &protoEventing.EventingFunctionUrlAuth{
			Auth: &protoEventing.EventingFunctionUrlAuth_BearerAuth{
				BearerAuth: &protoEventing.EventingFunctionUrlAuthBearer{Key: auth.BearerKey},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown eventing url auth: %T", auth)
	}
}

func eventingSettingsToProto(s gocb.EventingFunctionSettings) (*protoEventing.EventingFunctionSettings, error) {
	settings := &protoEventing.EventingFunctionSettings{
		AppLogDir:              proto.String(s.AppLogDir),
		AppLogMaxFiles:         proto.Int64(int64(s.AppLogMaxFiles)),
		AppLogMaxSize:          proto.Int64(int64(s.AppLogMaxSize)),
		BucketCacheAge:         proto.Int64(int64(s.BucketCacheAge)),
		BucketCacheSize:        proto.Int64(int64(s.BucketCacheSize)),
		CheckpointInterval:     durationpb.New(s.CheckpointInterval),
		CppWorkerThreadCount:   proto.Int64(int64(s.CPPWorkerThreadCount)),
		CurlMaxAllowedRespSize: proto.Int64(int64(s.CurlMaxAllowedRespSize)),
		DeploymentStatus:       eventingDeploymentStatusToProto(s.DeploymentStatus).Enum(),
		Description:            proto.String(s.Description),
		EnableAppLogRotation:   proto.Bool(s.EnableAppLogRotation),
		ExecutionTimeout:       durationpb.New(s.ExecutionTimeout),
		HandlerFooters:         s.HandlerFooters,
		HandlerHeaders:         s.HandlerHeaders,
		LcbInstCapacity:        proto.Int64(int64(s.LCBInstCapacity)),
		LcbRetryCount:          proto.Int64(int64(s.LCBRetryCount)),
		LcbTimeout:             durationpb.New(s.LCBTimeout),
		NumTimerPartitions:     proto.Int64(int64(s.NumTimerPartitions)),
		ProcessingStatus:       eventingProcessingStatusToProto(s.ProcessingStatus).Enum(),
		QueryPrepareAll:        proto.Bool(s.QueryPrepareAll),
		SockBatchSize:          proto.Int64(int64(s.SockBatchSize)),
		TickDuration:           durationpb.New(s.TickDuration),
		TimerContextSize:       proto.Int64(int64(s.TimerContextSize)),
		UserPrefix:             proto.String(s.UserPrefix),
		WorkerCount:            proto.Int64(int64(s.WorkerCount)),
	}

	switch s.DCPStreamBoundary {
	case "":
	case gocb.EventingFunctionDCPBoundaryEverything:
		settings.DcpStreamBoundary = protoEventing.EventingFunctionDcpBoundary_EVERYTHING.Enum()
	case gocb.EventingFunctionDCPBoundaryFromNow:
		settings.DcpStreamBoundary = protoEventing.EventingFunctionDcpBoundary_FROM_NOW.Enum()
	default:
		return nil, fmt.Errorf("unknown dcp stream boundary: %s", s.DCPStreamBoundary)
	}

	switch s.LanguageCompatibility {
	case "":
	case gocb.EventingFunctionLanguageCompatibilityVersion600:
		settings.LanguageCompatibility = protoEventing.EventingFunctionLanguageCompatibility_VERSION_6_0_0.Enum()
	case gocb.EventingFunctionLanguageCompatibilityVersion650:
		settings.LanguageCompatibility = protoEventing.EventingFunctionLanguageCompatibility_VERSION_6_5_0.Enum()
	case gocb.EventingFunctionLanguageCompatibilityVersion662:
		settings.LanguageCompatibility = protoEventing.EventingFunctionLanguageCompatibility_VERSION_6_6_2.Enum()
	case gocb.EventingFunctionLanguageCompatibilityVersion720:
		settings.LanguageCompatibility = protoEventing.EventingFunctionLanguageCompatibility_VERSION_7_2_0.Enum()
	default:
		return nil, fmt.Errorf("unknown language compatibility: %s", s.LanguageCompatibility)
	}

	switch s.LogLevel {
	case "":
	case gocb.EventingFunctionLogLevelInfo:
		settings.LogLevel = protoEventing.EventingFunctionLogLevel_INFO.Enum()
	case gocb.EventingFunctionLogLevelError:
		settings.LogLevel = protoEventing.EventingFunctionLogLevel_ERROR.Enum()
	case gocb.EventingFunctionLogLevelWarning:
		settings.LogLevel = protoEventing.EventingFunctionLogLevel_WARNING.Enum()
	case gocb.EventingFunctionLogLevelDebug:
		settings.LogLevel = protoEventing.EventingFunctionLogLevel_DEBUG.Enum()
	case gocb.EventingFunctionLogLevelTrace:
		settings.LogLevel = protoEventing.EventingFunctionLogLevel_TRACE.Enum()
	default:
		return nil, fmt.Errorf("unknown log level: %s", s.LogLevel)
	}

	switch s.QueryConsistency {
	case 0:
	case gocb.QueryScanConsistencyNotBounded:
		settings.QueryConsistency = shared.ScanConsistency_NOT_BOUNDED.Enum()
	case gocb.QueryScanConsistencyRequestPlus:
		settings.QueryConsistency = shared.ScanConsistency_REQUEST_PLUS.Enum()
	default:
		return nil, fmt.Errorf("unknown query consistency: %v", s.QueryConsistency)
	}

	return settings, nil
}

func eventingBucketAccessToProto(a gocb.EventingFunctionBucketAccess) (protoEventing.EventingFunctionBucketAccess, error) {
	switch a {
	case gocb.EventingFunctionBucketAccessReadOnly:
		return protoEventing.EventingFunctionBucketAccess_READ_ONLY, nil
	case gocb.EventingFunctionBucketAccessReadWrite:
		return protoEventing.EventingFunctionBucketAccess_READ_WRITE, nil
	default:
		return 0, fmt.Errorf("unknown bucket access: %s", a)
	}
}

func eventingDeploymentStatusToProto(s gocb.EventingFunctionDeploymentStatus) protoEventing.EventingFunctionDeploymentStatus {
	if s == gocb.EventingFunctionDeploymentStatusDeployed {
		return protoEventing.EventingFunctionDeploymentStatus_DEPLOYMENT_STATUS_DEPLOYED
	}
	return protoEventing.EventingFunctionDeploymentStatus_DEPLOYMENT_STATUS_UNDEPLOYED
}

func eventingProcessingStatusToProto(s gocb.EventingFunctionProcessingStatus) protoEventing.EventingFunctionProcessingStatus {
	if s == gocb.EventingFunctionProcessingStatusRunning {
		return protoEventing.EventingFunctionProcessingStatus_PROCESSING_STATUS_RUNNING
	}
	return protoEventing.EventingFunctionProcessingStatus_PROCESSING_STATUS_PAUSED
}

func eventingFunctionStatusToProto(s gocb.EventingFunctionStatus) protoEventing.EventingFunctionStatus {
	switch s {
	case gocb.EventingFunctionStateUndeployed:
		return protoEventing.EventingFunctionStatus_UNDEPLOYED
	case gocb.EventingFunctionStateDeploying:
		return protoEventing.EventingFunctionStatus_DEPLOYING
	case gocb.EventingFunctionStateDeployed:
		return protoEventing.EventingFunctionStatus_DEPLOYED
	case gocb.EventingFunctionStateUndeploying:
		return protoEventing.EventingFunctionStatus_UNDEPLOYING
	case gocb.EventingFunctionStatePaused:
		return protoEventing.EventingFunctionStatus_PAUSED
	case gocb.EventingFunctionStatePausing:
		return protoEventing.EventingFunctionStatus_PAUSING
	case gocb.EventingFunctionStateResuming:
		return protoEventing.EventingFunctionStatus_RESUMING
	default:
		return protoEventing.EventingFunctionStatus_UNKNOWN
	}
}
