package main

import (
	protoPerformer "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/performer"
	protoSDK "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
)

func PerformerCaps() []protoPerformer.Caps {
	return []protoPerformer.Caps{
		protoPerformer.Caps_GRPC_TESTING,
		protoPerformer.Caps_KV_SUPPORT_1,
		protoPerformer.Caps_CLUSTER_CONFIG_1,
		protoPerformer.Caps_CLUSTER_CONFIG_CERT,
		protoPerformer.Caps_OBSERVABILITY_1,
		protoPerformer.Caps_CONTENT_AS_PERFORMER_VALIDATION,
		protoPerformer.Caps_TRANSACTIONS_SUPPORT_1,
		protoPerformer.Caps_TRANSACTIONS_WORKLOAD_1,
		protoPerformer.Caps_TXN_CLIENT_CONTEXT_ID_SUPPORT,
	}
}

func SDKCaps() []protoSDK.Caps {
	return []protoSDK.Caps{
		protoSDK.Caps_SDK_QUERY_INDEX_MANAGEMENT,
		protoSDK.Caps_SDK_LOOKUP_IN,
		protoSDK.Caps_SDK_QUERY,
		protoSDK.Caps_SDK_BUCKET_MANAGEMENT,
		protoSDK.Caps_SDK_COLLECTION_MANAGEMENT,
		protoSDK.Caps_SDK_KV,
		protoSDK.Caps_SDK_SEARCH,
		protoSDK.Caps_SDK_SEARCH_INDEX_MANAGEMENT,
		protoSDK.Caps_WAIT_UNTIL_READY,
		protoSDK.Caps_SUPPORTS_AUTHENTICATOR,
		protoSDK.Caps_SDK_COLLECTION_QUERY_INDEX_MANAGEMENT,
		protoSDK.Caps_SDK_KV_RANGE_SCAN,
		protoSDK.Caps_SDK_LOOKUP_IN_REPLICAS,
		protoSDK.Caps_SDK_QUERY_READ_FROM_REPLICA,
		protoSDK.Caps_SDK_MANAGEMENT_HISTORY_RETENTION,
		protoSDK.Caps_SDK_DOCUMENT_NOT_LOCKED,
		protoSDK.Caps_SDK_QUERY_BOTH_POSITIONAL_AND_NAMED_PARAMETERS,
		protoSDK.Caps_SDK_VECTOR_SEARCH,
		protoSDK.Caps_SDK_SCOPE_SEARCH,
		protoSDK.Caps_SDK_SCOPE_SEARCH_INDEX_MANAGEMENT,
		protoSDK.Caps_SDK_SEARCH_RFC_REVISION_11,
		protoSDK.Caps_SDK_INDEX_MANAGEMENT_RFC_REVISION_25,
		protoSDK.Caps_SDK_VECTOR_SEARCH_BASE64,
		protoSDK.Caps_SDK_ZONE_AWARE_READ_FROM_REPLICA,
		protoSDK.Caps_SDK_OBSERVABILITY_CLUSTER_LABELS,
		protoSDK.Caps_SDK_OBSERVABILITY_RFC_REV_24,
		protoSDK.Caps_SDK_APP_TELEMETRY,
		protoSDK.Caps_SDK_BUCKET_SETTINGS_NUM_VBUCKETS,
		protoSDK.Caps_SDK_PREFILTER_VECTOR_SEARCH,
		protoSDK.Caps_SDK_SET_AUTHENTICATOR,
		protoSDK.Caps_SDK_JWT,
		protoSDK.Caps_SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS,
		protoSDK.Caps_SDK_QUERY_2120,
	}
}
