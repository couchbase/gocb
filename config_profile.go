package gocb

import "time"

var developmentProfile = ClusterOptions{
	TimeoutsConfig: TimeoutsConfig{
		KVTimeout:         20 * time.Second,
		ConnectTimeout:    20 * time.Second,
		KVDurableTimeout:  20 * time.Second,
		KVScanTimeout:     20 * time.Second,
		ViewTimeout:       120 * time.Second,
		AnalyticsTimeout:  120 * time.Second,
		SearchTimeout:     120 * time.Second,
		ManagementTimeout: 120 * time.Second,
		QueryTimeout:      120 * time.Second,
	},
}

// ClusterConfigProfile represents a named profile that can be applied to ClusterOptions.
// VOLATILE: This API is subject to change at any time.
type ClusterConfigProfile string

const (
	// ClusterConfigProfileWanDevelopment represents a wan development profile that can be applied to the ClusterOptions
	// overwriting any properties that exist on the profile.
	// VOLATILE: This API is subject to change at any time.
	ClusterConfigProfileWanDevelopment ClusterConfigProfile = "wan-development"

	// ClusterConfigProfileLocalDevelopment represents a local development profile that can be applied to the ClusterOptions
	// overwriting any properties that exist on the profile.
	ClusterConfigProfileLocalDevelopment ClusterConfigProfile = "local-development"
)

// ApplyProfile will apply a named profile to the ClusterOptions overwriting any properties that
// exist on the profile.
// VOLATILE: This API is subject to change at any time.
func (opts *ClusterOptions) ApplyProfile(profile ClusterConfigProfile) error {
	if profile == ClusterConfigProfileWanDevelopment {
		opts.TimeoutsConfig = developmentProfile.TimeoutsConfig
		return nil
	}

	return makeInvalidArgumentsError("unknown configuration profile")
}

// WithProfile will apply a multiples named profiles to the ClusterOptions overwriting any properties that
// exist on the profile.
func (opts *ClusterOptions) WithProfile(profile ClusterConfigProfile) error {
	if profile == ClusterConfigProfileWanDevelopment {
		opts.TimeoutsConfig = developmentProfile.TimeoutsConfig
		return nil
	}

	if profile == ClusterConfigProfileLocalDevelopment {
		opts.TimeoutsConfig = developmentProfile.TimeoutsConfig
		opts.TransactionsConfig.DurabilityLevel = DurabilityLevelNone
	}

	return makeInvalidArgumentsError("unknown configuration profile")
}
