package gocb

import "time"

var defaultConfig = ClusterOptions{
	TimeoutsConfig: TimeoutsConfig{
		KVTimeout:         2500 * time.Millisecond,
		ConnectTimeout:    10 * time.Second,
		KVDurableTimeout:  10 * time.Second,
		ViewTimeout:       75 * time.Second,
		AnalyticsTimeout:  75 * time.Second,
		SearchTimeout:     75 * time.Second,
		ManagementTimeout: 75 * time.Second,
	},
	Transcoder:    NewJSONTranscoder(),
	Tracer:        NewThresholdLoggingTracer(nil),
	Meter:         newAggregatingMeter(nil),
	RetryStrategy: NewBestEffortRetryStrategy(nil),
}

func (suite *UnitTestSuite) TestDevelopmentConfigProfile() {
	auth := PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}

	options := defaultConfig
	options.Authenticator = auth
	err := options.ApplyProfile(ClusterConfigProfileWanDevelopment)
	suite.Require().Nil(err)

	suite.Assert().Equal(20*time.Second, options.TimeoutsConfig.KVTimeout)
	suite.Assert().Equal(20*time.Second, options.TimeoutsConfig.ConnectTimeout)
	suite.Assert().Equal(20*time.Second, options.TimeoutsConfig.KVDurableTimeout)
	suite.Assert().Equal(120*time.Second, options.TimeoutsConfig.ViewTimeout)
	suite.Assert().Equal(120*time.Second, options.TimeoutsConfig.AnalyticsTimeout)
	suite.Assert().Equal(120*time.Second, options.TimeoutsConfig.SearchTimeout)
	suite.Assert().Equal(120*time.Second, options.TimeoutsConfig.ManagementTimeout)
}

func (suite *UnitTestSuite) TestUnknownConfigProfile() {
	auth := PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}

	options := defaultConfig
	options.Authenticator = auth
	err := options.ApplyProfile("unknown")
	suite.Require().ErrorIs(err, ErrInvalidArgument)
}
