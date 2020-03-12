package gocb

import (
	"flag"
	"os"
	"strings"
	"testing"
)

var globalConfig testConfig

type testConfig struct {
	Server       string
	User         string
	Password     string
	Bucket       string
	Version      string
	Collection   string
	FeatureFlags []TestFeatureFlag
}

func TestMain(m *testing.M) {
	server := envFlagString("GOCBSERVER", "server", "",
		"The connection string to connect to for a real server")
	user := envFlagString("GOCBUSER", "user", "",
		"The username to use to authenticate when using a real server")
	password := envFlagString("GOCBPASS", "pass", "",
		"The password to use to authenticate when using a real server")
	bucketName := envFlagString("GOCBBUCKET", "bucket", "default",
		"The bucket to use to test against")
	version := envFlagString("GOCBVER", "version", "",
		"The server version being tested against (major.minor.patch.build_edition)")
	collectionName := envFlagString("GOCBCOLL", "collection-name", "",
		"The name of the collection to use")
	featuresToTest := envFlagString("GOCBFEAT", "features", "",
		"The features that should be tested, applicable only for integration test runs")
	disableLogger := envFlagBool("GOCBNOLOG", "disable-logger", false,
		"Whether to disable the logger")
	flag.Parse()

	if testing.Short() {
		mustBeNil := func(val interface{}, name string) {
			flag.Visit(func(f *flag.Flag) {
				if f.Name == name {
					panic(name + " cannot be used in short mode")
				}
			})
		}
		mustBeNil(server, "server")
		mustBeNil(user, "user")
		mustBeNil(password, "pass")
		mustBeNil(bucketName, "bucket")
		mustBeNil(version, "version")
		mustBeNil(collectionName, "collection-name")
	}

	var featureFlags []TestFeatureFlag
	featureFlagStrs := strings.Split(*featuresToTest, ",")
	for _, featureFlagStr := range featureFlagStrs {
		if len(featureFlagStr) == 0 {
			continue
		}

		if featureFlagStr[0] == '+' {
			featureFlags = append(featureFlags, TestFeatureFlag{
				Enabled: true,
				Feature: FeatureCode(featureFlagStr[1:]),
			})
			continue
		} else if featureFlagStr[0] == '-' {
			featureFlags = append(featureFlags, TestFeatureFlag{
				Enabled: false,
				Feature: FeatureCode(featureFlagStr[1:]),
			})
			continue
		}

		panic("failed to parse specified feature codes")
	}

	if !*disableLogger {
		SetLogger(VerboseStdioLogger())
	}

	globalConfig.Server = *server
	globalConfig.User = *user
	globalConfig.Password = *password
	globalConfig.Bucket = *bucketName
	globalConfig.Version = *version
	globalConfig.Collection = *collectionName
	globalConfig.FeatureFlags = featureFlags

	os.Exit(m.Run())
}

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
}

func envFlagBool(envName, name string, value bool, usage string) *bool {
	envValue := os.Getenv(envName)
	if envValue != "" {
		if envValue == "0" {
			value = false
		} else if strings.ToLower(envValue) == "false" {
			value = false
		} else {
			value = true
		}
	}
	return flag.Bool(name, value, usage)
}
