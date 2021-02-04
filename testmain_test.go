package gocb

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

var globalConfig testConfig

type testConfig struct {
	Server       string
	User         string
	Password     string
	Bucket       string
	Version      string
	Collection   string
	Scope        string
	FeatureFlags []TestFeatureFlag

	connstr string
	auth    Authenticator
}

func TestMain(m *testing.M) {
	initialGoroutineCount := runtime.NumGoroutine()

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
	scopeName := envFlagString("GOCBSCOP", "scope-name", "",
		"The name of the scope to use")
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
		mustBeNil(scopeName, "scope-name")
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
	globalConfig.Scope = *scopeName
	globalConfig.FeatureFlags = featureFlags

	result := m.Run()

	// Loop for at most a second checking for goroutines leaks, this gives any HTTP goroutines time to shutdown
	start := time.Now()
	var finalGoroutineCount int
	for time.Now().Sub(start) <= 1*time.Second {
		runtime.Gosched()
		finalGoroutineCount = runtime.NumGoroutine()
		if finalGoroutineCount == initialGoroutineCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if finalGoroutineCount != initialGoroutineCount {
		log.Printf("Detected a goroutine leak (%d before != %d after), failing", initialGoroutineCount, finalGoroutineCount)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		result = 1
	} else {
		log.Printf("No goroutines appear to have leaked (%d before == %d after)", initialGoroutineCount, finalGoroutineCount)
	}

	os.Exit(result)
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
