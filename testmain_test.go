package gocb

import (
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	cavescli "github.com/couchbaselabs/gocaves/client"
	"github.com/google/uuid"
)

var globalConfig testConfig

var globalBucket *Bucket
var globalCollection *Collection
var globalScope *Scope
var globalCluster *testCluster
var globalTracer *testTracer
var globalMeter *testMeter

type testConfig struct {
	Server       string
	User         string
	Password     string
	Bucket       string
	Version      string
	Collection   string
	Scope        string
	FeatureFlags []TestFeatureFlag

	connstr   string
	certsPath string
	auth      Authenticator
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
	certsPath := envFlagString("GOCBCERTS", "certs-path", "",
		"The path to the couchbase certs directory")
	enableTxnLoadTests := envFlagBool("GOCBENABLETXNLOAD", "enable-txn-load-tests", false,
		"Whether to enable transaction load tests")
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
		mustBeNil(scopeName, "enable-txn-load-tests")
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

	// These are big tests, don't run unless explicitly enabled.
	if !(*enableTxnLoadTests) {
		featureFlags = append(featureFlags, TestFeatureFlag{
			Enabled: false,
			Feature: TransactionsBulkFeature,
		})
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
	globalConfig.certsPath = *certsPath

	if !testing.Short() {
		setupCluster()
	}

	result := m.Run()

	if globalCluster != nil {
		err := globalCluster.Close(nil)
		if err != nil {
			panic(err)
		}

		if globalCluster.Mock != nil {
			err := globalCluster.Mock.Shutdown()
			if err != nil {
				panic(err)
			}
		}
	}

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

func setupCluster() {
	var err error
	var connStr string
	var mock *cavescli.Client
	var mockID string
	if globalConfig.Server == "" {
		if globalConfig.Version != "" {
			panic("version cannot be specified with mock")
		}

		mock, err = cavescli.NewClient(cavescli.NewClientOptions{
			Version: "v0.0.1-53",
		})
		if err != nil {
			panic(err.Error())
		}

		mockID = uuid.New().String()
		connStr, err = mock.StartTesting(mockID, "gocb-"+Version())
		if err != nil {
			panic(err)
		}

		globalConfig.Bucket = "default"
		globalConfig.Version = "0.0.1-53"
		globalConfig.Server = connStr
		globalConfig.User = "Administrator"
		globalConfig.Password = "password"

		// gocb itself doesn't use the default client but the mock downloader does so let's make sure that it
		// doesn't hold any goroutines open which will affect our goroutine leak detector.
		http.DefaultClient.CloseIdleConnections()
	} else {
		connStr = globalConfig.Server

		if globalConfig.Version == "" {
			globalConfig.Version = defaultServerVersion
		}
	}

	auth := PasswordAuthenticator{
		Username: globalConfig.User,
		Password: globalConfig.Password,
	}

	options := ClusterOptions{Authenticator: auth}

	if globalConfig.certsPath != "" {
		rootCAs := x509.NewCertPool()
		files, err := ioutil.ReadDir(globalConfig.certsPath)
		if err != nil {
			log.Fatal(err)
		}
		for _, f := range files {
			certs, err := ioutil.ReadFile(globalConfig.certsPath + "/" + f.Name())
			if strings.Contains(f.Name(), "roots") {
				if err != nil {
					log.Fatalf("Failed to append %q to RootCAs: %v", f, err)
				}
				if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
					log.Println("No certs appended, using system certs only")
				}
			}
		}
		options.SecurityConfig.TLSRootCAs = rootCAs
		connStr = "couchbases://" + connStr
	}

	globalTracer = newTestTracer()
	globalMeter = newTestMeter()
	options.Tracer = globalTracer
	options.Meter = globalMeter

	cluster, err := Connect(connStr, options)
	if err != nil {
		panic(err.Error())
	}

	globalConfig.connstr = connStr
	globalConfig.auth = auth

	nodeVersion, err := newNodeVersion(globalConfig.Version, mock != nil)
	if err != nil {
		panic(err.Error())
	}

	globalCluster = &testCluster{
		Cluster:      cluster,
		Mock:         mock,
		RunID:        mockID,
		Version:      nodeVersion,
		FeatureFlags: globalConfig.FeatureFlags,
	}

	globalBucket = globalCluster.Bucket(globalConfig.Bucket)

	if globalConfig.Scope != "" {
		globalScope = globalBucket.Scope(globalConfig.Scope)
	} else {
		globalScope = globalBucket.DefaultScope()
	}

	if globalConfig.Collection != "" {
		globalCollection = globalScope.Collection(globalConfig.Collection)
	} else {
		globalCollection = globalScope.Collection("_default")
	}
}
