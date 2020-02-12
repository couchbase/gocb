package gocb

import (
	"flag"
	"os"
	"testing"
)

var globalConfig testConfig

type testConfig struct {
	Server     string
	User       string
	Password   string
	Bucket     string
	Version    string
	Collection string
}

func TestMain(m *testing.M) {
	server := flag.String("server", "", "The connection string to connect to for a real server")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	version := flag.String("version", "", "The server version being tested against (major.minor.patch.build_edition)")
	collectionName := flag.String("collection-name", "", "The name of the collection to use")
	disableLogger := flag.Bool("disable-logger", false, "Whether to disable the logger")
	flag.Parse()

	if testing.Short() {
		mustBeNil := func(val interface{}, name string) {
			flag.Visit(func(f *flag.Flag) {
				if f.Name == name {
					panic(name + " cannot be used in short name")
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

	if !*disableLogger {
		SetLogger(VerboseStdioLogger())
	}

	globalConfig.Server = *server
	globalConfig.User = *user
	globalConfig.Password = *password
	globalConfig.Bucket = *bucketName
	globalConfig.Version = *version
	globalConfig.Collection = *collectionName

	os.Exit(m.Run())
}
