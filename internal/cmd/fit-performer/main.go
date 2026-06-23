package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const maxMsgSize = 26214400 // 25MiB

func main() {
	port := envFlagInt("TXNPERFORMERPORT", "port", 8060,
		"The port to use")
	version := envFlagString("TXNVERSION", "version", "1.0.0",
		"The version to use")
	logLevel := envFlagString("LOG_LEVEL", "loglevel", "info", "the log level to use")
	flag.Parse()

	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.999999999Z07:00",
	})
	logger.SetOutput(os.Stdout)
	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		log.Printf("Invalid log level: %s, setting to info\n", err)
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}

	performer := NewPerformer(logger, *version)

	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
	}

	grpcSrv := grpc.NewServer(serverOpts...)
	protocol.RegisterPerformerServiceServer(grpcSrv, performer)

	logger.Logf(logrus.InfoLevel, "Starting grpc server at %d", *port)
	logger.Logf(logrus.DebugLevel, "Version: %s", *version)
	err = grpcSrv.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to start grpc server: %v", err)
	}
}

func envFlagString(envName, name string, value string, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
}

func envFlagInt(envName, name string, value int, usage string) *int {
	envValue := os.Getenv(envName)
	if envValue != "" {
		var err error
		value, err = strconv.Atoi(envValue)
		if err != nil {
			panic("failed to parse string as int")
		}
	}
	return flag.Int(name, value, usage)
}
