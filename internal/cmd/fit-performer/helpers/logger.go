package helpers

import (
	"github.com/couchbase/gocb/v2"
	"github.com/sirupsen/logrus"
)

type GocbLogger struct {
	logger *logrus.Logger
}

func NewLogger(logger *logrus.Logger) *GocbLogger {
	return &GocbLogger{
		logger: logger,
	}
}

func (logger *GocbLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	// We need to do some conversion between gocb and logrus levels as they don't match up.
	var logrusLevel logrus.Level
	switch level {
	case gocb.LogError:
		logrusLevel = logrus.ErrorLevel
	case gocb.LogWarn:
		logrusLevel = logrus.WarnLevel
	case gocb.LogInfo:
		logrusLevel = logrus.InfoLevel
	case gocb.LogDebug:
		logrusLevel = logrus.DebugLevel
	case gocb.LogTrace:
		logrusLevel = logrus.TraceLevel
	case gocb.LogSched:
		logrusLevel = logrus.TraceLevel
	case gocb.LogMaxVerbosity:
		logrusLevel = logrus.TraceLevel
	}
	// Send the data to the logrus Logf function to make sure that it gets formatted correctly.
	logger.logger.Logf(logrusLevel, format, v...)
	return nil
}
