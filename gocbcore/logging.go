package gocbcore

import (
	"fmt"
)

type Logger interface {
	Output(s string) error
}

type defaultLogger struct {
}

func (l *defaultLogger) Output(s string) error {
	_, err := fmt.Println(s)
	return err
}

var (
	globalDefaultLogger defaultLogger
	globalLogger        Logger
)

func DefaultStdOutLogger() Logger {
	return &globalDefaultLogger
}

func SetLogger(logger Logger) {
	globalLogger = logger
}

func logDebugf(format string, v ...interface{}) {
	if globalLogger != nil {
		globalLogger.Output(fmt.Sprintf("DEBUG: "+format, v...))
	}
}
