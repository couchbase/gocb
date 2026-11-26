package gocb

import (
	"bytes"
	"sync"
)

func (suite *UnitTestSuite) TestWarnIfLoggingDisabled() {
	originalLogger := globalLogger
	originalOutput := loggingDisabledWarningOutput
	defer func() {
		globalLogger = originalLogger
		loggingDisabledWarningOutput = originalOutput
		loggingDisabledWarningOnce = sync.Once{}
	}()

	var buf bytes.Buffer
	loggingDisabledWarningOutput = &buf

	suite.Run("logger set", func() {
		buf.Reset()
		loggingDisabledWarningOnce = sync.Once{}
		globalLogger = DefaultStdioLogger()

		warnIfLoggingDisabled()

		suite.Empty(buf.String())
	})

	suite.Run("logger not set", func() {
		buf.Reset()
		loggingDisabledWarningOnce = sync.Once{}
		globalLogger = nil

		warnIfLoggingDisabled()
		warnIfLoggingDisabled()

		suite.Equal(loggingDisabledWarning+"\n", buf.String())
	})
}
