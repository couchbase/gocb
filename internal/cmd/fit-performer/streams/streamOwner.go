package streams

import (
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"

	"github.com/sirupsen/logrus"
)

type StreamSender struct {
	Stream Stream
	Sender sender.ResultSender
}

type StreamOwner struct {
	streams map[string]*StreamSender
	logger  *logrus.Logger
}

func NewStreamOwner(logger *logrus.Logger) *StreamOwner {
	return &StreamOwner{
		streams: make(map[string]*StreamSender),
		logger:  logger,
	}
}

func (so *StreamOwner) Add(streamID string, stream *StreamSender) {
	so.streams[streamID] = stream
}

func (so *StreamOwner) Get(streamID string) *StreamSender {
	if stream, ok := so.streams[streamID]; ok {
		return stream
	}

	return nil
}

func (so *StreamOwner) WaitForCompletion(runID string) {
	// This may seem odd but it improves debuggability.
	streams := make(map[string]*StreamSender)
	for streamID, stream := range so.streams {
		if stream.Stream.RunID() == runID {
			streams[streamID] = stream
		}
	}

	so.logger.Logf(logrus.InfoLevel, "Waiting for %d streams to complete", len(streams))

	for streamID, stream := range streams {
		<-stream.Stream.Completed()
		delete(so.streams, streamID)
	}
}
