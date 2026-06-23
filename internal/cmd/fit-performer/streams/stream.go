package streams

type StreamItem interface {
	IsStreamItem()
}

type Stream interface {
	Next() StreamItem
	Err() error
	Cancel() error
	RunID() string
	Completed() <-chan struct{}
	// Internally marking the stream as completed is complicated as we don't write
	// directly into the stream from.
	Finish()
}
