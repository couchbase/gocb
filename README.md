[![GoDoc](https://godoc.org/github.com/couchbaselabs/gocb?status.png)](https://godoc.org/github.com/couchbaselabs/gocb)

# Couchbase Go Client

This is the official Couchbase Go SDK.  If you are looking for our
previous unofficial prototype Go client library, please see:
[http://www.github.com/couchbaselabs/go-couchbase](go-couchbase).

The Go SDK library allows you to connect to a Couchbase cluster from
Go. It is written in pure Go, and uses the included gocbcore library to
handle communicating to the cluster over the Couchbase binary
protocol.

Note that this library is still in pre-release development and is not
yet ready to perform under the stress of a production environment.

Bug Tracker - [http://www.couchbase.com/issues/browse/GOCBC](http://www.couchbase.com/issues/browse/GOCBC)


## Installing

To install the latest development version, run:
```bash
go get gopkg.in/couchbaselabs/gocb.v0
```


## License
Copyright 2015 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See
[LICENSE](https://github.com/couchbase/couchnode/blob/master/LICENSE)
for further details.
