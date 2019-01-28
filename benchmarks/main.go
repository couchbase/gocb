package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocb"
)

type worker interface {
	Start()
	Stop()
	Ops() int
	Errors() int
}

type getWorker struct {
	col   *gocb.Collection
	key   string
	value interface{}
	delay time.Duration

	errors int
	ops    int
	stop   chan bool
}

func (w *getWorker) Start() {
	for {
		select {
		case <-w.stop:
			return
		default:
			w.ops++
			_, err := w.col.Get(w.key, nil)
			if err != nil {
				w.errors++
			}
		}

		if w.delay > 0 {
			time.Sleep(w.delay)
		}
	}
}

func (w *getWorker) Stop() {
	w.stop <- true
}

func (w *getWorker) Ops() int {
	return w.ops
}

func (w *getWorker) Errors() int {
	return w.errors
}

type upsertWorker struct {
	col   *gocb.Collection
	key   string
	value interface{}
	delay time.Duration

	upsertErrors int
	upsertOps    int
	stop         chan bool
}

func (w *upsertWorker) Start() {
	for {
		select {
		case <-w.stop:
			return
		default:
			w.upsertOps++
			_, err := w.col.Upsert(w.key, w.value, nil)
			if err != nil {
				w.upsertErrors++
			}
		}

		if w.delay > 0 {
			time.Sleep(w.delay)
		}
	}
}

func (w *upsertWorker) Stop() {
	w.stop <- true
}

func (w *upsertWorker) Ops() int {
	return w.upsertOps
}

func (w *upsertWorker) Errors() int {
	return w.upsertErrors
}

func main() {
	connStr := flag.String("connStr", "", "The connection string to connect to for a real server")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	duration := flag.Int("duration", 30, "The duration to run tests for (seconds)")
	setThreads := flag.Int("setThreads", 5, "The number of set 'threads' to use")
	getThreads := flag.Int("getThreads", 5, "The number of get 'threads' to use")
	delay := flag.Int("delay", 0, "The delay to leave between operations (milliseconds)")
	keySize := flag.Int("keySize", 12, "The key size to use (bytes)")
	valueSize := flag.Int("valueSize", 128, "The key size to use (bytes)")

	flag.Parse()

	cluster, err := gocb.NewCluster(*connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: *user,
			Password: *password,
		},
	})
	if err != nil {
		panic(err)
	}

	bucket, err := cluster.Bucket(*bucketName, nil)
	if err != nil {
		panic(err)
	}

	collection, err := bucket.DefaultCollection(nil)
	if err != nil {
		panic(err)
	}

	var key string
	for i := 0; i < *keySize; i++ {
		key += "K"
	}

	var val string
	for i := 0; i < *valueSize; i++ {
		val += "V"
	}

	start := time.Now()
	d := *delay
	var closeWg sync.WaitGroup
	closeWg.Add(*getThreads + *setThreads)
	var workers []worker
	for i := 0; i < *setThreads; i++ {
		w := upsertWorker{
			col:   collection,
			key:   key,
			value: val,
			delay: time.Duration(d) * time.Millisecond,
			stop:  make(chan bool),
		}
		workers = append(workers, &w)
		go w.Start()
	}
	for i := 0; i < *getThreads; i++ {
		w := getWorker{
			col:   collection,
			key:   key,
			value: val,
			delay: time.Duration(d) * time.Millisecond,
			stop:  make(chan bool),
		}
		workers = append(workers, &w)
		go w.Start()
	}

	<-time.After(time.Duration(*duration) * time.Second)

	for _, worker := range workers {
		worker.Stop()
	}

	ended := time.Now()

	var ops int
	var errs int
	for _, worker := range workers {
		ops += worker.Ops()
		errs += worker.Errors()
	}

	dur := ended.Sub(start).Seconds()

	fmt.Println("Ran for: ", dur)
	fmt.Println("Total ops: ", ops)
	fmt.Println("Total errors: ", errs)
	fmt.Printf("ops/second: %f", float64(ops)/dur)
}
