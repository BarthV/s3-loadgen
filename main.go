package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/minio/minio-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	logLevel        = flag.String("l", "info", "Log level")
	endpoint        = flag.String("e", "default-s3-endpoint:9000", "S3 endpoint host:port")
	accessKeyID     = flag.String("a", "TEMP_DEMO_ACCESS_KEY", "S3 access key ID")
	secretAccessKey = flag.String("k", "TEMP_DEMO_SECRET_KEY", "S3 secret key")
	useSSL          = flag.Bool("s", false, "Use SSL (default disabled)")

	writeBucketName = "s3-loadgen-writes"
	readBucketName  = "s3-loadgen-reads"
	location        = "par"

	writeOpCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_write_counter",
		Help: "s3 writes operations counter.",
	})
	writeErrCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_write_err_counter",
		Help: "s3 writes errors counter.",
	})
	readOpCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_read_counter",
		Help: "s3 reads operations counter.",
	})
	readErrCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_read_err_counter",
		Help: "s3 read errors counter.",
	})
	writeDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "s3_write_durations_histogram_seconds",
		Help:    "S3 write operations latency distributions.",
		Buckets: []float64{0.01, 0.0125, 0.015, 0.0175, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1, 0.125, 0.15, 0.175, 0.2, 0.25, 0.3, 0.4, 0.5},
	})
	readDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "s3_read_durations_histogram_seconds",
		Help:    "S3 read operations latency distributions.",
		Buckets: []float64{0.01, 0.0125, 0.015, 0.0175, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1, 0.125, 0.15, 0.175, 0.2, 0.25, 0.3, 0.4, 0.5},
	})
)

const letterBytes = "\nabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\n"

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  ./s3-loadgen -e <HOST:PORT> -a <S3_ID> -k <S3_secret> [-s] [-l <loglevel>]\n")
	os.Exit(2)
}

func initPrometheus() {
	prometheus.MustRegister(writeOpCounter)
	prometheus.MustRegister(writeErrCounter)
	prometheus.MustRegister(writeDurationsHistogram)
	prometheus.MustRegister(readOpCounter)
	prometheus.MustRegister(readErrCounter)
	prometheus.MustRegister(readDurationsHistogram)
}

func initLogger() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	level, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Warn("Impossible to parse log level (-l), fallback to info")
	} else {
		log.SetLevel(level)
	}
}

func prepareBuckets(minioClient *minio.Client) {
	// Verify & creates new buckets for reads and writes operations.
	// Create write bucket
	err := minioClient.MakeBucket(writeBucketName, location)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, err := minioClient.BucketExists(writeBucketName)
		if err == nil && exists {
			log.Printf("We already own %s bucket\n", writeBucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s bucket\n", writeBucketName)
	}

	// Create read bucket
	err = minioClient.MakeBucket(readBucketName, location)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, err := minioClient.BucketExists(readBucketName)
		if err == nil && exists {
			log.Printf("We already own %s bucket\n", readBucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s bucket\n", readBucketName)
	}
}

func fillReadBucket(minioClient *minio.Client) {
	for i := 1; i <= 1000; i++ {
		writeObjectWithID(i, minioClient)
	}
}

func randomAlphaString(len int) string {
	b := make([]byte, len)
	for i := range b {
		b[i] = letterBytes[rand.Intn(54)]
	}
	return string(b)
}

func writeObjectWithID(id int, minioClient *minio.Client) {
	tmpfile, err := ioutil.TempFile("/tmp/", "s3-loadgen-")
	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	objectNameWithID := fmt.Sprintf("s3-loadgen-%d", id)

	if err != nil {
		log.Error("Cannot create temporary file: ", err)
		return
	}

	if _, err = tmpfile.Write([]byte(randomAlphaString(250000))); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}

	n, err := minioClient.FPutObject(readBucketName, objectNameWithID, tmpfile.Name(), minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		log.Error("Cannot create temporary file: ", err)
		return
	}
	log.Debugf("Object stored in read bucket: %s (%dB)", objectNameWithID, n)
}

func readRndObject(minioClient *minio.Client) {
	readOpCounter.Inc()
	objectNameWithID := fmt.Sprintf("s3-loadgen-%d", rand.Intn(1000)+1)
	objectFilePath := fmt.Sprintf("/tmp/%s", objectNameWithID)

	start := time.Now()
	err := minioClient.FGetObject(readBucketName, objectNameWithID, objectFilePath, minio.GetObjectOptions{})
	readDuration := time.Since(start)

	if err != nil {
		log.Errorf("Cannot get S3 object: %s (to %s) ", objectNameWithID, objectFilePath)
		readErrCounter.Inc()
		return
	}
	readDurationsHistogram.Observe(readDuration.Seconds())

	defer os.Remove(objectFilePath)
	log.Debugf("Object fetched from read bucket: %s", objectNameWithID)
}

func writeRndObject(minioClient *minio.Client) {
	writeOpCounter.Inc()
	tmpfile, err := ioutil.TempFile("/tmp/", "s3-loadgen-")
	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	if err != nil {
		log.Error("Cannot create temporary file: ", err)
		writeErrCounter.Inc()
		return
	}

	if _, err = tmpfile.Write([]byte(randomAlphaString(250000))); err != nil {
		log.Fatal("Failed to write to temporary file", err)
		writeErrCounter.Inc()
		return
	}

	start := time.Now()
	n, err := minioClient.FPutObject(writeBucketName, path.Base(tmpfile.Name()), tmpfile.Name(), minio.PutObjectOptions{ContentType: "text/plain"})
	writeDuration := time.Since(start)

	if err != nil {
		log.Error("Cannot create temporary file: ", err)
		writeErrCounter.Inc()
		return
	}
	writeDurationsHistogram.Observe(writeDuration.Seconds())

	log.Debugf("Object stored: %s (%dB) in %f seconds", path.Base(tmpfile.Name()), n, writeDuration.Seconds())
}

func main() {
	flag.Usage = usage
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
	initLogger()
	initPrometheus()

	// Initialize Prometheus metrics endpoint
	log.Info("Serving Prometheus metrics endpoint at :9090")
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":9090", nil)

	// Initialize minio client object
	log.Infof("Connecting to %s", *endpoint)
	minioClient, err := minio.New(*endpoint, *accessKeyID, *secretAccessKey, *useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	log.Info("Minio S3 client successfully bootstrapped")
	log.Debugf("%#v\n", minioClient) // Debug minioClient instance

	prepareBuckets(minioClient)
	fillReadBucket(minioClient)

	// go writeObjectLoop(minioClient)
	writeRndObject(minioClient)

	tickerWrites := time.NewTicker(250 * time.Millisecond)
	tickerReads := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-tickerWrites.C:
			go writeRndObject(minioClient)
		case <-tickerReads.C:
			go readRndObject(minioClient)
		}
	}
}
