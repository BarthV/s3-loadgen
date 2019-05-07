package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
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
	readMissCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_read_miss_counter",
		Help: "s3 reads miss counter.",
	})
	readHitCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_read_hit_counter",
		Help: "s3 reads hit counter.",
	})
	readErrCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "s3_read_err_counter",
		Help: "s3 read errors counter.",
	})
	writeDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "s3_write_durations_histogram_seconds",
		Help:    "S3 write operations latency distributions.",
		Buckets: []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.11, 0.12, 0.14, 0.16, 0.18, 0.2, 0.25, 0.3, 0.4, 0.5, 0.75, 1},
	})
	readDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "s3_read_durations_histogram_seconds",
		Help:    "S3 read operations latency distributions.",
		Buckets: []float64{0.006, 0.01, 0.012, 0.014, 0.016, 0.018, 0.02, 0.024, 0.028, 0.035, 0.05, 0.075, 0.1, 0.125, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5},
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
	prometheus.MustRegister(readHitCounter)
	prometheus.MustRegister(readMissCounter)
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
	for i := 1; i <= 2000; i++ {
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
	objectNameWithID := fmt.Sprintf("s3-loadgen-%d", id)

	n, err := minioClient.PutObject(readBucketName, objectNameWithID, strings.NewReader(randomAlphaString(250000)), 250000, minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		log.Error("Cannot put S3 Object: ", err)
		return
	}
	log.Debugf("Object stored in read bucket: %s (%dB)", objectNameWithID, n)
}

func readRndObject(minioClient *minio.Client) {
	readOpCounter.Inc()
	objectNameWithID := fmt.Sprintf("s3-loadgen-%d", rand.Intn(2000)+1)

	start := time.Now()
	obj, err := minioClient.GetObject(readBucketName, objectNameWithID, minio.GetObjectOptions{})
	if err != nil {
		log.Errorf("Cannot touch S3 object %s : %s", objectNameWithID, err)
		readErrCounter.Inc()
		return
	}
	defer obj.Close()

	stat, err := obj.Stat()
	if err != nil {
		if err.Error() == "The specified key does not exist." {
			log.Warnf("Object %s is missing in read bucket !", objectNameWithID)
			readMissCounter.Inc()
			return
		}
		log.Errorf("Cannot stats S3 object %s : %s", objectNameWithID, err)
		readErrCounter.Inc()
		return
	}

	n, err := io.CopyN(ioutil.Discard, obj, stat.Size)
	readDuration := time.Since(start)
	if err != nil {
		log.Errorf("Cannot read S3 object %s : %s", objectNameWithID, err)
		readErrCounter.Inc()
		return
	}

	readDurationsHistogram.Observe(readDuration.Seconds())
	readHitCounter.Inc()
	log.Debugf("Object fetched: %s (%dB) in %f seconds", objectNameWithID, n, readDuration.Seconds())
}

func writeRndObject(minioClient *minio.Client) {
	writeOpCounter.Inc()
	objectRandomName := fmt.Sprintf("s3-loadgen-%d", rand.Intn(9999999999)+1)

	start := time.Now()
	n, err := minioClient.PutObject(writeBucketName, objectRandomName, strings.NewReader(randomAlphaString(250000)), 250000, minio.PutObjectOptions{ContentType: "text/plain"})
	writeDuration := time.Since(start)

	if err != nil {
		log.Error("Cannot put S3 Object: ", err)
		writeErrCounter.Inc()
		return
	}
	writeDurationsHistogram.Observe(writeDuration.Seconds())

	log.Debugf("Object stored: %s (%dB) in %f seconds", objectRandomName, n, writeDuration.Seconds())
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

	tickerWrites := time.NewTicker(200 * time.Millisecond)
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
