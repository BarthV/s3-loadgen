package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/minio/minio-go"
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
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  ./s3-loadgen -e <HOST:PORT> -a <S3_ID> -k <S3_secret> [-s] [-l <loglevel>]\n")
	os.Exit(2)
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

func writeRndObject(minioClient *minio.Client) {
	tmpfile, err := ioutil.TempFile("/tmp/", "s3-loadgen-")
	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	if err != nil {
		log.Error("Cannot create temporary file: ", err)
		return
	}

	if _, err = tmpfile.Write([]byte(randomdata.Paragraph())); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}

	n, err := minioClient.FPutObject(writeBucketName, path.Base(tmpfile.Name()), tmpfile.Name(), minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		log.Error("Cannot create temporary file: ", err)
		return
	}
	log.Debugf("Object stored: %s - %d", path.Base(tmpfile.Name()), n)
}

func writeObjectLoop(minioClient *minio.Client) {
	tickerWrites := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-tickerWrites.C:
			go writeRndObject(minioClient)
		}
	}
}

func prepareBuckets(minioClient *minio.Client) {
	// Make a new bucket for reads and writes operations.
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

func main() {
	flag.Usage = usage
	flag.Parse()

	initLogger()

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

	go writeObjectLoop(minioClient)
	// writeRndObject(minioClient)

	// lol :troll:
	endlessWait := make(chan struct{})
	<-endlessWait
}
