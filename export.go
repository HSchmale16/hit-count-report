/**
 * Contains methods to export the dynamodb table into monthly json files
 */

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	AWS_S3_REGION        = "us-east-1"
	UPLOAD_BUCKET        = "hit-count-exports-us-east-1"
	UPLOAD_OBJECT_FORMAT = "YEAR=2006/01-02"
)

type exportComplete chan bool

func handleExports(dSvc *dynamodb.DynamoDB, numDays int, startTime time.Time, doneChan exportComplete) {
	// fmt.Println("Before sleep the time is:", time.Now().Unix()) // Before sleep the time is: 1257894000
	//time.Sleep(1 * time.Second)
	// fmt.Println("After sleep the time is:", time.Now().Unix()) // After sleep the time is: 1257894002

	s3session, err := session.NewSession(&aws.Config{
		Region: aws.String(AWS_S3_REGION),
	})
	if err != nil {
		log.Fatal(err)
	}

	s3svc := s3.New(s3session)

	var wg sync.WaitGroup
	for i := 1; i <= numDays; i++ {
		wg.Add(1)
		ts := startTime.AddDate(0, 0, -i)
		go handleExportForDay(ts, s3svc, dSvc, &wg)
		//futures = append(futures, getStatsString(yesterdayStr, svc, false))
	}
	fmt.Println("Starting wait")
	wg.Wait()
	fmt.Println("Done waiting")

	doneChan <- true
}

func handleExportForDay(ts time.Time, s3svc *s3.S3, svcDynamodb *dynamodb.DynamoDB, wg *sync.WaitGroup) {
	defer wg.Done()

	targetDateStr := ts.Format(YYYYMMDD)
	objName := ts.Format(UPLOAD_OBJECT_FORMAT) + ".json"

	ch := make(chan bool)
	var items []HitCountItem
	go makeRequest(svcDynamodb, targetDateStr, &items, ch)
	<-ch

	if len(items) == 0 {
		fmt.Println("Skipping ", targetDateStr)
		return
	}

	var data []byte
	for _, item := range items {
		result, err := json.Marshal(item)
		if err != nil {
			log.Fatal(err)
		}
		data = append(data, result...)
		data = append(data, byte('\n'))
	}

	/*
		result, err := gzipBytes(data)
		if err != nil {
			log.Fatal("Failed to gzip data before upload")
		}
	*/

	_, err := s3svc.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(data),
		Bucket: aws.String(UPLOAD_BUCKET),
		Key:    aws.String(objName),
	})

	if err != nil {
		log.Fatal(err)
	}
}

func gzipBytes(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(src)
	if err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
