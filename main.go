package main

import (
	"flag"
	"fmt"
	"log"

	//	"os"
	//	"runtime/pprof"

	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	YYYYMMDD = "2006-01-02"
)

var requestUnits int64

func main() {
	/*
		pprofFile, pprofErr := os.Create("cpu.pprof")
		if pprofErr != nil {
			log.Fatal(pprofErr)
		}
		pprof.StartCPUProfile(pprofFile)
		defer pprof.StopCPUProfile()
		//*/
	now := time.Now().UTC()
	var nowStr string

	var numDays int
	var shouldExport bool

	flag.BoolVar(&shouldExport, "s3export", false, "Should export database to s3")
	flag.StringVar(&nowStr, "targetDate", now.Format(YYYYMMDD), "Target date to start printing as expanded")
	flag.IntVar(&numDays, "numDays", 7, "Number of days to print out details of")

	flag.Parse()

	//fmt.Print(shouldExport)

	if nowStr != now.Format(YYYYMMDD) {
		newNow, err := time.Parse(YYYYMMDD, nowStr)
		if err != nil {
			log.Fatal("Bad date format", err)
		}
		now = newNow
	}

	s := session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	generateReport(s, now, numDays, shouldExport)

	fmt.Printf("Total Capacity Units = %.1f\n", float64(requestUnits)*(1./2.))
}

func generateReport(session *session.Session, now time.Time, N int, shouldExport bool) {
	var futures []futureStatsString

	dSvc := dynamodb.New(session)
	s3svc := s3.New(session)

	nowStr := now.Format(YYYYMMDD)
	futures = append(futures, getStatsString(nowStr, dSvc, s3svc, true, shouldExport))

	for i := 1; i <= N; i++ {
		yesterdayStr := now.AddDate(0, 0, -i).Format(YYYYMMDD)
		futures = append(futures, getStatsString(yesterdayStr, dSvc, s3svc, false, shouldExport))
	}

	for _, item := range futures {
		for line := range item {
			fmt.Print(line)
		}
	}

}

func makeRequest(svc *dynamodb.DynamoDB, whenAt string, items *[]HitCountItem, ch chan bool) {

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(whenAt),
			},
		},
		KeyConditionExpression: aws.String("as_of_when = :v1"),
		TableName:              aws.String("hit_counts"),
		IndexName:              aws.String("as_of_when-index"),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	result, err := svc.Query(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return // make([]HitCountItem, 0)
	}

	atomic.AddInt64(&requestUnits, int64(*result.ConsumedCapacity.CapacityUnits*2))
	// result.ConsumedCapacity.CapacityUnits

	//log.Fatal(result.Items)

	for index := range result.Items {
		item := result.Items[index]

		var lastHit string
		lastHitPtr := item["last_hit_at"]
		if lastHitPtr == nil {
			lastHit = ""
		} else {
			lastHit = *lastHitPtr.S
		}

		thing := HitCountItem{
			AsOfWhen: StringItem{
				S: *item["as_of_when"].S,
			},
			Url: StringItem{
				S: *item["the_url"].S,
			},
			TodayCount: NumberItem{
				N: *item["today_count"].N,
			},
			LastHit: StringItem{
				S: lastHit,
			},
			AccumCount: NumberItem{
				N: *item["accumulated_count"].N,
			},
			UserIdHashes: (*item["user_id_hashes"]).SS,
		}

		*items = append(*items, thing)

	}

	ch <- true

	return // items
}
