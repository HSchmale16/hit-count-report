package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	YYYYMMDD = "2006-01-02"
)

type StringItem struct {
	S string
}

type NumberItem struct {
	N string
}

type HitCountItem struct {
	TodayCount NumberItem `json:"today_count"`
	AccumCount NumberItem `json:"accumulated_count"`
	AsOfWhen   StringItem
	Url        StringItem `json:"the_url"`
	LastHit    StringItem `json:"last_hit_at"`
	Delta      int        // delta from last position
}

type HitCountQueryResult struct {
	Items []HitCountItem
}

func int10(s string) int64 {
	a, _ := strconv.ParseInt(s, 10, 0)
	return a
}

func printReport(items []HitCountItem) {
	sum := 0
	uncounted := 0
	distinctUncounts := 0

	sort.Slice(items, func(i, j int) bool {
		a := int10(items[i].TodayCount.N)
		b := int10(items[j].TodayCount.N)

		if a != b {
			return a > b
		}
		return items[i].AsOfWhen.S > items[j].AsOfWhen.S
	})
	fmt.Printf("%45s\t%10s\t%4s\t%5s\t%5s\n",
		"Post", "Last Hit", "Hits", "Accum", "Total")

	for index := range items {
		url := items[index].Url.S
		if strings.HasSuffix(url, ".html") && strings.HasPrefix(url, "/20") {
			url = url[1 : len(url)-5]

			num := int10(items[index].TodayCount.N)
			sum += int(num)

			fmt.Printf("%45s\t%10s\t%4d\t%5d\t%5d\n",
				url, items[index].LastHit.S[11:19], num, sum,
				int10(items[index].AccumCount.N))
		} else {
			num := int10(items[index].TodayCount.N)
			uncounted += int(num)
			distinctUncounts++
			continue
		}
	}
}

func computeDeltas(items *[]HitCountItem, oldItems []HitCountItem) {
	for cIndex, cValue := range *items {

		for oIndex, oValue := range oldItems {
			if oValue.Url == cValue.Url {
				(*items)[cIndex].Delta = cIndex - oIndex
			}
		}
	}
}

func generateDates() chan string {
	channel := make(chan string, 2)
	go func() {
		for i := 0; i < 60; i++ {
			dt := time.Now()
			when := dt.AddDate(0, 0, -i)
			whenStr := when.Format(YYYYMMDD)
			channel <- whenStr
		}
		close(channel)
	}()

	return channel
}

func parallelScan() {
	s := session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	svc := dynamodb.New(s)

	totalCap := 0

	ctx := context.Background()

	var wg sync.WaitGroup
	totalWorkers := int64(7)

	for i := int64(0); i < totalWorkers; i++ {
		wg.Add(1)
		go func(i, total int64) {
			err := svc.ScanPagesWithContext(ctx,
				&dynamodb.ScanInput{
					TableName: aws.String("hit_counts"),
					//IndexName:              aws.String("as_of_when-the_url-index-2"),
					ReturnConsumedCapacity: aws.String("TOTAL"),
					Segment:                aws.Int64(i),
					TotalSegments:          aws.Int64(total),
					//Limit:                  aws.Int64(256),
				},
				func(page *dynamodb.ScanOutput, lastpage bool) bool {
					//fmt.Println(page)

					totalCap += int(*page.ConsumedCapacity.CapacityUnits)

					if lastpage {
						fmt.Println(totalCap)
					}

					return true
				},
			)

			if err != nil {
				log.Fatal(err)
			}

			wg.Done()
		}(i, int64(totalWorkers))
	}

	wg.Wait()
}

type SummaryStats struct {
	DistinctPosts int64
	PostHits      int64
	NotPosts      int64
	NotPostViews  int64
	TotalViews    int64
}

func computeSummaryStats(items []HitCountItem) SummaryStats {
	var stats SummaryStats

	for index, value := range items {
		url := items[index].Url.S

		if strings.HasSuffix(url, ".html") && strings.HasPrefix(url, "/20") {
			stats.PostHits += int10(value.TodayCount.N)
			stats.DistinctPosts += 1
		} else if strings.HasSuffix(url, "-dev") {
			// skip dev mode stuff
			continue
		} else {
			stats.NotPostViews += int10(value.TodayCount.N)
			stats.NotPosts += 1
		}

		stats.TotalViews += int10(value.TodayCount.N)
	}

	return stats
}

func main() {
	/*
		pprofFile, pprofErr := os.Create("cpu.pprof")
		if pprofErr != nil {
			log.Fatal(pprofErr)
		}
		pprof.StartCPUProfile(pprofFile)
		defer pprof.StopCPUProfile()
	*/

	now := time.Now().UTC()
	nowStr := now.Format(YYYYMMDD)
	yesterdayStr := now.AddDate(0, 0, -1).Format(YYYYMMDD)

	s := session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	svc := dynamodb.New(s)

	ch := make(chan bool)
	var old_items []HitCountItem
	var items []HitCountItem
	go makeRequest(svc, nowStr, &items, ch)
	//go loadDynamodbFile(&old_items, ch)
	go makeRequest(svc, yesterdayStr, &old_items, ch)

	<-ch
	<-ch

	computeDeltas(&items, old_items)

	printReport(items)
	stats := computeSummaryStats(items)

	fmt.Printf("Posts(Views/Distinct): %d / %d + Other(Views/Distinct): %d / %d  = %d \n",
		stats.PostHits, stats.DistinctPosts,
		stats.NotPostViews, stats.NotPosts,
		stats.TotalViews)

	stats = computeSummaryStats(old_items)
	fmt.Printf("Yesterday Posts(Views/Distinct): %d / %d + Other(Views/Distinct): %d / %d  = %d \n",
		stats.PostHits, stats.DistinctPosts,
		stats.NotPostViews, stats.NotPosts,
		stats.TotalViews)
}

func loadDynamodbFile(items *[]HitCountItem, ch chan bool) {
	content, err := ioutil.ReadFile("./stuff.json")
	if err != nil {
		log.Fatal("Failed to open: ", err)
	}

	var payload HitCountQueryResult
	err = json.Unmarshal(content, &payload)
	if err != nil {
		log.Fatal("Failed to unmarshall: ", err)
	}

	*items = payload.Items

	fmt.Println("LOADED")
	ch <- true
	fmt.Println("Posted back")
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
		IndexName:              aws.String("as_of_when-the_url-index-2"),
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

	//items := make([]HitCountItem, *result.Count)
	for index := range result.Items {
		item := result.Items[index]
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
				S: *item["last_hit_at"].S,
			},
			AccumCount: NumberItem{
				N: *item["accumulated_count"].N,
			},
		}

		*items = append(*items, thing)

	}

	ch <- true

	return // items
}

func oldMain() {
	var record HitCountQueryResult

	dec := json.NewDecoder(os.Stdin)
	for {
		err := dec.Decode(&record)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)

		}

		printReport(record.Items)

	}
}
