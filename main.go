package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	YYYYMMDD = "2006-01-02"
)

type futureStatsString chan string

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

type SummaryStats struct {
	DistinctPosts int64
	PostHits      int64
	NotPosts      int64
	NotPostViews  int64
	TotalViews    int64
}

func int10(s string) int64 {
	a, _ := strconv.ParseInt(s, 10, 0)
	return a
}

func printReport(items []HitCountItem, results futureStatsString) {
	sum := 0
	uncounted := 0
	distinctUncounts := 0

	sort.Slice(items, func(i, j int) bool {
		a := int10(items[i].TodayCount.N)
		b := int10(items[j].TodayCount.N)

		if a != b {
			return a > b
		}
		return items[i].AsOfWhen.S < items[j].AsOfWhen.S
	})
	results <- fmt.Sprintf("%45s\t%10s\t%4s\t%5s\t%5s\n",
		"Post", "Last Hit", "Hits", "Accum", "Total")

	for index := range items {
		url := items[index].Url.S
		if strings.HasSuffix(url, ".html") && strings.HasPrefix(url, "/20") {
			url = url[1 : len(url)-5]

			num := int10(items[index].TodayCount.N)
			sum += int(num)

			results <- fmt.Sprintf("%45s\t%10s\t%4d\t%5d\t%5d\n",
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

func computeSummaryStats(items []HitCountItem) SummaryStats {
	stats := SummaryStats{}

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

func getStatsString(AsOfWhen string, svc *dynamodb.DynamoDB, fullReport bool) futureStatsString {
	ch := make(futureStatsString)

	go func() {
		var items []HitCountItem

		ch2 := make(chan bool)
		go makeRequest(svc, AsOfWhen, &items, ch2)
		<-ch2
		close(ch2)

		stats := computeSummaryStats(items)

		if fullReport {
			printReport(items, ch)
		}

		ch <- fmt.Sprintf("%s Posts(Views/Distinct): %3d / %3d + Others: %3d / %3d  = %4d \n",
			AsOfWhen,
			stats.PostHits, stats.DistinctPosts,
			stats.NotPostViews, stats.NotPosts,
			stats.TotalViews)

		close(ch)
	}()

	return ch
}

func main() {
	pprofFile, pprofErr := os.Create("cpu.pprof")
	if pprofErr != nil {
		log.Fatal(pprofErr)
	}
	pprof.StartCPUProfile(pprofFile)
	defer pprof.StopCPUProfile()
	//*/
	now := time.Now().UTC()
	var nowStr string

	var N int

	flag.StringVar(&nowStr, "targetDate", now.Format(YYYYMMDD), "Target date to start printing as expanded")
	flag.IntVar(&N, "numDays", 7, "Number of days to print out details of")

	flag.Parse()

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
	svc := dynamodb.New(s)

	var futures []futureStatsString

	futures = append(futures, getStatsString(nowStr, svc, true))

	for i := 1; i <= N; i++ {
		yesterdayStr := now.AddDate(0, 0, -i).Format(YYYYMMDD)
		futures = append(futures, getStatsString(yesterdayStr, svc, false))
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
		}

		*items = append(*items, thing)

	}

	ch <- true

	return // items
}
