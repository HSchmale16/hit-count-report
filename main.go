package main

import (
	"flag"
	"fmt"
	"log"
//	"os"
//	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	YYYYMMDD = "2006-01-02"
)

var requestUnits int64

type futureStatsString chan string

type StringItem struct {
	S string
}

type NumberItem struct {
	N string
}

type HitCountItem struct {
	TodayCount   NumberItem `json:"today_count"`
	AccumCount   NumberItem `json:"accumulated_count"`
	AsOfWhen     StringItem
	Url          StringItem `json:"the_url"`
	LastHit      StringItem `json:"last_hit_at"`
	UserIdHashes []*string
	Delta        int // delta from last position
}

type HitCountQueryResult struct {
	Items []HitCountItem
}

type SummaryStats struct {
	DistinctPosts    int64
	PostHits         int64
	NotPosts         int64
	NotPostViews     int64
	TotalViews       int64
	NumDistinctUsers int
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
		return items[i].LastHit.S[11:19] > items[j].LastHit.S[11:19]
	})

	// we're going to check all the users for a post and see if they show up greater than once in the collection.
	users := make(map[string]int)
	for _, item := range items {
		for _, user := range item.UserIdHashes {
			users[*user] += 1
		}
	}

	results <- fmt.Sprintf("%45s  %10s  %4s  %5s\n",
		"Post", "Last Hit", "Hits", "Total")

	for index, item := range items {
		url := items[index].Url.S
		if strings.HasSuffix(url, ".html") && strings.HasPrefix(url, "/20") {
			url = url[1 : len(url)-5]

			// If the same user has visited at least one other page today mark it as such
			for _, user := range item.UserIdHashes {
				if users[*user] > 1 {
					url = "(*) " + url
					break
				}
			}

			num := int10(items[index].TodayCount.N)
			sum += int(num)

			results <- fmt.Sprintf("%45s  %10s  %4d  %5d\n",
				url, items[index].LastHit.S[11:19], num,
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

	users := make(map[string]int8)

	for index, value := range items {
		url := items[index].Url.S

		for _, user := range value.UserIdHashes {
			users[*user] += 1
		}

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

	stats.NumDistinctUsers = len(users)

	return stats
}

func getStatsString(AsOfWhen string, svc *dynamodb.DynamoDB, fullReport bool) futureStatsString {
	ch := make(futureStatsString, 5)

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

		ch <- fmt.Sprintf("\t%s Posts(Tv/Cnt): %3d/%3d + Others: %3d/%3d = %4d(u=%d)\n",
			AsOfWhen,
			stats.PostHits, stats.DistinctPosts,
			stats.NotPostViews, stats.NotPosts,
			stats.TotalViews,
			stats.NumDistinctUsers,
		)

		close(ch)
	}()

	return ch
}

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

	fmt.Printf("Total Capacity Units = %.1f\n", float64(requestUnits)*(1./2.))

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
