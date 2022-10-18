package main

import (
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

	sort.Slice(items, func(i, j int) bool {
		return int10(items[i].TodayCount.N) > int10(items[j].TodayCount.N)
	})

	for index := range items {
		if !strings.HasSuffix(items[index].Url.S, ".html") {
			num := int10(items[index].TodayCount.N)
			uncounted += int(num)
			continue
		}

		num := int10(items[index].TodayCount.N)
		sum += int(num)

		fmt.Printf("%50s %10s %4d %5d\n",
			items[index].Url.S, items[index].LastHit.S[11:], num, sum)
	}

	fmt.Printf("Non post hits = %d\n", uncounted)
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

func mainDatePicker() {

	s := session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	svc := dynamodb.New(s)
	var wg sync.WaitGroup

	datesChan := generateDates()

	dataMap := make(map[string][]HitCountItem)

	for date := range datesChan {
		wg.Add(1)

		go func(d string) {
			var list []HitCountItem

			makeRequest2(svc, d, &list)
			dataMap[d] = list

			wg.Done()
		}(date)

	}
	wg.Wait()

	fmt.Println(dataMap)

}

func main() {
	now := time.Now().UTC()
	nowStr := now.Format(YYYYMMDD)

	yesterdayStr := "2022-09-20"

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
	//printReport(old_items)
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

func makeRequest2(svc *dynamodb.DynamoDB, whenAt string, items *[]HitCountItem) {

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
		}

		*items = append(*items, thing)
	}

	return // items
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
