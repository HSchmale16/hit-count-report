package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type StringItem struct {
	S string
}

type NumberItem struct {
	N string
}

type HitCountItem struct {
	TodayCount   NumberItem `json:"today_count"`
	AccumCount   NumberItem `json:"accumulated_count"`
	AsOfWhen     StringItem `json:"as_of_when"`
	Url          StringItem `json:"the_url"`
	LastHit      StringItem `json:"last_hit_at"`
	UserIdHashes []*string  `json:"user_id_hashes"`
	Delta        int        // delta from last position
}

type futureStatsString chan string

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
