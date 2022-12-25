package main

import "strconv"

func int10(s string) int64 {
	a, _ := strconv.ParseInt(s, 10, 0)
	return a
}
