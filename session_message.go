package main

import "time"

type sessionMessage struct {
	userid string
	time   time.Time
	retry  int
}
