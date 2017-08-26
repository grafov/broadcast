package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/grafov/bcast"
	"github.com/grafov/broadcast"
)

func main() {
	benchBroadcast()
	time.Sleep(100 * time.Millisecond)
	benchBcast()
}

func benchBroadcast() {
	const (
		totalReaders  = 1000
		totalMessages = 10000
	)
	var wg sync.WaitGroup
	ch := broadcast.New()
	var members [totalReaders]*broadcast.Reader
	for i := 0; i < totalReaders; i++ {
		members[i] = ch.AddReader()
	}

	started := time.Now()
	for n := 0; n < totalReaders; n++ {
		wg.Add(1)
		go func(i int) {
			r := members[i]
			for n := 0; n < totalMessages; n++ {
				r.Recv()
			}
			wg.Done()
		}(n)
	}
	for n := 0; n < totalMessages; n++ {
		ch.Send("sample message for the benchmark")
	}
	print("sent")
	wg.Wait()
	end := time.Since(started)
	fmt.Printf("brodcast package time %v\n", end)
}

func benchBcast() {
	const (
		totalReaders  = 1000
		totalMessages = 10000
	)
	var wg sync.WaitGroup
	group := bcast.NewGroup()           // create broadcast group
	go group.Broadcast(2 * time.Minute) // accepts messages
	var members [totalReaders]*bcast.Member
	for i := 0; i < totalReaders; i++ {
		members[i] = group.Join()
	}

	started := time.Now()
	for n := 0; n < totalReaders; n++ {
		wg.Add(1)
		go func(i int) {
			r := members[i]
			for n := 0; n < totalMessages; n++ {
				r.Recv()
			}
			wg.Done()
		}(n)
	}
	for n := 0; n < totalMessages; n++ {
		group.Send("sample message for the benchmark")
	}
	wg.Wait()
	end := time.Since(started)
	fmt.Printf("bcast package time %v\n", end)
}
