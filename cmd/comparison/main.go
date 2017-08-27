package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/grafov/bcast"
	"github.com/grafov/broadcast"
)

const (
	totalReaders  = 200000
	totalMessages = 100
)

func main() {
	benchBroadcast()
	time.Sleep(100 * time.Millisecond)
	benchBcast()
}

func benchBroadcast() {
	var wg sync.WaitGroup
	ch := broadcast.New()
	var members [totalReaders]*broadcast.Reader
	for i := 0; i < totalReaders; i++ {
		members[i] = ch.AddReader()
	}
	//	var results [totalReaders][totalMessages]string

	started := time.Now()
	for n := 0; n < totalReaders; n++ {
		wg.Add(1)
		go func(i int) {
			r := members[i]
			for n := 0; n < totalMessages; n++ {
				_, ok := r.Recv()
				if !ok {
					break
				}
				//				results[i][n] = result.(string)
			}
			wg.Done()
		}(n)
	}
	//	time.Sleep(3 * time.Second)
	for n := 0; n < totalMessages; n++ {
		ch.Send(fmt.Sprintf("message-%d", n))
	}
	println("broadcast: messages sent")
	wg.Wait()
	// println("=========================================")
	// for m, values := range results {
	// 	for i, value := range values {
	// 		println(m, i, value)
	// 	}
	// 	println()

	// }
	end := time.Since(started)
	fmt.Printf("brodcast package time %v\n", end)
}

func benchBcast() {
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
		group.Send(fmt.Sprintf("message-%d", n))
	}
	println("bcast: messages sent")
	wg.Wait()
	end := time.Since(started)
	fmt.Printf("bcast package time %v\n", end)
}
