// Proof for broadcasting: no channels for workers, no sync.Mutex.
package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

const buflen = 10

type ringbuf struct {
	Rows   [buflen]interface{}
	RCount [buflen]int64
}

var maxDataItems = 30

func consume(buf *ringbuf, name int) {
	var cur int64
	var w int
	for i := 0; i < maxDataItems; i++ {
	checkCounter:
		rcount := atomic.LoadInt64(&buf.RCount[cur])
		// Row is empty.
		if rcount <= 0 {
			time.Sleep(10 * time.Microsecond)
			w++
			fmt.Println(name, cur, "wait for data", w)
			goto checkCounter
		}
		val := buf.Rows[cur]
		atomic.AddInt64(&buf.RCount[cur], -1)
		// pass data to something
		// ...
		fmt.Println(name, cur, val.(int))
		cur++
		cur = cur % buflen
	}
}

var workers = 10

func generate(data *ringbuf) int {
	var cur int64
	var w int
	for i := 0; i < maxDataItems; i++ {
	waitForWorkers:
		if !atomic.CompareAndSwapInt64(&data.RCount[cur], 0, -1) {
			time.Sleep(10 * time.Microsecond)
			w++
			goto waitForWorkers
		}
		// Assign new value:
		data.Rows[cur] = i
		atomic.StoreInt64(&data.RCount[cur], int64(workers))
		fmt.Println("G", cur, data.Rows)
		cur++
		cur = cur % buflen
	}
	return w
}

func main() {
	var rb ringbuf
	for i := 0; i < workers; i++ {
		go consume(&rb, i)
	}
	w := generate(&rb)
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("%#v\n", rb.Rows)
	fmt.Printf("wait workers for %d\n", w)
}
