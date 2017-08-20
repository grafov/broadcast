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
	var (
		cur int64
		w   int
	)
	for i := 0; i < maxDataItems; i++ {
	checkCounter:
		rcount := atomic.LoadInt64(&buf.RCount[cur])
		// Check when generator writing to this row (-1) or the row
		// has been read by all consumers.
		if rcount <= 0 {
			time.Sleep(10 * time.Microsecond)
			w++
			fmt.Println(name, cur, "wait for data", w)
			goto checkCounter
		}

		// Generator will wait until RCount=0 so it can be safely read
		// from the row here.
		val := buf.Rows[cur]

		// The row has been consumed so decrease read count.
		atomic.AddInt64(&buf.RCount[cur], -1)

		// Pass data to something
		// ...
		fmt.Println(name, cur, val.(int))

		// Scroll to the next row.
		cur++
		cur = cur % buflen
	}
}

var workers = 10

func generate(data *ringbuf) int {
	var (
		cur int64
		w   int
	)
	for i := 0; i < maxDataItems; i++ {
	waitForWorkers:
		// If data has been read by all workers then the RCount must
		// be zero.  For clarity generator set it to -1. It will
		// prevent this row from reading by the workers.
		if !atomic.CompareAndSwapInt64(&data.RCount[cur], 0, -1) {
			// The row hasn't read yet. So the generator waits for all
			// the consumers read it.
			time.Sleep(5 * time.Microsecond)
			w++
			goto waitForWorkers
		}

		// vvv CRITICAL CODE SECTION: WRITE TO THE ROW IS ALLOWED HERE

		// The row protected by Rcount=-1 here.
		// So it can safely assign a new value here.
		data.Rows[cur] = i

		// Reinitialize RCount for enable reading again: set it to the number of workers.
		atomic.StoreInt64(&data.RCount[cur], int64(workers))

		// ^^^ THE END OF CRITICAL CODE SECTION

		// Here the data from the row could be read by any worker.
		fmt.Println("G", cur, data.Rows)

		// Scroll to the next row.
		cur++
		cur = cur % buflen
	}
	return w
}

func main() {
	var rb ringbuf
	// Create and run workers.
	for i := 0; i < workers; i++ {
		go consume(&rb, i)
	}

	// Start generator. It generates sequence, stores it to ringbuf and exits.
	w := generate(&rb)

	time.Sleep(100 * time.Millisecond)
	fmt.Printf("%#v\n", rb.Rows)
	fmt.Printf("wait workers for %d\n", w)
}
