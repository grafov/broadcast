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

var maxDataItems = 100

func consume(buf *ringbuf, name int) {
	var (
		cur int64
		w   int
	)
	for i := 0; i < maxDataItems*generators; i++ {
	checkCounter:
		adr := &buf.RCount[cur]
		rcount := atomic.LoadInt64(adr)
		// Check when generator writing to this row (-1) or the row
		// is empty (0).
		if rcount <= 0 {
			time.Sleep(10 * time.Microsecond)
			w++
			fmt.Println(name, cur, "wait for data", w)
			goto checkCounter
		}

		// RCount should still be > 0 in this place.

		// Generator will wait until RCount=0 so it can be safely read
		// from the row here.
		val := buf.Rows[cur]

		// Pass data to something
		// ...
		fmt.Println(name, cur, val.(string))

		// The row has been consumed so decrease read count.
		atomic.AddInt64(adr, -1)

		// Scroll to the next row.
		cur++
		cur = cur % buflen
	}
}

var (
	generators = 30
	consumers  = 3
)

func generate(rb *ringbuf, name int) {
	var (
		cur int64
		adr = &rb.RCount[cur]
	)
	for i := 0; i < maxDataItems; i++ {
	waitForWorkers:
		// If data has been read by all workers then the RCount must
		// be zero. For clarity generator set it to -1. It will
		// prevent this row from reading by the workers.
		if !atomic.CompareAndSwapInt64(adr, 0, -1) {
			if rc := atomic.LoadInt64(adr); rc == -1 {
				// The row already has been taken by another writer.
				// Scroll to the next one.
				cur++
				cur++
				cur = cur % buflen
				adr = &rb.RCount[cur]
			} else {
				// The row hasn't read yet. So the generator waits for all
				// the consumers read it.
				time.Sleep(50 * time.Nanosecond)
			}
			goto waitForWorkers
		}

		// vvv CRITICAL CODE SECTION: WRITE TO THE ROW IS ALLOWED HERE

		// The row protected by Rcount=-1 here.
		// So it can safely assign a new value here.
		rb.Rows[cur] = fmt.Sprintf("g%d-%d", name, i)

		fmt.Println(fmt.Sprintf("G%d", name), cur, rb.Rows[cur], atomic.LoadInt64(adr))

		// Reinitialize RCount for enable reading again: set it to the number of workers.
		atomic.StoreInt64(adr, int64(consumers))

		// ^^^ THE END OF CRITICAL CODE SECTION

		// Scroll to the next row.
		cur++
		cur = cur % buflen
	}
}

func main() {
	//	tfile, _ := os.OpenFile("/tmp/trace.out", os.O_CREATE|os.O_WRONLY, 0666)
	//	trace.Start(tfile)

	var rb ringbuf

	// Start consumers. The generator generates sequence, stores it
	// to ringbuf and exits.
	for i := 0; i < generators; i++ {
		go generate(&rb, i)
	}

	// Create and run workers.
	for i := 0; i < consumers; i++ {
		go consume(&rb, i)
	}

	time.Sleep(2 * time.Second)
	//	fmt.Printf("%#v\n", rb.Rows)

	//	trace.Stop()
}
