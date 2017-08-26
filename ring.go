package broadcast

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const buflen = 160

type (
	channel struct {
		sync.Mutex // XXX

		Data       *ringbuf
		Readers    int64
		Generation uint64
	}
	ringbuf struct {
		WriteTo    int64
		WritePause int64
		Payload    [buflen]interface{}
		RC         [buflen]int64
		Generation [buflen]uint64
	}
)

func New() *channel {
	return &channel{Data: new(ringbuf)}
}

type Reader struct {
	c         *channel
	cur       int64
	version   uint64
	waitCount int
}

func (c *channel) AddReader() *Reader {
	var r = new(Reader)
	r.c = c
	atomic.AddInt64(&c.Readers, 1)
	r.version = atomic.AddUint64(&c.Generation, 1)
	return r
}

func (w *Reader) Close() {
	// XXX check for negatives
	atomic.AddInt64(&w.c.Readers, -1)
}

// Scroll reader to the next row.
func (r *Reader) nextRow() {
	r.cur++
	r.cur %= buflen
}

func (r *Reader) Recv() interface{} {
checkRC:
	adr := &r.c.Data.RC[r.cur]
	rc := atomic.LoadInt64(adr)
	// Check when generator writing to this row (-1) or the row
	// is empty (0).
	if rc <= 0 {
		println("nothing for read")
		r.c.printDebug()
		if r.waitCount > 10000 {
			time.Sleep(10000 * time.Nanosecond)
			goto checkRC
		}
		r.waitCount++
		time.Sleep(time.Duration(r.waitCount))
		goto checkRC
	}
	r.waitCount = 0
	// RC should still be > 0 in this place.
	// Generator will wait until RC=0 so it can be safely read
	// from the row here.
	version := r.c.Data.Generation[r.cur]
	if r.version < version {
		r.version = version
		// XXX обновить генерацию
		// XXX обновить значение readers из ch
	}
	result := r.c.Data.Payload[r.cur]
	// The row has been consumed so decrease read count.
	atomic.AddInt64(adr, -1)
	r.nextRow()
	return result
}

type Writer struct {
	c         *channel
	cur       int64
	version   uint64
	waitCount int
}

func (c *channel) Send(data interface{}) {
	var (
		cur = atomic.LoadInt64(&c.Data.WriteTo)
		adr = &c.Data.RC[cur]
	)
waitForReaders:
	// If data has been read by all workers then the RC must
	// be zero. For clarity generator set it to -1. It will
	// prevent this row from reading by the workers.
	if !atomic.CompareAndSwapInt64(adr, 0, -1) {
		if rc := atomic.LoadInt64(adr); rc == -1 {
			// The row already has been taken by another writer.
			cur++
			cur %= buflen
			atomic.StoreInt64(&c.Data.WriteTo, cur)
			println("taken", rc, cur)
			adr = &c.Data.RC[cur]
			atomic.StoreInt64(&c.Data.WritePause, 0)
		} else {
			// The row hasn't read yet. So the generator waits for all
			// the consumers read it.
			pause := atomic.LoadInt64(&c.Data.WritePause)
			if pause > 10000 {
				time.Sleep(10000 * time.Nanosecond)
				rc := atomic.LoadInt64(adr)
				// XXX
				println("here big", rc, pause, c.Data.WriteTo)
				c.printDebug()
				os.Exit(1)
				goto waitForReaders
			}
			atomic.AddInt64(&c.Data.WritePause, 1)
			rc := atomic.LoadInt64(adr)
			time.Sleep(time.Duration(pause))
			println("here", rc, pause) // XXX
		}
		goto waitForReaders
	}
	// The row protected by Rcount=-1 here.
	// So it can safely assign a new value here.
	c.Data.Payload[cur] = data
	// Reinitialize RC for enable reading again: set it to the number of workers.
	atomic.StoreInt64(adr, c.Readers)
	cur++
	cur %= buflen
	atomic.StoreInt64(&c.Data.WriteTo, cur)
}

func (c *channel) printDebug() {
	c.Lock()
	for i, rc := range c.Data.RC {
		fmt.Printf("%4d: %5d %v\n", i, rc, c.Data.Payload[i])
	}
	c.Unlock()
}
