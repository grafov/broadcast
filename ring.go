package broadcast

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const buflen = 8

type (
	channel struct {
		sync.Mutex // XXX

		WriteTo          int64
		WaitReaderCount  int64
		Readers          int64
		GlobalGeneration uint64
		Generation       [buflen]uint64
		RC               [buflen]int64
		Payload          [buflen]interface{}
	}
)

func New() *channel {
	return new(channel)
}

func (c *channel) Send(data interface{}) {
	var (
		cur = atomic.LoadInt64(&c.WriteTo)
		adr = &c.RC[cur]
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
			atomic.StoreInt64(&c.WriteTo, cur)
			println("taken", rc, cur)
			adr = &c.RC[cur]
			atomic.StoreInt64(&c.WaitReaderCount, 0)
		} else {
			// The row hasn't read yet. So the generator waits for all
			// the consumers read it.
			pause := atomic.LoadInt64(&c.WaitReaderCount)
			if pause > 100 {
				time.Sleep(100 * time.Nanosecond)
				rc := atomic.LoadInt64(adr)
				// XXX
				println("here big", rc, pause, c.WriteTo)
				c.printDebug()
				os.Exit(1)
				goto waitForReaders
			}
			atomic.AddInt64(&c.WaitReaderCount, 1)
			rc := atomic.LoadInt64(adr)
			time.Sleep(time.Duration(pause))
			println("here", rc, pause) // XXX
		}
		goto waitForReaders
	}
	// The row protected by Rcount=-1 here.
	// So it can safely assign a new value here.
	c.Payload[cur] = data
	// Reinitialize RC for enable reading again: set it to the number of workers.
	atomic.StoreInt64(adr, atomic.LoadInt64(&c.Readers))
	cur++
	cur %= buflen
	atomic.StoreInt64(&c.WriteTo, cur)
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
	r.version = atomic.AddUint64(&c.GlobalGeneration, 1)
	return r
}

func (w *Reader) Close() {
	// XXX check for negatives
	atomic.AddInt64(&w.c.Readers, -1)
}

// TODO в bool кидать признак закрытия канала
func (r *Reader) Recv() (interface{}, bool) {
checkRC:
	adr := &r.c.RC[r.cur]
	rc := atomic.LoadInt64(adr)
	// Check when generator writing to this row (-1) or the row
	// is empty (0).
	if rc <= 0 {
		//		println("nothing for read") // проверка тут пригодится для закрытия канала
		//		r.c.printDebug()
		if r.waitCount > 100 {
			time.Sleep(100 * time.Nanosecond)
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
	version := r.c.Generation[r.cur]
	if r.version < version {
		r.version = version
		// XXX обновить генерацию
		// XXX обновить значение readers из ch
	}
	result := r.c.Payload[r.cur]
	// The row has been consumed so decrease read count.
	atomic.AddInt64(adr, -1)
	r.cur++
	r.cur %= buflen

	return result, true // XXX
}

func (c *channel) printDebug() {
	c.Lock()
	println()
	for i, rc := range c.RC {
		fmt.Printf("%4d: %5d %v\n", i, rc, c.Payload[i])
	}
	c.Unlock()
}
