package broadcast

import (
	"fmt"
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
		cur   = atomic.LoadInt64(&c.WriteTo)
		rcadr = &c.RC[cur]
	)
waitForReaders:
	// If data has been read by all workers then the RC must
	// be zero. For clarity generator set it to -1. It will
	// prevent this row from reading by the workers.
	rc := atomic.LoadInt64(rcadr)
	if !atomic.CompareAndSwapInt64(rcadr, 0, -1) {
		if rc == -1 {
			// The row already has been taken by another writer.
			cur++
			cur = cur % buflen
			rcadr = &c.RC[cur]
			atomic.StoreInt64(&c.WriteTo, cur)
			//			println("taken", rc, cur)
			atomic.StoreInt64(&c.WaitReaderCount, 0)
		} else {
			// The row hasn't read yet. So the generator waits for all
			// the consumers read it.
			pause := atomic.LoadInt64(&c.WaitReaderCount)
			if pause > 100 {
				time.Sleep(100 * time.Nanosecond)
				rc := atomic.LoadInt64(rcadr)
				// XXX
				println("here big", rc, pause, c.WriteTo, data.(string))
				c.printDebug(cur, data)
				//				os.Exit(1)
				//				return // XXX
				goto waitForReaders
			}
			atomic.AddInt64(&c.WaitReaderCount, 1)
			//			rc := atomic.LoadInt64(adr)
			time.Sleep(time.Duration(pause))
			//			println("here", rc, pause, data.(string)) // XXX
		}
		goto waitForReaders
	}
	// The row protected by Rcount=-1 here.
	// So it can safely assign a new value here.
	c.Payload[cur] = data
	// Reinitialize RC for enable reading again: set it to the number of workers.
	cur++
	atomic.StoreInt64(&c.WriteTo, cur%buflen)
	atomic.StoreInt64(rcadr, atomic.LoadInt64(&c.Readers))
	c.printDebug(cur%buflen, data)
}

var readers []*Reader

func (c *channel) AddReader() *Reader {
	var r = new(Reader)
	r.c = c
	println(atomic.AddInt64(&c.Readers, 1))
	r.version = atomic.AddUint64(&c.GlobalGeneration, 1)

	// XXX
	readers = append(readers, r)
	return r
}

type Reader struct {
	c         *channel
	cur       int64
	version   uint64
	waitCount int
}

func (w *Reader) Close() {
	// XXX check for negatives
	atomic.AddInt64(&w.c.Readers, -1)
}

// TODO в bool кидать признак закрытия канала
func (r *Reader) Recv() (interface{}, bool) {
checkRC:
	adr := &(r.c.RC[r.cur])
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
	// version := r.c.Generation[r.cur]
	// if r.version < version {
	// 	r.version = version
	// 	// XXX обновить генерацию
	// 	// XXX бновить значение readers из ch
	// }
	result := r.c.Payload[r.cur]
	// The row has been consumed so decrease read count.
	atomic.AddInt64(adr, -1)
	cur := r.cur
	r.cur++
	r.cur %= buflen
	r.c.printDebug(0, nil)
	return fmt.Sprintf("%s(%d)", result, cur), true // XXX
}

func (c *channel) printDebug(cur int64, val interface{}) {
	c.Lock()
	println("Ringbuf:")
	for i, rc := range c.RC {
		fmt.Printf("%4d: %5d %v\n", i, rc, c.Payload[i])
	}
	println("Readers:")
	for i, r := range readers {
		fmt.Printf("%4d <- %5d\n", i, r.cur)
	}
	println("Writer:")
	println(c.WriteTo)
	if val != nil {
		println(cur, val.(string))
	}
	c.Unlock()
}
