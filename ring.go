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

		Readers         int64
		WaitReaderCount int64
		NextIdx         uint64
		Generation      [buflen]uint64 // incremented on each write
		RL              [buflen]int64  // reads left
		Payload         [buflen]interface{}
	}
)

func New() *channel {
	c := new(channel)
	for i, _ := range c.Generation {
		c.Generation[i] = 1
	}
	return c
}

func (c *channel) Send(data interface{}) {
	var (
		cur   = atomic.LoadUint64(&c.NextIdx)
		rladr = &c.RL[cur]
	)
waitForReaders:
	// If data has been read by all workers then the RL must
	// be zero. For clarity generator set it to -1. It will
	// prevent this row from reading by the workers.
	rl := atomic.LoadInt64(rladr)
	if !atomic.CompareAndSwapInt64(rladr, 0, -1) {
		if rl == -1 {
			// The row already has been taken by another writer.
			cur++
			cur = cur % buflen
			rladr = &c.RL[cur]
			atomic.StoreUint64(&c.NextIdx, cur)
			//			println("taken", rl, cur)
			atomic.StoreInt64(&c.WaitReaderCount, 0)
		} else {
			// TODO возвращать ошибку здесь, вместо паузы - return err
			// The row hasn't read yet. So the generator waits for all
			// the consumers read it.
			pause := atomic.LoadInt64(&c.WaitReaderCount)
			if pause > 100 {
				time.Sleep(100 * time.Nanosecond)
				rl := atomic.LoadInt64(rladr)
				// XXX
				println("here big", rl, pause, c.NextIdx, data.(string))
				c.printDebug(cur, data)
				//				os.Exit(1)
				//				return // XXX
				goto waitForReaders
			}
			atomic.AddInt64(&c.WaitReaderCount, 1)
			//			rl := atomic.LoadInt64(adr)
			time.Sleep(time.Duration(pause))
			//			println("here", rl, pause, data.(string)) // XXX
		}
		goto waitForReaders
	}
	// The row protected by Rcount=-1 here.
	// So it can safely assign a new value here.
	c.Payload[cur] = data
	atomic.AddUint64(&c.Generation[cur], 1)
	cur++
	atomic.StoreUint64(&c.NextIdx, cur%buflen)
	// Reinitialize RL for enable reading again: set it to the number of readers.
	atomic.StoreInt64(rladr, atomic.LoadInt64(&c.Readers))
	c.printDebug(cur%buflen, data)
}

var readers []*Reader

func (c *channel) AddReader() *Reader {
	var r = new(Reader)
	r.c = c
	r.cur = atomic.LoadUint64(&c.NextIdx)
	r.from = (r.cur - 1) % buflen

	// XXX для отладки
	readers = append(readers, r)
	println(atomic.AddInt64(&c.Readers, 1))

	return r
}

type Reader struct {
	c         *channel
	from      uint64
	cur       uint64
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
	rladr := &(r.c.RL[r.cur])
	rl := atomic.LoadInt64(rladr)
	// Check when generator writing to this row (-1) or the row
	// is empty (0).
	if rl <= 0 {
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
	// RL>0 so any writer will not touch it
	var generation = r.c.Generation[r.cur]
	if r.cur == r.from {
		if r.version > generation {
			// the reader has read this line so wait for another generation
			r.waitCount++
			time.Sleep(time.Duration(r.waitCount))
			goto checkRC
			//			return nil, false
		}
		r.version = generation
		r.from++
		r.from %= buflen
	}
	r.waitCount = 0
	// RL should still be > 0 in this place.
	// Generator will wait until RL=0 so it can be safely read
	// from the row here.
	result := r.c.Payload[r.cur]
	// The row has been consumed so decrease read count.
	atomic.AddInt64(rladr, -1)
	cur := r.cur
	r.cur++
	r.cur %= buflen
	r.c.printDebug(0, nil)
	return fmt.Sprintf("%s(%d)", result, cur), true // XXX
}

func (c *channel) printDebug(cur uint64, val interface{}) {
	c.Lock()
	println("Ringbuf:")
	for i, rl := range c.RL {
		fmt.Printf("%4d: %5d g:%d %v\n", i, rl, c.Generation[i], c.Payload[i])
	}
	println("Readers:")
	for i, r := range readers {
		fmt.Printf("%4d <- v:%d %5d %5d\n", i, r.version, r.cur, r.from)
	}
	println("Writer:")
	println(c.NextIdx)
	if val != nil {
		println(cur, val.(string))
	}
	c.Unlock()
}
