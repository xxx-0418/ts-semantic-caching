package query

import (
	"encoding/gob"
	"io"
	"log"
	"sync"
)

type scanner struct {
	r     io.Reader
	limit *uint64
}

func newScanner(limit *uint64) *scanner {
	return &scanner{limit: limit}
}

func (s *scanner) setReader(r io.Reader) *scanner {
	s.r = r
	return s
}

func (s *scanner) scan(pool *sync.Pool, c chan Query) {
	decoder := gob.NewDecoder(s.r)

	n := uint64(0)
	for {
		if *s.limit > 0 && n >= *s.limit {
			break
		}

		q := pool.Get().(Query)
		err := decoder.Decode(q)
		if err == io.EOF {
			// EOF, all done
			break
		}
		if err != nil {
			// Can't read, time to quit
			log.Fatal(err)
		}
		q.SetID(n)
		c <- q
		n++
	}
}
