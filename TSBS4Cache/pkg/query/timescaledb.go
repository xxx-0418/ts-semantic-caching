package query

import (
	"fmt"
	"sync"
)

type TimescaleDB struct {
	HumanLabel       []byte
	HumanDescription []byte

	Hypertable []byte
	SqlQuery   []byte
	id         uint64
}

var TimescaleDBPool = sync.Pool{
	New: func() interface{} {
		return &TimescaleDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Hypertable:       make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

func NewTimescaleDB() *TimescaleDB {
	return TimescaleDBPool.Get().(*TimescaleDB)
}

func (q *TimescaleDB) GetID() uint64 {
	return q.id
}

func (q *TimescaleDB) SetID(n uint64) {
	q.id = n
}

func (q *TimescaleDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Hypertable: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Hypertable, q.SqlQuery)
}

func (q *TimescaleDB) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *TimescaleDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *TimescaleDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Hypertable = q.Hypertable[:0]
	q.SqlQuery = q.SqlQuery[:0]

	TimescaleDBPool.Put(q)
}
