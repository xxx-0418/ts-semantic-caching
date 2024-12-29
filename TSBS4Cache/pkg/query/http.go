package query

import (
	"fmt"
	"sync"
)

type HTTP struct {
	HumanLabel       []byte
	HumanDescription []byte
	Method           []byte
	Path             []byte
	Body             []byte
	RawQuery         []byte
	StartTimestamp   int64
	EndTimestamp     int64
	id               uint64
}

var HTTPPool = sync.Pool{
	New: func() interface{} {
		return &HTTP{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			Method:           []byte{},
			Path:             []byte{},
			Body:             []byte{},
			RawQuery:         []byte{},
			StartTimestamp:   0,
			EndTimestamp:     0,
		}
	},
}

func NewHTTP() *HTTP {
	return HTTPPool.Get().(*HTTP)
}

func (q *HTTP) GetID() uint64 {
	return q.id
}

func (q *HTTP) SetID(n uint64) {
	q.id = n
}

func (q *HTTP) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Method: \"%s\", Path: \"%s\", Body: \"%s\"", q.HumanLabel, q.HumanDescription, q.Method, q.Path, q.Body)
}

func (q *HTTP) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *HTTP) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *HTTP) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.Method = q.Method[:0]
	q.Path = q.Path[:0]
	q.Body = q.Body[:0]
	q.StartTimestamp = 0
	q.EndTimestamp = 0

	HTTPPool.Put(q)
}
