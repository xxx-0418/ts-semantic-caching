package query

import (
	"fmt"
)

type Query interface {
	Release()
	HumanLabelName() []byte
	HumanDescriptionName() []byte
	GetID() uint64
	SetID(uint64)
	fmt.Stringer
}
