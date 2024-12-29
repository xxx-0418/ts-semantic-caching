package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

var (
	labelMem = []byte("mem")

	memoryTotalChoices = []int64{8 << 30, 12 << 30, 16 << 30}

	memoryFieldKeys = [][]byte{
		[]byte("total"),
		[]byte("available"),
		[]byte("used"),
		[]byte("free"),
		[]byte("cached"),
		[]byte("buffered"),
		[]byte("used_percent"),
		[]byte("available_percent"),
		[]byte("buffered_percent"),
	}
)

type MemMeasurement struct {
	*common.SubsystemMeasurement
	bytesTotal int64
}

func NewMemMeasurement(start time.Time) *MemMeasurement {
	sub := common.NewSubsystemMeasurement(start, 3)
	bytesTotal := common.RandomInt64SliceChoice(memoryTotalChoices)

	nd := common.ND(0.0, float64(bytesTotal)/64)

	sub.Distributions[0] = common.CWD(nd, 0.0, float64(bytesTotal), rand.Float64()*float64(bytesTotal))
	sub.Distributions[1] = common.CWD(nd, 0.0, float64(bytesTotal), rand.Float64()*float64(bytesTotal))
	sub.Distributions[2] = common.CWD(nd, 0.0, float64(bytesTotal), rand.Float64()*float64(bytesTotal))
	return &MemMeasurement{
		SubsystemMeasurement: sub,
		bytesTotal:           bytesTotal,
	}
}

func (m *MemMeasurement) ToPoint(p *data.Point) {
	p.SetMeasurementName(labelMem)
	p.SetTimestamp(&m.Timestamp)

	total := m.bytesTotal
	used := int64(m.Distributions[0].Get())
	cached := int64(m.Distributions[1].Get())
	buffered := int64(m.Distributions[2].Get())
	available := total - int64(used)

	p.AppendField(memoryFieldKeys[0], total)
	p.AppendField(memoryFieldKeys[1], available)
	p.AppendField(memoryFieldKeys[2], used)
	p.AppendField(memoryFieldKeys[3], available)
	p.AppendField(memoryFieldKeys[4], cached)
	p.AppendField(memoryFieldKeys[5], buffered)
	p.AppendField(memoryFieldKeys[6], 100.0*(float64(used)/float64(total)))
	p.AppendField(memoryFieldKeys[7], 100.0*(float64(available)/float64(total)))
	p.AppendField(memoryFieldKeys[8], 100.0*(float64(buffered))/float64(total))
}
