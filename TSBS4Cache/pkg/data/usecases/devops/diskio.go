package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

const (
	diskSerialFmt = "%03d-%03d-%03d"
)

var (
	labelDiskIO       = []byte("diskio")
	labelDiskIOSerial = []byte("serial")

	opsND   = common.ND(50, 1)
	bytesND = common.ND(100, 1)
	timeND  = common.ND(5, 1)

	diskIOFields = []common.LabeledDistributionMaker{
		{Label: []byte("reads"), DistributionMaker: func() common.Distribution { return common.MWD(opsND, 0) }},
		{Label: []byte("writes"), DistributionMaker: func() common.Distribution { return common.MWD(opsND, 0) }},
		{Label: []byte("read_bytes"), DistributionMaker: func() common.Distribution { return common.MWD(bytesND, 0) }},
		{Label: []byte("write_bytes"), DistributionMaker: func() common.Distribution { return common.MWD(bytesND, 0) }},
		{Label: []byte("read_time"), DistributionMaker: func() common.Distribution { return common.MWD(timeND, 0) }},
		{Label: []byte("write_time"), DistributionMaker: func() common.Distribution { return common.MWD(timeND, 0) }},
		{Label: []byte("io_time"), DistributionMaker: func() common.Distribution { return common.MWD(timeND, 0) }},
	}
)

type DiskIOMeasurement struct {
	*common.SubsystemMeasurement
	serial string
}

func NewDiskIOMeasurement(start time.Time) *DiskIOMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, diskIOFields)
	serial := fmt.Sprintf(diskSerialFmt, rand.Intn(1000), rand.Intn(1000), rand.Intn(1000))
	return &DiskIOMeasurement{
		SubsystemMeasurement: sub,
		serial:               serial,
	}
}

func (m *DiskIOMeasurement) ToPoint(p *data.Point) {
	m.ToPointAllInt64(p, labelDiskIO, diskIOFields)
	p.AppendTag(labelDiskIOSerial, m.serial)
}
