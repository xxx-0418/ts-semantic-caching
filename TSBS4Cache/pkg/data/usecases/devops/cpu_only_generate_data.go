package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

type CPUOnlySimulator struct {
	*commonDevopsSimulator
}

func (d *CPUOnlySimulator) Fields() map[string][]string {
	return d.fields(d.hosts[0].SimulatedMeasurements[:1])
}

func (d *CPUOnlySimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}

func (d *CPUOnlySimulator) Next(p *data.Point) bool {
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
		}

		d.adjustNumHostsForEpoch()
	}

	return d.populatePoint(p, 0)
}

type CPUOnlySimulatorConfig commonDevopsSimulatorConfig

func (c *CPUOnlySimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	hostInfos := make([]Host, c.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = c.HostConstructor(NewHostCtx(i, c.Start))
	}

	epochs := calculateEpochs(commonDevopsSimulatorConfig(*c), interval)
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		maxPoints = limit
	}
	sim := &CPUOnlySimulator{&commonDevopsSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		epoch:          0,
		epochs:         epochs,
		epochHosts:     c.InitHostCount,
		initHosts:      c.InitHostCount,
		timestampStart: c.Start,
		timestampEnd:   c.End,
		interval:       interval,
	}}

	return sim
}
