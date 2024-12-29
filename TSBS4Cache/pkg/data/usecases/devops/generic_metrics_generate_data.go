package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math"
	"time"
)

type GenericMetricsSimulatorConfig struct {
	*DevopsSimulatorConfig
}

type GenericMetricsSimulator struct {
	*commonDevopsSimulator
}

func (c *GenericMetricsSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	hostInfos := make([]Host, c.HostCount)
	initGenericMetricFields(c.MaxMetricCount)
	hostMetricCount := generateHostMetricCount(c.HostCount, c.MaxMetricCount)
	epochs := calculateEpochs(commonDevopsSimulatorConfig(*c.DevopsSimulatorConfig), interval)
	epochsToLive := generateHostEpochsToLive(c.HostCount, epochs)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = c.HostConstructor(&HostContext{i, c.Start, hostMetricCount[i], epochsToLive[i]})
	}
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		maxPoints = limit
	}
	dg := &GenericMetricsSimulator{
		commonDevopsSimulator: &commonDevopsSimulator{
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
		},
	}

	return dg
}

func (gms *GenericMetricsSimulator) Fields() map[string][]string {
	maxIndex := 0
	for i, h := range gms.hosts {
		if h.GenericMetricCount > gms.hosts[maxIndex].GenericMetricCount {
			maxIndex = i
		}
	}
	return gms.fields(gms.hosts[maxIndex].SimulatedMeasurements[:1])
}

func (gms *GenericMetricsSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagKeys:   gms.TagKeys(),
		TagTypes:  gms.TagTypes(),
		FieldKeys: gms.Fields(),
	}
}

func (gms *GenericMetricsSimulator) Next(p *data.Point) bool {
	if gms.hostIndex >= uint64(len(gms.hosts)) {
		gms.hostIndex = 0
		for _, h := range gms.hosts {
			h.TickAll(gms.interval)
		}
		gms.adjustNumHostsForEpoch()
	}

	if gms.hostIndex < gms.epochHosts {
		host := &gms.hosts[gms.hostIndex]
		if host.StartEpoch == math.MaxUint64 {
			host.StartEpoch = gms.epoch
		}
		if host.EpochsToLive == 0 {
			// found forever living host
			return gms.populatePoint(p, 0)
		}
		if host.StartEpoch+host.EpochsToLive > gms.epoch {
			return gms.populatePoint(p, 0)
		}
	}

	gms.hostIndex++
	gms.madePoints++
	return false
}
