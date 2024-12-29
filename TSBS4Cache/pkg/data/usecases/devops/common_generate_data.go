package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

type HostContext struct {
	id           int
	start        time.Time
	metricCount  uint64
	epochsToLive uint64
}

type commonDevopsSimulatorConfig struct {
	Start           time.Time
	End             time.Time
	InitHostCount   uint64
	HostCount       uint64
	HostConstructor func(ctx *HostContext) Host
	MaxMetricCount  uint64
}

func NewHostCtx(id int, start time.Time) *HostContext {
	return &HostContext{id, start, 0, 0}
}

func NewHostCtxTime(start time.Time) *HostContext {
	return &HostContext{0, start, 0, 0}
}

func calculateEpochs(c commonDevopsSimulatorConfig, interval time.Duration) uint64 {
	return uint64(c.End.Sub(c.Start).Nanoseconds() / interval.Nanoseconds())
}

type commonDevopsSimulator struct {
	madePoints uint64
	maxPoints  uint64

	hostIndex uint64
	hosts     []Host

	epoch      uint64
	epochs     uint64
	epochHosts uint64
	initHosts  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration
}

func (s *commonDevopsSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *commonDevopsSimulator) Fields() map[string][]string {
	if len(s.hosts) <= 0 {
		panic("cannot get fields because no hosts added")
	}
	return s.fields(s.hosts[0].SimulatedMeasurements)
}

func (s *commonDevopsSimulator) TagKeys() []string {
	tagKeysAsStr := make([]string, len(MachineTagKeys))
	for i, t := range MachineTagKeys {
		tagKeysAsStr[i] = string(t)
	}
	return tagKeysAsStr
}

func (s *commonDevopsSimulator) TagTypes() []string {
	types := make([]string, len(MachineTagKeys))
	for i := 0; i < len(MachineTagKeys); i++ {
		types[i] = machineTagType.String()
	}
	return types
}

func (d *commonDevopsSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}
func (s *commonDevopsSimulator) fields(measurements []common.SimulatedMeasurement) map[string][]string {
	fields := make(map[string][]string)
	for _, sm := range measurements {
		point := data.NewPoint()
		sm.ToPoint(point)
		fieldKeys := point.FieldKeys()
		fieldKeysAsStr := make([]string, len(fieldKeys))
		for i, k := range fieldKeys {
			fieldKeysAsStr[i] = string(k)
		}
		fields[string(point.MeasurementName())] = fieldKeysAsStr
	}

	return fields
}

func (s *commonDevopsSimulator) populatePoint(p *data.Point, measureIdx int) bool {
	host := &s.hosts[s.hostIndex]

	p.AppendTag(MachineTagKeys[0], host.Name)
	p.AppendTag(MachineTagKeys[1], host.Region)
	p.AppendTag(MachineTagKeys[2], host.Datacenter)
	p.AppendTag(MachineTagKeys[3], host.Rack)
	p.AppendTag(MachineTagKeys[4], host.OS)
	p.AppendTag(MachineTagKeys[5], host.Arch)
	p.AppendTag(MachineTagKeys[6], host.Team)
	p.AppendTag(MachineTagKeys[7], host.Service)
	p.AppendTag(MachineTagKeys[8], host.ServiceVersion)
	p.AppendTag(MachineTagKeys[9], host.ServiceEnvironment)

	host.SimulatedMeasurements[measureIdx].ToPoint(p)

	ret := s.hostIndex < s.epochHosts
	s.madePoints++
	s.hostIndex++
	return ret
}

func (s *commonDevopsSimulator) adjustNumHostsForEpoch() {
	s.epoch++
	missingScale := float64(uint64(len(s.hosts)) - s.initHosts)
	s.epochHosts = s.initHosts + uint64(missingScale*float64(s.epoch)/float64(s.epochs-1))
}
