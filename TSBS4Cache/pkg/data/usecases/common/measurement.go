package common

import (
	"github.com/timescale/tsbs/pkg/data"
	"time"
)

type SubsystemMeasurement struct {
	Timestamp     time.Time
	Distributions []Distribution
}

func NewSubsystemMeasurement(start time.Time, numDistributions int) *SubsystemMeasurement {
	return &SubsystemMeasurement{
		Timestamp:     start,
		Distributions: make([]Distribution, numDistributions),
	}
}

func NewSubsystemMeasurementWithDistributionMakers(start time.Time, makers []LabeledDistributionMaker) *SubsystemMeasurement {
	m := NewSubsystemMeasurement(start, len(makers))
	for i := 0; i < len(makers); i++ {
		m.Distributions[i] = makers[i].DistributionMaker()
	}
	return m
}

func (m *SubsystemMeasurement) Tick(d time.Duration) {
	m.Timestamp = m.Timestamp.Add(d)
	for i := range m.Distributions {
		m.Distributions[i].Advance()
	}
}

func (m *SubsystemMeasurement) ToPoint(p *data.Point, measurementName []byte, labels []LabeledDistributionMaker) {
	p.SetMeasurementName(measurementName)
	p.SetTimestamp(&m.Timestamp)

	for i, d := range m.Distributions {
		p.AppendField(labels[i].Label, d.Get())
	}
}

func (m *SubsystemMeasurement) ToPointAllInt64(p *data.Point, measurementName []byte, labels []LabeledDistributionMaker) {
	p.SetMeasurementName(measurementName)
	p.SetTimestamp(&m.Timestamp)

	for i, d := range m.Distributions {
		p.AppendField(labels[i].Label, int64(d.Get()))
	}
}

type LabeledDistributionMaker struct {
	Label             []byte
	DistributionMaker func() Distribution
}
