package common

import (
	"github.com/timescale/tsbs/pkg/data"
	"reflect"
	"time"
)

type SimulatorConfig interface {
	NewSimulator(time.Duration, uint64) Simulator
}

type BaseSimulatorConfig struct {
	Start                time.Time
	End                  time.Time
	InitGeneratorScale   uint64
	GeneratorScale       uint64
	GeneratorConstructor func(i int, start time.Time) Generator
}

func calculateEpochs(duration time.Duration, interval time.Duration) uint64 {
	return uint64(duration.Nanoseconds() / interval.Nanoseconds())
}

func (sc *BaseSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) Simulator {
	generators := make([]Generator, sc.GeneratorScale)
	for i := 0; i < len(generators); i++ {
		generators[i] = sc.GeneratorConstructor(i, sc.Start)
	}

	epochs := calculateEpochs(sc.End.Sub(sc.Start), interval)
	maxPoints := epochs * sc.GeneratorScale * uint64(len(generators[0].Measurements()))
	if limit > 0 && limit < maxPoints {
		maxPoints = limit
	}
	sim := &BaseSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		generatorIndex: 0,
		generators:     generators,

		epoch:           0,
		epochs:          epochs,
		epochGenerators: sc.InitGeneratorScale,
		initGenerators:  sc.InitGeneratorScale,
		timestampStart:  sc.Start,
		timestampEnd:    sc.End,
		interval:        interval,

		simulatedMeasurementIndex: 0,
	}

	return sim
}

type GeneratedDataHeaders struct {
	TagTypes  []string
	TagKeys   []string
	FieldKeys map[string][]string
}

type Simulator interface {
	Finished() bool
	Next(*data.Point) bool
	Fields() map[string][]string
	TagKeys() []string
	TagTypes() []string
	Headers() *GeneratedDataHeaders
}

type BaseSimulator struct {
	madePoints uint64
	maxPoints  uint64

	generatorIndex uint64
	generators     []Generator

	epoch           uint64
	epochs          uint64
	epochGenerators uint64
	initGenerators  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration

	simulatedMeasurementIndex int
}

func (s *BaseSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *BaseSimulator) Next(p *data.Point) bool {
	if s.generatorIndex == uint64(len(s.generators)) {
		s.generatorIndex = 0
		s.simulatedMeasurementIndex++
	}

	if s.simulatedMeasurementIndex == len(s.generators[0].Measurements()) {
		s.simulatedMeasurementIndex = 0

		for i := 0; i < len(s.generators); i++ {
			s.generators[i].TickAll(s.interval)
		}

		s.adjustNumHostsForEpoch()
	}

	generator := s.generators[s.generatorIndex]

	for _, tag := range generator.Tags() {
		p.AppendTag(tag.Key, tag.Value)
	}

	generator.Measurements()[s.simulatedMeasurementIndex].ToPoint(p)

	ret := s.generatorIndex < s.epochGenerators
	s.madePoints++
	s.generatorIndex++
	return ret
}

func (s *BaseSimulator) Fields() map[string][]string {
	if len(s.generators) <= 0 {
		panic("cannot get fields because no Generators added")
	}

	toReturn := make(map[string][]string, len(s.generators))
	for _, sm := range s.generators[0].Measurements() {
		point := data.NewPoint()
		sm.ToPoint(point)
		fieldKeys := point.FieldKeys()
		fieldKeysAsStr := make([]string, len(fieldKeys))
		for i, k := range fieldKeys {
			fieldKeysAsStr[i] = string(k)
		}
		toReturn[string(point.MeasurementName())] = fieldKeysAsStr
	}

	return toReturn
}

func (s *BaseSimulator) TagKeys() []string {
	if len(s.generators) <= 0 {
		panic("cannot get tag keys because no Generators added")
	}

	tags := s.generators[0].Tags()
	tagKeys := make([]string, len(tags))
	for i, tag := range tags {
		tagKeys[i] = string(tag.Key)
	}

	return tagKeys
}

func (s *BaseSimulator) TagTypes() []string {
	if len(s.generators) <= 0 {
		panic("cannot get tag types because no Generators added")
	}

	tags := s.generators[0].Tags()
	types := make([]string, len(tags))
	for i, tag := range tags {
		types[i] = reflect.TypeOf(tag.Value).String()
	}

	return types
}

func (s *BaseSimulator) Headers() *GeneratedDataHeaders {
	return &GeneratedDataHeaders{
		TagTypes:  s.TagTypes(),
		TagKeys:   s.TagKeys(),
		FieldKeys: s.Fields(),
	}
}

func (s *BaseSimulator) adjustNumHostsForEpoch() {
	s.epoch++
	missingScale := float64(uint64(len(s.generators)) - s.initGenerators)
	s.epochGenerators = s.initGenerators + uint64(missingScale*float64(s.epoch)/float64(s.epochs-1))
}

type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*data.Point)
}
