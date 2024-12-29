package common

import (
	"math"
	"math/rand"
)

type Distribution interface {
	Advance()
	Get() float64
}

type NormalDistribution struct {
	Mean   float64
	StdDev float64

	value float64
}

func ND(mean, stddev float64) *NormalDistribution {
	return &NormalDistribution{
		Mean:   mean,
		StdDev: stddev,
	}
}

func (d *NormalDistribution) Advance() {
	d.value = rand.NormFloat64()*d.StdDev + d.Mean
}

func (d *NormalDistribution) Get() float64 {
	return d.value
}

type UniformDistribution struct {
	Low  float64
	High float64

	value float64
}

func UD(low, high float64) *UniformDistribution {
	return &UniformDistribution{
		Low:  low,
		High: high,
	}
}

func (d *UniformDistribution) Advance() {
	x := rand.Float64() // uniform
	x *= d.High - d.Low
	x += d.Low
	d.value = x
}

func (d *UniformDistribution) Get() float64 {
	return d.value
}

type RandomWalkDistribution struct {
	Step Distribution

	State float64
}

func WD(step Distribution, state float64) *RandomWalkDistribution {
	return &RandomWalkDistribution{
		Step:  step,
		State: state,
	}
}

func (d *RandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += d.Step.Get()
}

func (d *RandomWalkDistribution) Get() float64 {
	return d.State
}

type ClampedRandomWalkDistribution struct {
	Step Distribution
	Min  float64
	Max  float64

	State float64
}

func CWD(step Distribution, min, max, state float64) *ClampedRandomWalkDistribution {
	return &ClampedRandomWalkDistribution{
		Step: step,
		Min:  min,
		Max:  max,

		State: state,
	}
}

func (d *ClampedRandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += d.Step.Get()
	if d.State > d.Max {
		d.State = d.Max
	}
	if d.State < d.Min {
		d.State = d.Min
	}
}

func (d *ClampedRandomWalkDistribution) Get() float64 {
	return d.State
}

type MonotonicRandomWalkDistribution struct {
	Step  Distribution
	State float64
}

func (d *MonotonicRandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += math.Abs(d.Step.Get())
}

func (d *MonotonicRandomWalkDistribution) Get() float64 {
	return d.State
}

func MWD(step Distribution, state float64) *MonotonicRandomWalkDistribution {
	return &MonotonicRandomWalkDistribution{
		Step:  step,
		State: state,
	}
}

type ConstantDistribution struct {
	State float64
}

func (d *ConstantDistribution) Advance() {
}

func (d *ConstantDistribution) Get() float64 {
	return d.State
}

type FloatPrecision struct {
	step      Distribution
	precision float64
}

func (f *FloatPrecision) Advance() {
	f.step.Advance()
}

func (f *FloatPrecision) Get() float64 {
	return float64(int(f.step.Get()*f.precision)) / f.precision
}

func FP(step Distribution, precision int) *FloatPrecision {
	if precision < 0 {
		precision = 0
	} else if precision > 5 {
		precision = 5
	}
	return &FloatPrecision{
		step:      step,
		precision: math.Pow(10, float64(precision)),
	}
}

type LazyDistribution struct {
	motive    Distribution
	step      Distribution
	threshold float64
}

func LD(motive, dist Distribution, threshold float64) *LazyDistribution {
	return &LazyDistribution{
		step:      dist,
		motive:    motive,
		threshold: threshold,
	}
}

func (d *LazyDistribution) Advance() {
	d.motive.Advance()
	if d.motive.Get() < d.threshold {
		return
	}
	d.step.Advance()
}

func (d *LazyDistribution) Get() float64 {
	return d.step.Get()
}
