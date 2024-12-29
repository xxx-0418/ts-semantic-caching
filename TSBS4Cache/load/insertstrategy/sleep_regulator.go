package insertstrategy

import (
	"fmt"
	"math/rand"
	"time"
)

type nowProviderFn func() time.Time
type generateSleepTimeFn func() time.Duration

type SleepRegulator interface {
	Sleep(workerNum int, startedWorkAt time.Time)
}

type noWait struct{}

func NoWait() SleepRegulator {
	return &noWait{}
}

func (n *noWait) Sleep(workerNum int, startedWorkAt time.Time) {
}

type sleepRegulator struct {
	sleepTimes map[int]generateSleepTimeFn
	nowFn      nowProviderFn
}

func NewSleepRegulator(insertIntervalString string, numWorkers int, initialRand *rand.Rand) (SleepRegulator, error) {
	if numWorkers <= 0 {
		return nil, fmt.Errorf("number of workers must be positive, can't be %d", numWorkers)
	}

	sleepTimes, err := parseInsertIntervalString(insertIntervalString, numWorkers, initialRand)
	if err != nil {
		return nil, err
	}

	return &sleepRegulator{
		sleepTimes: sleepTimes,
		nowFn:      time.Now,
	}, nil
}

func (s *sleepRegulator) Sleep(workerNum int, startedWorkAt time.Time) {
	sleepGenerator, ok := s.sleepTimes[workerNum]
	if !ok {
		panic(fmt.Sprintf("invalid worker number: %d", workerNum))
	}

	timeToSleep := sleepGenerator()

	shouldSleepUntil := startedWorkAt.Add(timeToSleep)
	now := s.nowFn()
	if !shouldSleepUntil.After(now) {
		return
	}
	durationToSleep := shouldSleepUntil.Sub(now)
	time.Sleep(durationToSleep)
}
