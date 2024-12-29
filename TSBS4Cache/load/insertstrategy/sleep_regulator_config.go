package insertstrategy

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	workerSleepUnit     = time.Second
	intervalSeparator   = ","
	rangeSeparator      = "-"
	intervalFormatError = "worker insert interval could not be parsed as integer constant or range. Required: 'x' or 'x-y' | x,y are uint x<y"
)

func parseInsertIntervalString(insertIntervalString string, numWorkers int, initialRand *rand.Rand) (map[int]generateSleepTimeFn, error) {
	randsPerWorker := makeRandsForWorkers(numWorkers, initialRand)
	splitIntervals := splitIntervalString(insertIntervalString)
	numIntervals := len(splitIntervals)
	sleepGenerators := make(map[int]generateSleepTimeFn)
	currentInterval := 0
	var err error

	for i := 0; i < numWorkers; i++ {
		intervalToParse := splitIntervals[currentInterval]
		sleepGenerators[i], err = parseSingleIntervalString(intervalToParse, randsPerWorker[i])
		if err != nil {
			return nil, err
		}

		if currentInterval < numIntervals-1 {
			currentInterval++
		}
	}

	return sleepGenerators, nil
}

func parseSingleIntervalString(rangeStr string, randForWorker *rand.Rand) (generateSleepTimeFn, error) {
	if number, err := strconv.Atoi(rangeStr); err == nil {
		return newConstantSleepTimeGenerator(number), nil
	}

	if numbers, err := attemptRangeParse(rangeStr); err == nil {
		return newRangeSleepTimeGenerator(numbers[0], numbers[1], randForWorker), nil
	}

	return nil, errors.New(intervalFormatError)
}

func attemptRangeParse(rangeString string) ([]int, error) {
	parts := strings.SplitN(rangeString, rangeSeparator, 2)
	if len(parts) != 2 {
		return nil, errors.New(intervalFormatError)
	}

	var first, second int
	var err error
	if first, err = strconv.Atoi(parts[0]); err != nil {
		return nil, errors.New(intervalFormatError)
	}

	if second, err = strconv.Atoi(parts[1]); err != nil {
		return nil, errors.New(intervalFormatError)
	}

	if first >= second {
		return nil, errors.New(intervalFormatError)
	}

	return []int{first, second}, nil
}

func splitIntervalString(insertIntervalString string) []string {
	if insertIntervalString == "" {
		return []string{"0"}
	}
	return strings.Split(insertIntervalString, intervalSeparator)
}

func makeRandsForWorkers(num int, initialRand *rand.Rand) []*rand.Rand {
	toReturn := make([]*rand.Rand, num)
	for i := 0; i < num; i++ {
		seed := initialRand.Int63()
		src := rand.NewSource(seed)
		toReturn[i] = rand.New(src)
	}

	return toReturn
}

func newConstantSleepTimeGenerator(maxSleepTime int) generateSleepTimeFn {
	maxSleepDuration := time.Duration(maxSleepTime) * workerSleepUnit
	return func() time.Duration {
		return maxSleepDuration
	}
}

func newRangeSleepTimeGenerator(minSleepTime, maxSleepTime int, randToUse *rand.Rand) generateSleepTimeFn {
	if randToUse == nil {
		panic("random number generator passed to range sleep generator was nil")
	}
	return func() time.Duration {
		sleep := minSleepTime + randToUse.Intn(maxSleepTime-minSleepTime)
		return time.Duration(sleep) * workerSleepUnit
	}
}
