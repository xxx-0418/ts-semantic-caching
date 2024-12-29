package load

import (
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
	"sync/atomic"
	"time"
)

type noFlowBenchmarkRunner struct {
	CommonBenchmarkRunner
}

func (l *noFlowBenchmarkRunner) RunBenchmark(b targets.Benchmark) {
	wg, start := l.preRun(b)

	var numChannels uint
	if l.HashWorkers {
		numChannels = l.Workers
	} else {
		numChannels = 1
	}
	channels := l.createChannels(numChannels, l.ChannelCapacity)

	for i := uint(0); i < l.Workers; i++ {
		go l.work(b, wg, channels[i%numChannels], i)
	}
	scanWithoutFlowControl(b.GetDataSource(), b.GetPointIndexer(numChannels), b.GetBatchFactory(), channels, l.BatchSize, l.Limit)
	for _, c := range channels {
		close(c)
	}
	l.postRun(wg, start)
}

func (l *noFlowBenchmarkRunner) createChannels(numChannels, capacity uint) []chan targets.Batch {
	channels := make([]chan targets.Batch, numChannels)
	for i := uint(0); i < numChannels; i++ {
		channels[i] = make(chan targets.Batch, capacity)
	}
	return channels
}

func (l *noFlowBenchmarkRunner) work(b targets.Benchmark, wg *sync.WaitGroup, c <-chan targets.Batch, workerNum uint) {
	proc := b.GetProcessor()
	proc.Init(int(workerNum), l.DoLoad, l.HashWorkers)

	for batch := range c {
		startedWorkAt := time.Now()
		metricCnt, rowCnt := proc.ProcessBatch(batch, l.DoLoad)
		atomic.AddUint64(&l.metricCnt, metricCnt)
		atomic.AddUint64(&l.rowCnt, rowCnt)
		l.timeToSleep(workerNum, startedWorkAt)
	}

	switch c := proc.(type) {
	case targets.ProcessorCloser:
		c.Close(l.DoLoad)
	}

	wg.Done()
}
