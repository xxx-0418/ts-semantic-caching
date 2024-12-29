package query

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type statProcessor interface {
	getArgs() *statProcessorArgs
	send(stats []*Stat)
	sendWarm(stats []*Stat)
	process(workers uint)
	CloseAndWait()
	GetTotalsMap() map[string]interface{}
}

type statProcessorArgs struct {
	prewarmQueries   bool
	limit            *uint64
	burnIn           uint64
	printInterval    uint64
	hdrLatenciesFile string
}

type defaultStatProcessor struct {
	args               *statProcessorArgs
	wg                 sync.WaitGroup
	c                  chan *Stat
	opsCount           uint64
	startTime          time.Time
	endTime            time.Time
	statMapping        map[string]*statGroup
	totalByteLength    uint64
	prevByteLength     uint64
	totalFullyGetNum   uint64
	prevFullyGetNum    uint64
	totalPartialGetNum uint64
	prevPartialGetNum  uint64
}

func newStatProcessor(args *statProcessorArgs) statProcessor {
	if args == nil {
		panic("Stat Processor needs args")
	}
	return &defaultStatProcessor{args: args}
}

func (sp *defaultStatProcessor) getArgs() *statProcessorArgs {
	return sp.args
}

func (sp *defaultStatProcessor) send(stats []*Stat) {
	if stats == nil {
		return
	}

	for _, s := range stats {
		sp.c <- s
	}
}

func (sp *defaultStatProcessor) sendWarm(stats []*Stat) {
	if stats == nil {
		return
	}

	for _, s := range stats {
		s.isWarm = true
	}
	sp.send(stats)
}

func (sp *defaultStatProcessor) process(workers uint) {
	sp.c = make(chan *Stat, workers)
	sp.wg.Add(1)
	const allQueriesLabel = labelAllQueries
	sp.statMapping = map[string]*statGroup{
		allQueriesLabel: newStatGroup(*sp.args.limit),
	}
	if sp.args.prewarmQueries {
		sp.statMapping[labelColdQueries] = newStatGroup(*sp.args.limit)
		sp.statMapping[labelWarmQueries] = newStatGroup(*sp.args.limit)
	}

	i := uint64(0)
	sp.startTime = time.Now()
	prevTime := sp.startTime
	prevRequestCount := uint64(0)

	for stat := range sp.c {
		atomic.AddUint64(&sp.opsCount, 1)
		if i > sp.args.burnIn {
			atomic.AddUint64(&sp.totalByteLength, stat.byteLength)
			if stat.hitKind == 2 {
				atomic.AddUint64(&sp.totalFullyGetNum, 1)
			} else if stat.hitKind == 1 {
				atomic.AddUint64(&sp.totalPartialGetNum, 1)
			}
		}

		if i < sp.args.burnIn {
			i++
			statPool.Put(stat)
			continue
		} else if i == sp.args.burnIn && sp.args.burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", sp.args.burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}

			sp.startTime = time.Now()
			prevTime = sp.startTime
			prevRequestCount = sp.args.burnIn
		}
		if _, ok := sp.statMapping[string(stat.label)]; !ok {
			sp.statMapping[string(stat.label)] = newStatGroup(*sp.args.limit)
		}

		sp.statMapping[string(stat.label)].push(stat.value)

		if !stat.isPartial {
			sp.statMapping[allQueriesLabel].push(stat.value)

			if sp.args.prewarmQueries {
				if stat.isWarm {
					sp.statMapping[labelWarmQueries].push(stat.value)
				} else {
					sp.statMapping[labelColdQueries].push(stat.value)
				}
			}

			if !sp.args.prewarmQueries || !stat.isWarm {
				i++
			}
		}

		statPool.Put(stat)

		if sp.args.printInterval > 0 && i > 0 && i%sp.args.printInterval == 0 && (i < *sp.args.limit || *sp.args.limit == 0) {
			now := time.Now()
			sinceStart := now.Sub(sp.startTime)
			took := now.Sub(prevTime)

			intervalQueryRate := float64(sp.opsCount-prevRequestCount) / float64(took.Seconds())
			overallQueryRate := float64(sp.opsCount-sp.args.burnIn) / float64(sinceStart.Seconds())

			intervalFullyGetNum := sp.totalFullyGetNum - sp.prevFullyGetNum
			overallFullyGetNum := sp.totalFullyGetNum
			intervalPartiallyGetNum := sp.totalPartialGetNum - sp.prevPartialGetNum
			overallPartiallyGetNum := sp.totalPartialGetNum

			intervalFullyGetRate := float64(intervalFullyGetNum) / float64(sp.opsCount-prevRequestCount)
			overallFullyGetRate := float64(sp.totalFullyGetNum) / float64(i-sp.args.burnIn)
			intervalPartiallyGetRate := float64(intervalPartiallyGetNum) / float64(sp.opsCount-prevRequestCount)
			overallPartiallyGetRate := float64(sp.totalPartialGetNum) / float64(i-sp.args.burnIn)

			_, err := fmt.Fprintf(os.Stderr, "After %d queries with %d workers:\n\tInterval query rate: %0.4f queries/sec\tOverall query rate: %0.4f queries/sec\n",
				i-sp.args.burnIn,
				workers,
				intervalQueryRate,
				overallQueryRate,
			)
			_, err = fmt.Fprintf(os.Stderr, "\tInterval fully get number: %d \t\t\t Overall fully get number: %d\n",
				intervalFullyGetNum,
				overallFullyGetNum,
			)
			_, err = fmt.Fprintf(os.Stderr, "\tInterval fully get rate: %0.4f \t\t Overall fully get rate: %0.4f \n",
				intervalFullyGetRate,
				overallFullyGetRate,
			)
			_, err = fmt.Fprintf(os.Stderr, "\tInterval partially get number: %d \t\t Overall partially get number: %d\n",
				intervalPartiallyGetNum,
				overallPartiallyGetNum,
			)
			_, err = fmt.Fprintf(os.Stderr, "\tInterval partially get rate: %0.4f \t\t Overall partially get rate: %0.4f \n",
				intervalPartiallyGetRate,
				overallPartiallyGetRate,
			)

			if err != nil {
				log.Fatal(err)
			}
			err = writeStatGroupMap(os.Stderr, sp.statMapping)
			if err != nil {
				log.Fatal(err)
			}
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
			prevRequestCount = sp.opsCount
			sp.prevByteLength = sp.totalByteLength
			sp.prevFullyGetNum = sp.totalFullyGetNum
			sp.prevPartialGetNum = sp.totalPartialGetNum

			prevTime = now
		}
	}
	sinceStart := time.Now().Sub(sp.startTime)
	overallQueryRate := float64(sp.opsCount-sp.args.burnIn) / float64(sinceStart.Seconds())
	overallFullyGetNum := sp.totalFullyGetNum
	overallPartiallyGetNum := sp.totalPartialGetNum
	overallFullyGetRate := float64(sp.totalFullyGetNum) / float64(i-sp.args.burnIn)
	overallPartiallyGetRate := float64(sp.totalPartialGetNum) / float64(i-sp.args.burnIn)

	_, err := fmt.Printf("Run complete after %d queries with %d workers \n\t\t(Overall query rate %0.4f queries/sec)\n"+
		"\t\t(Overall fully get number %d )\n\t\t(Overall fully get rate %0.4f )\n\t\t(Overall partially get number %d )\n\t\t(Overall partially get rate %0.4f )\n",
		i-sp.args.burnIn, workers, overallQueryRate, overallFullyGetNum, overallFullyGetRate, overallPartiallyGetNum, overallPartiallyGetRate)

	if err != nil {
		log.Fatal(err)
	}
	err = writeStatGroupMap(os.Stdout, sp.statMapping)
	if err != nil {
		log.Fatal(err)
	}

	if len(sp.args.hdrLatenciesFile) > 0 {
		_, _ = fmt.Printf("Saving High Dynamic Range (HDR) Histogram of Response Latencies to %s\n", sp.args.hdrLatenciesFile)
		var b bytes.Buffer
		bw := bufio.NewWriter(&b)
		_, err = sp.statMapping[allQueriesLabel].latencyHDRHistogram.PercentilesPrint(bw, 10, 1000.0)
		if err != nil {
			log.Fatal(err)
		}
		err = ioutil.WriteFile(sp.args.hdrLatenciesFile, b.Bytes(), 0644)
		if err != nil {
			log.Fatal(err)
		}

	}

	sp.wg.Done()
}

func generateQuantileMap(hist *hdrhistogram.Histogram) (int64, map[string]float64) {
	ops := hist.TotalCount()
	q0 := 0.0
	q50 := 0.0
	q95 := 0.0
	q99 := 0.0
	q999 := 0.0
	q100 := 0.0
	if ops > 0 {
		q0 = float64(hist.ValueAtQuantile(0.0)) / 10e2
		q50 = float64(hist.ValueAtQuantile(50.0)) / 10e2
		q95 = float64(hist.ValueAtQuantile(95.0)) / 10e2
		q99 = float64(hist.ValueAtQuantile(99.0)) / 10e2
		q999 = float64(hist.ValueAtQuantile(99.90)) / 10e2
		q100 = float64(hist.ValueAtQuantile(100.0)) / 10e2
	}

	mp := map[string]float64{"q0": q0, "q50": q50, "q95": q95, "q99": q99, "q999": q999, "q100": q100}
	return ops, mp
}

func (sp *defaultStatProcessor) GetTotalsMap() map[string]interface{} {
	totals := make(map[string]interface{})
	totals["prewarmQueries"] = sp.args.prewarmQueries
	totals["limit"] = sp.args.limit
	totals["burnIn"] = sp.args.burnIn
	sinceStart := time.Now().Sub(sp.startTime)
	queryRates := make(map[string]interface{})
	for label, statGroup := range sp.statMapping {
		overallQueryRate := float64(statGroup.count) / sinceStart.Seconds()
		queryRates[stripRegex(label)] = overallQueryRate
	}
	totals["overallQueryRates"] = queryRates
	quantiles := make(map[string]interface{})
	for label, statGroup := range sp.statMapping {
		_, all := generateQuantileMap(statGroup.latencyHDRHistogram)
		quantiles[stripRegex(label)] = all
	}
	totals["overallQuantiles"] = quantiles
	return totals
}

func stripRegex(in string) string {
	reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
	return reg.ReplaceAllString(in, "_")
}

func (sp *defaultStatProcessor) CloseAndWait() {
	close(sp.c)
	sp.wg.Wait()
}
