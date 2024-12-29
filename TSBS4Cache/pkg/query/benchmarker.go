package query

import (
	"bufio"
	"encoding/json"
	"fmt"
	influxdb_client "github.com/timescale/tsbs/InfluxDB-client/v2"
	"github.com/timescale/tsbs/TimescaleDB_Client/timescaledb_client"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

const (
	labelAllQueries  = "all queries"
	labelColdQueries = "cold queries"
	labelWarmQueries = "warm queries"

	defaultReadSize = 4 << 20
)

type BenchmarkRunnerConfig struct {
	DBName           string `mapstructure:"db-name"`
	Limit            uint64 `mapstructure:"max-queries"`
	LimitRPS         uint64 `mapstructure:"max-rps"`
	MemProfile       string `mapstructure:"memprofile"`
	HDRLatenciesFile string `mapstructure:"hdr-latencies"`
	Workers          uint   `mapstructure:"workers"`
	PrintResponses   bool   `mapstructure:"print-responses"`
	Debug            int    `mapstructure:"debug"`
	FileName         string `mapstructure:"file"`
	BurnIn           uint64 `mapstructure:"burn-in"`
	PrintInterval    uint64 `mapstructure:"print-interval"`
	PrewarmQueries   bool   `mapstructure:"prewarm-queries"`
	ResultsFile      string `mapstructure:"results-file"`

	CacheURL string `mapstructure:"cache-url"`
	UseCache string `mapstructure:"use-cache"`
}

func (c BenchmarkRunnerConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.String("db-name", "benchmark", "Name of database to use for queries")
	fs.Uint64("burn-in", 0, "Number of queries to ignore before collecting statistics.")
	fs.Uint64("max-queries", 0, "Limit the number of queries to send, 0 = no limit")
	fs.Uint64("max-rps", 0, "Limit the rate of queries per second, 0 = no limit")
	fs.Uint64("print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	fs.String("memprofile", "", "Write a memory profile to this file.")
	fs.String("hdr-latencies", "", "Write the High Dynamic Range (HDR) Histogram of Response Latencies to this file.")
	fs.Uint("workers", 1, "Number of concurrent requests to make.")
	fs.Bool("prewarm-queries", false, "Run each query twice in a row so the warm query is guaranteed to be a cache hit")
	fs.Bool("print-responses", false, "Pretty print response bodies for correctness checking (default false).")
	fs.Int("debug", 0, "Whether to print debug messages.")
	fs.String("file", "", "File name to read queries from")
	fs.String("results-file", "", "Write the test results summary json to this file")

	fs.String("cache-url", "http://localhost:11211", "STsCache url")
	fs.String("use-cache", "db", "use STsCache , tscache ,default use database")

}

type BenchmarkRunner struct {
	BenchmarkRunnerConfig
	br      *bufio.Reader
	sp      statProcessor
	scanner *scanner
	ch      chan Query
}

func NewBenchmarkRunner(config BenchmarkRunnerConfig) *BenchmarkRunner {
	runner := &BenchmarkRunner{BenchmarkRunnerConfig: config}
	runner.scanner = newScanner(&runner.Limit)
	spArgs := &statProcessorArgs{
		limit:            &runner.Limit,
		printInterval:    runner.PrintInterval,
		prewarmQueries:   runner.PrewarmQueries,
		burnIn:           runner.BurnIn,
		hdrLatenciesFile: runner.HDRLatenciesFile,
	}

	if strings.EqualFold(influxdb_client.DbName, constants.FormatInflux) {
		influxdb_client.DB = config.DBName

		influxdb_client.UseCache = config.UseCache
		influxdb_client.STsCacheURL = config.CacheURL
		STsCacheURLArr := strings.Split(influxdb_client.STsCacheURL, ",")
		influxdb_client.STsConnArr = influxdb_client.InitStsConnsArr(STsCacheURLArr)
	} else if strings.EqualFold(timescaledb_client.DbName, constants.FormatTimescaleDB) {
		timescaledb_client.DB = config.DBName

		timescaledb_client.UseCache = config.UseCache
		timescaledb_client.STsCacheURL = config.CacheURL
		STsCacheURLArr := strings.Split(timescaledb_client.STsCacheURL, ",")
		timescaledb_client.STsConnArr = timescaledb_client.InitStsConnsArr(STsCacheURLArr)
	}

	runner.sp = newStatProcessor(spArgs)
	return runner
}

func (b *BenchmarkRunner) SetLimit(limit uint64) {
	b.Limit = limit
}

func (b *BenchmarkRunner) DoPrintResponses() bool {
	return b.PrintResponses
}

func (b *BenchmarkRunner) DebugLevel() int {
	return b.Debug
}

func (b *BenchmarkRunner) DatabaseName() string {
	return b.DBName
}

type ProcessorCreate func() Processor

type Processor interface {
	Init(workerNum int)
	ProcessQuery(q Query, isWarm bool) ([]*Stat, error)
}

func (b *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if b.br == nil {
		if len(b.FileName) > 0 {
			file, err := os.Open(b.FileName)
			if err != nil {
				panic(fmt.Sprintf("cannot open file for read %s: %v", b.FileName, err))
			}
			b.br = bufio.NewReaderSize(file, defaultReadSize)
		} else {
			b.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return b.br
}

func (b *BenchmarkRunner) Run(queryPool *sync.Pool, processorCreateFn ProcessorCreate) {
	if b.Workers == 0 {
		panic("must have at least one worker")
	}

	spArgs := b.sp.getArgs()
	if spArgs.burnIn > b.Limit {
		panic("burn-in is larger than limit")
	}
	b.ch = make(chan Query, b.Workers)

	go b.sp.process(b.Workers)

	rateLimiter := getRateLimiter(b.LimitRPS, b.Workers)

	var wg sync.WaitGroup
	for i := 0; i < int(b.Workers); i++ {
		wg.Add(1)
		go b.processorHandler(&wg, rateLimiter, queryPool, processorCreateFn(), i)
	}

	wallStart := time.Now()
	b.scanner.setReader(b.GetBufferedReader()).scan(queryPool, b.ch)
	close(b.ch)

	wg.Wait()
	b.sp.CloseAndWait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	if len(b.MemProfile) > 0 {
		f, err := os.Create(b.MemProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}

	if len(b.BenchmarkRunnerConfig.ResultsFile) > 0 {
		b.saveTestResult(wallTook, wallStart, wallEnd)
	}
}

func (b *BenchmarkRunner) saveTestResult(took time.Duration, start time.Time, end time.Time) {
	testResult := LoaderTestResult{
		ResultFormatVersion: BenchmarkTestResultVersion,
		RunnerConfig:        b.BenchmarkRunnerConfig,
		StartTime:           start.UTC().Unix() * 1000,
		EndTime:             end.UTC().Unix() * 1000,
		DurationMillis:      took.Milliseconds(),
		Totals:              b.sp.GetTotalsMap(),
	}

	_, _ = fmt.Printf("Saving results json file to %s\n", b.BenchmarkRunnerConfig.ResultsFile)
	file, err := json.MarshalIndent(testResult, "", " ")
	if err != nil {
		log.Fatal(err)
	}

	err = ioutil.WriteFile(b.BenchmarkRunnerConfig.ResultsFile, file, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func (b *BenchmarkRunner) processorHandler(wg *sync.WaitGroup, rateLimiter *rate.Limiter, queryPool *sync.Pool, processor Processor, workerNum int) {
	processor.Init(workerNum)
	for query := range b.ch {
		r := rateLimiter.Reserve()
		time.Sleep(r.Delay())

		stats, err := processor.ProcessQuery(query, false)
		if err != nil {
			panic(err)
		}
		b.sp.send(stats)

		spArgs := b.sp.getArgs()
		if spArgs.prewarmQueries {
			stats, err = processor.ProcessQuery(query, true)
			if err != nil {
				panic(err)
			}
			b.sp.sendWarm(stats)
		}
		queryPool.Put(query)
	}
	wg.Done()
}

func getRateLimiter(limitRPS uint64, workers uint) *rate.Limiter {
	var requestRate = rate.Inf
	var requestBurst = 0
	if limitRPS != 0 {
		requestRate = rate.Limit(limitRPS)
		requestBurst = int(workers)
	}
	return rate.NewLimiter(requestRate, requestBurst)
}
