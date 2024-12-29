package inputs

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"github.com/timescale/tsbs/zipfian/counter"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	queryUtils "github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalUtils "github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/query/config"
	"github.com/timescale/tsbs/pkg/query/factories"

	"github.com/timescale/tsbs/zipfian/distributionGenerator"
)

const (
	errBadQueryTypeFmt          = "invalid query type for use case '%s': '%s'"
	errCouldNotDebugFmt         = "could not write debug output: %v"
	errCouldNotEncodeQueryFmt   = "could not encode query: %v"
	errCouldNotQueryStatsFmt    = "could not output query stats: %v"
	errUseCaseNotImplementedFmt = "use case '%s' not implemented for format '%s'"
	errInvalidFactory           = "query generator factory for database '%s' does not implement the correct interface"
	errUnknownUseCaseFmt        = "use case '%s' is undefined"
	errCannotParseTimeFmt       = "cannot parse time from string '%s': %v"
	errBadUseFmt                = "invalid use case specified: '%v'"
)

type DevopsGeneratorMaker interface {
	NewDevops(start, end time.Time, scale int) (queryUtils.QueryGenerator, error)
}

type IoTGeneratorMaker interface {
	NewIoT(start, end time.Time, scale int) (queryUtils.QueryGenerator, error)
}

type QueryGenerator struct {
	Out      io.Writer
	DebugOut io.Writer

	conf          *config.QueryGeneratorConfig
	useCaseMatrix map[string]map[string]queryUtils.QueryFillerMaker
	factories     map[string]interface{}
	tsStart       time.Time
	tsEnd         time.Time
	bufOut        *bufio.Writer
}

func NewQueryGenerator(useCaseMatrix map[string]map[string]queryUtils.QueryFillerMaker) *QueryGenerator {
	return &QueryGenerator{
		useCaseMatrix: useCaseMatrix,
		factories:     make(map[string]interface{}),
	}
}

func (g *QueryGenerator) Generate(config common.GeneratorConfig) error {
	err := g.init(config)
	if err != nil {
		return err
	}

	useGen, err := g.getUseCaseGenerator(g.conf)
	if err != nil {
		return err
	}

	filler := g.useCaseMatrix[g.conf.Use][g.conf.QueryType](useGen)

	return g.runQueryGeneration(useGen, filler, g.conf)
}

func (g *QueryGenerator) init(conf common.GeneratorConfig) error {
	if conf == nil {
		return fmt.Errorf(ErrNoConfig)
	}
	switch conf.(type) {
	case *config.QueryGeneratorConfig:
	default:
		return fmt.Errorf(ErrInvalidDataConfig)
	}
	g.conf = conf.(*config.QueryGeneratorConfig)

	err := g.conf.Validate()
	if err != nil {
		return err
	}

	if err := g.initFactories(); err != nil {
		return err
	}

	if _, ok := g.useCaseMatrix[g.conf.Use]; !ok {
		return fmt.Errorf(errBadUseFmt, g.conf.Use)
	}

	if _, ok := g.useCaseMatrix[g.conf.Use][g.conf.QueryType]; !ok {
		return fmt.Errorf(errBadQueryTypeFmt, g.conf.Use, g.conf.QueryType)
	}

	g.tsStart, err = internalUtils.ParseUTCTime(g.conf.TimeStart)
	if err != nil {
		return fmt.Errorf(errCannotParseTimeFmt, g.conf.TimeStart, err)
	}
	g.tsEnd, err = internalUtils.ParseUTCTime(g.conf.TimeEnd)
	if err != nil {
		return fmt.Errorf(errCannotParseTimeFmt, g.conf.TimeEnd, err)
	}

	if g.Out == nil {
		g.Out = os.Stdout
	}
	g.bufOut, err = getBufferedWriter(g.conf.File, g.Out)
	if err != nil {
		return err
	}

	if g.DebugOut == nil {
		g.DebugOut = os.Stderr
	}

	return nil
}

func (g *QueryGenerator) initFactories() error {
	factoryMap := factories.InitQueryFactories(g.conf)
	for db, fac := range factoryMap {
		if err := g.addFactory(db, fac); err != nil {
			return err
		}
	}
	return nil
}

func (g *QueryGenerator) addFactory(database string, factory interface{}) error {
	validFactory := false

	switch factory.(type) {
	case DevopsGeneratorMaker, IoTGeneratorMaker:
		validFactory = true
	}

	if !validFactory {
		return fmt.Errorf(errInvalidFactory, database)
	}

	g.factories[database] = factory

	return nil
}

func (g *QueryGenerator) getUseCaseGenerator(c *config.QueryGeneratorConfig) (queryUtils.QueryGenerator, error) {
	scale := int(c.Scale)
	var factory interface{}
	var ok bool

	if factory, ok = g.factories[c.Format]; !ok {
		return nil, fmt.Errorf(errUnknownFormatFmt, c.Format)
	}

	switch c.Use {
	case common.UseCaseIoT:
		iotFactory, ok := factory.(IoTGeneratorMaker)

		if !ok {
			return nil, fmt.Errorf(errUseCaseNotImplementedFmt, c.Use, c.Format)
		}

		return iotFactory.NewIoT(g.tsStart, g.tsEnd, scale)
	case common.UseCaseDevops, common.UseCaseCPUOnly, common.UseCaseCPUSingle:
		devopsFactory, ok := factory.(DevopsGeneratorMaker)
		if !ok {
			return nil, fmt.Errorf(errUseCaseNotImplementedFmt, c.Use, c.Format)
		}

		return devopsFactory.NewDevops(g.tsStart, g.tsEnd, scale)
	default:
		return nil, fmt.Errorf(errUnknownUseCaseFmt, c.Use)
	}
}

func (g *QueryGenerator) runQueryGeneration(useGen queryUtils.QueryGenerator, filler queryUtils.QueryFiller, c *config.QueryGeneratorConfig) error {
	stats := make(map[string]int64)
	currentGroup := uint(0)
	enc := gob.NewEncoder(g.bufOut)
	defer g.bufOut.Flush()

	rand.Seed(g.conf.Seed)
	if g.conf.Debug > 0 {
		_, err := fmt.Fprintf(g.DebugOut, "using random seed %d\n", g.conf.Seed)
		if err != nil {
			return fmt.Errorf(errCouldNotDebugFmt, err)
		}
	}

	zipfian := distributionGenerator.NewZipfianWithItems(10, distributionGenerator.ZipfianConstant)
	cntrForNew := counter.NewCounter(1 * 365 * 2 * 4)
	latestForNew := distributionGenerator.NewSkewedLatest(cntrForNew)
	cntrForOld := counter.NewCounter(90 * 2 * 4)
	latestForOld := distributionGenerator.NewSkewedLatest(cntrForOld)

	zipNums := make([]int64, 0)
	latestNums := make([]int64, 0)
	newOrOld := make([]int, 0)
	rz := rand.New(rand.NewSource(time.Now().UnixNano()))
	rl := rand.New(rand.NewSource(time.Now().UnixNano()))

	var mu sync.Mutex
	random := func() {
		mu.Lock()

		zipNum := zipfian.Next(rz)
		zipNums = append(zipNums, zipNum)

		rdm := rand.Intn(common.Ratio[0] + common.Ratio[1])
		if rdm < common.Ratio[0] {
			latestNumForNew := latestForNew.Next(rl)
			latestNums = append(latestNums, latestNumForNew)
			newOrOld = append(newOrOld, 0)
		} else {
			latestNumForOld := latestForOld.Next(rl)
			latestNums = append(latestNums, latestNumForOld)
			newOrOld = append(newOrOld, 1)
		}
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for i := 0; i < int(c.Limit); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			random()
		}()
	}

	wg.Wait()

	for i := 0; i < int(c.Limit); i++ {
		q := useGen.GenerateEmptyQuery()
		q = filler.Fill(q, zipNums[i], latestNums[i], newOrOld[i])

		if currentGroup == c.InterleavedGroupID {
			err := enc.Encode(q)
			if err != nil {
				return fmt.Errorf(errCouldNotEncodeQueryFmt, err)
			}
			stats[string(q.HumanLabelName())]++

			if c.Debug > 0 {
				var debugMsg string
				if c.Debug == 1 {
					debugMsg = string(q.HumanLabelName())
				} else if c.Debug == 2 {
					debugMsg = string(q.HumanDescriptionName())
				} else if c.Debug >= 3 {
					debugMsg = q.String()
				}

				_, err = fmt.Fprintf(g.DebugOut, debugMsg+"\n")
				if err != nil {
					return fmt.Errorf(errCouldNotDebugFmt, err)
				}
			}
		}
		q.Release()

		currentGroup++
		if currentGroup == c.InterleavedNumGroups {
			currentGroup = 0
		}
	}

	fmt.Printf("ratio:\t%d\n", common.Ratio)

	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := fmt.Fprintf(g.DebugOut, "%s: %d points\n", k, stats[k])
		if err != nil {
			return fmt.Errorf(errCouldNotQueryStatsFmt, err)
		}
	}
	return nil
}
