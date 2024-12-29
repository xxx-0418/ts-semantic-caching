package inputs

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/usecases"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

const (
	ErrNoConfig          = "no GeneratorConfig provided"
	ErrInvalidDataConfig = "invalid config: DataGenerator needs a DataGeneratorConfig"
)

type DataGenerator struct {
	Out io.Writer

	config *common.DataGeneratorConfig

	bufOut *bufio.Writer
}

func (g *DataGenerator) init(config common.GeneratorConfig) error {
	if config == nil {
		return fmt.Errorf(ErrNoConfig)
	}
	switch config.(type) {
	case *common.DataGeneratorConfig:
	default:
		return fmt.Errorf(ErrInvalidDataConfig)
	}
	g.config = config.(*common.DataGeneratorConfig)

	err := g.config.Validate()
	if err != nil {
		return err
	}

	if g.Out == nil {
		g.Out = os.Stdout
	}
	g.bufOut, err = getBufferedWriter(g.config.File, g.Out)
	if err != nil {
		return err
	}

	return nil
}

func (g *DataGenerator) Generate(config common.GeneratorConfig, target targets.ImplementedTarget) error {
	err := g.init(config)
	if err != nil {
		return err
	}

	rand.Seed(g.config.Seed)

	scfg, err := usecases.GetSimulatorConfig(g.config)
	if err != nil {
		return err
	}

	sim := scfg.NewSimulator(g.config.LogInterval, g.config.Limit)
	serializer, err := g.getSerializer(sim, target)
	if err != nil {
		return err
	}

	return g.runSimulator(sim, serializer, g.config)
}

func (g *DataGenerator) CreateSimulator(config *common.DataGeneratorConfig) (common.Simulator, error) {
	err := g.init(config)
	if err != nil {
		return nil, err
	}
	rand.Seed(g.config.Seed)
	scfg, err := usecases.GetSimulatorConfig(g.config)
	if err != nil {
		return nil, err
	}

	return scfg.NewSimulator(g.config.LogInterval, g.config.Limit), nil
}

func (g *DataGenerator) runSimulator(sim common.Simulator, serializer serialize.PointSerializer, dgc *common.DataGeneratorConfig) error {
	defer g.bufOut.Flush()

	currGroupID := uint(0)
	point := data.NewPoint()
	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		if currGroupID == dgc.InterleavedGroupID {
			err := serializer.Serialize(point, g.bufOut)
			if err != nil {
				return fmt.Errorf("can not serialize point: %s", err)
			}
		}
		point.Reset()

		currGroupID = (currGroupID + 1) % dgc.InterleavedNumGroups
	}
	return nil
}

func (g *DataGenerator) getSerializer(sim common.Simulator, target targets.ImplementedTarget) (serialize.PointSerializer, error) {
	switch target.TargetName() {
	case constants.FormatCrateDB:
		fallthrough
	case constants.FormatClickhouse:
		fallthrough
	case constants.FormatTimescaleDB:
		g.writeHeader(sim.Headers())
	}
	return target.Serializer(), nil
}

// TODO should be implemented in targets package
func (g *DataGenerator) writeHeader(headers *common.GeneratedDataHeaders) {
	g.bufOut.WriteString("tags")

	types := headers.TagTypes
	for i, key := range headers.TagKeys {
		g.bufOut.WriteString(",")
		g.bufOut.Write([]byte(key))
		g.bufOut.WriteString(" ")
		g.bufOut.WriteString(types[i])
	}
	g.bufOut.WriteString("\n")
	keys := make([]string, 0)
	fields := headers.FieldKeys
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, measurementName := range keys {
		g.bufOut.WriteString(measurementName)
		for _, field := range fields[measurementName] {
			g.bufOut.WriteString(",")
			g.bufOut.Write([]byte(field))
		}
		g.bufOut.WriteString("\n")
	}
	g.bufOut.WriteString("\n")
}
