package targets

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type ImplementedTarget interface {
	Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (Benchmark, error)
	Serializer() serialize.PointSerializer
	TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet)
	TargetName() string
}

type Batch interface {
	Len() uint
	Append(data.LoadedPoint)
}

type PointIndexer interface {
	GetIndex(data.LoadedPoint) uint
}

type ConstantIndexer struct{}

func (i *ConstantIndexer) GetIndex(_ data.LoadedPoint) uint {
	return 0
}

type BatchFactory interface {
	New() Batch
}

type Benchmark interface {
	GetDataSource() DataSource

	GetBatchFactory() BatchFactory

	GetPointIndexer(maxPartitions uint) PointIndexer

	GetProcessor() Processor

	GetDBCreator() DBCreator
}

type DataSource interface {
	NextItem() data.LoadedPoint
	Headers() *common.GeneratedDataHeaders
}
