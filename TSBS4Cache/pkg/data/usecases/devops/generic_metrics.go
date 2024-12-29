package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"sort"
	"time"
)

var (
	labelGenericMetrics                                   = []byte("generic_metrics")
	genericMetricFields []common.LabeledDistributionMaker = nil
	metricND                                              = common.ND(0.0, 1.0)
	zipfRandSeed                                          = int64(1234)
)

type GenericMeasurements struct {
	*common.SubsystemMeasurement
}

func initGenericMetricFields(size uint64) {
	if genericMetricFields == nil {
		genericMetricFields = make([]common.LabeledDistributionMaker, size)
		for i := range genericMetricFields {
			genericMetricFields[i] = common.LabeledDistributionMaker{Label: []byte(fmt.Sprintf("metric_%d", i)), DistributionMaker: func() common.Distribution { return common.CWD(metricND, 0.0, 1000, rand.Float64()*1000) }}
		}
	}
}

func NewGenericMeasurements(start time.Time, count uint64) *GenericMeasurements {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, genericMetricFields[:count])
	return &GenericMeasurements{sub}
}

func (gm *GenericMeasurements) ToPoint(p *data.Point) {
	gm.ToPointAllInt64(p, labelGenericMetrics, genericMetricFields)
}

func generateHostMetricCount(hostCount uint64, maxMetricCount uint64) []uint64 {
	zipfMetricCountHosts := genZipfArray(hostCount-1, maxMetricCount)
	return append(zipfMetricCountHosts, maxMetricCount)
}

func generateHostEpochsToLive(hostCount uint64, epochs uint64) []uint64 {
	longLivedHosts := make([]uint64, hostCount-hostCount/2, hostCount-hostCount/2)
	shortLived := genZipfArray(hostCount/2, epochs)
	sort.Slice(shortLived, func(i, j int) bool {
		return shortLived[i] > shortLived[j]
	})
	return append(longLivedHosts, shortLived...)
}

func genZipfArray(arraySize uint64, maxValue uint64) []uint64 {
	zipfArray := make([]uint64, arraySize)
	zipf := rand.NewZipf(rand.New(rand.NewSource(zipfRandSeed)), 1.01, 1, maxValue)
	for i := range zipfArray {
		val := zipf.Uint64()
		if val == maxValue {
			zipfArray[i] = val
		} else {
			zipfArray[i] = val + 1
		}
	}
	return zipfArray
}
