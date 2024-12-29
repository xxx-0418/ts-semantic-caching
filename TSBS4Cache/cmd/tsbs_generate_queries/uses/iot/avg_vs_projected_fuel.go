package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type AvgVsProjectedFuelConsumption struct {
	core utils.QueryGenerator
}

func NewAvgVsProjectedFuelConsumption(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgVsProjectedFuelConsumption{
		core: core,
	}
}

func (i *AvgVsProjectedFuelConsumption) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(AvgVsProjectedFuelConsumptionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgVsProjectedFuelConsumption(q, zipNum, latestNum, newOrOld)
	return q
}
