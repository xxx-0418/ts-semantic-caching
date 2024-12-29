package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TrucksWithLowFuel struct {
	core utils.QueryGenerator
}

func NewTruckWithLowFuel(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLowFuel{
		core: core,
	}
}

func (i *TrucksWithLowFuel) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(TruckLowFuelFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLowFuel(q, zipNum, latestNum, newOrOld)
	return q
}
