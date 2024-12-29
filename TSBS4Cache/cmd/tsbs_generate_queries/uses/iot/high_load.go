package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TrucksWithHighLoad struct {
	core utils.QueryGenerator
}

func NewTruckWithHighLoad(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithHighLoad{
		core: core,
	}
}

func (i *TrucksWithHighLoad) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(TruckHighLoadFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithHighLoad(q, zipNum, latestNum, newOrOld)
	return q
}
