package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type LastLocSingleTruck struct {
	core utils.QueryGenerator
}

func NewLastLocSingleTruck(core utils.QueryGenerator) utils.QueryFiller {
	return &LastLocSingleTruck{
		core: core,
	}
}

func (i *LastLocSingleTruck) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(LastLocByTruckFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.LastLocByTruck(q, 1, zipNum, latestNum, newOrOld)
	return q
}
