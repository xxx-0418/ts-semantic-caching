package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type LastLocPerTruck struct {
	core utils.QueryGenerator
}

func NewLastLocPerTruck(core utils.QueryGenerator) utils.QueryFiller {
	return &LastLocPerTruck{
		core: core,
	}
}

func (i *LastLocPerTruck) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(LastLocFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.LastLocPerTruck(q, zipNum, latestNum, newOrOld)
	return q
}
