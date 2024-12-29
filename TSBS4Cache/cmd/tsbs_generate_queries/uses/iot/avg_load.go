package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type AvgLoad struct {
	core utils.QueryGenerator
}

func NewAvgLoad(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgLoad{
		core: core,
	}
}

func (i *AvgLoad) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(AvgLoadFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgLoad(q, zipNum, latestNum, newOrOld)
	return q
}
