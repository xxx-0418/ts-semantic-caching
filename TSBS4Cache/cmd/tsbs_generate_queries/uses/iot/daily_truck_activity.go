package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type DailyTruckActivity struct {
	core utils.QueryGenerator
}

func NewDailyTruckActivity(core utils.QueryGenerator) utils.QueryFiller {
	return &DailyTruckActivity{
		core: core,
	}
}

func (i *DailyTruckActivity) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(DailyTruckActivityFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DailyTruckActivity(q, zipNum, latestNum, newOrOld)
	return q
}
