package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type AvgDailyDrivingDuration struct {
	core utils.QueryGenerator
}

func NewAvgDailyDrivingDuration(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgDailyDrivingDuration{
		core: core,
	}
}

func (i *AvgDailyDrivingDuration) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(AvgDailyDrivingDurationFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgDailyDrivingDuration(q, zipNum, latestNum, newOrOld)
	return q
}
