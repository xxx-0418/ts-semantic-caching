package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type AvgDailyDrivingSession struct {
	core utils.QueryGenerator
}

func NewAvgDailyDrivingSession(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgDailyDrivingSession{
		core: core,
	}
}

func (i *AvgDailyDrivingSession) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(AvgDailyDrivingSessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgDailyDrivingSession(q, zipNum, latestNum, newOrOld)
	return q
}
