package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TrucksWithLongDailySession struct {
	core utils.QueryGenerator
}

func NewTruckWithLongDailySession(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLongDailySession{
		core: core,
	}
}

func (i *TrucksWithLongDailySession) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(TruckLongDailySessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLongDailySessions(q, zipNum, latestNum, newOrOld)
	return q
}
