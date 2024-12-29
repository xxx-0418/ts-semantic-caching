package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TrucksWithLongDrivingSession struct {
	core utils.QueryGenerator
}

func NewTrucksWithLongDrivingSession(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLongDrivingSession{
		core: core,
	}
}

func (i *TrucksWithLongDrivingSession) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(TruckLongDrivingSessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLongDrivingSessions(q, zipNum, latestNum, newOrOld)
	return q
}
