package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type StationaryTrucks struct {
	core utils.QueryGenerator
}

func NewStationaryTrucks(core utils.QueryGenerator) utils.QueryFiller {
	return &StationaryTrucks{
		core: core,
	}
}

func (i *StationaryTrucks) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(StationaryTrucksFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.StationaryTrucks(q, zipNum, latestNum, newOrOld)
	return q
}
