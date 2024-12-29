package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TruckBreakdownFrequency struct {
	core utils.QueryGenerator
}

func NewTruckBreakdownFrequency(core utils.QueryGenerator) utils.QueryFiller {
	return &TruckBreakdownFrequency{
		core: core,
	}
}

func (i *TruckBreakdownFrequency) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(TruckBreakdownFrequencyFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TruckBreakdownFrequency(q, zipNum, latestNum, newOrOld)
	return q
}
