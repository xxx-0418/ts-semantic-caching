package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type Groupby struct {
	core       utils.QueryGenerator
	numMetrics int
}

func NewGroupBy(numMetrics int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &Groupby{
			core:       core,
			numMetrics: numMetrics,
		}
	}
}

func (d *Groupby) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(DoubleGroupbyFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.GroupByTimeAndPrimaryTag(q, d.numMetrics)
	return q
}
