package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type GroupByOrderByLimit struct {
	core utils.QueryGenerator
}

func NewGroupByOrderByLimit(core utils.QueryGenerator) utils.QueryFiller {
	return &GroupByOrderByLimit{core}
}

func (d *GroupByOrderByLimit) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(GroupbyOrderbyLimitFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.GroupByOrderByLimit(q)
	return q
}
