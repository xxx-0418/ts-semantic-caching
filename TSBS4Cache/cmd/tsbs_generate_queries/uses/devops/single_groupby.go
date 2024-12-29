package devops

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type SingleGroupby struct {
	core    utils.QueryGenerator
	metrics int
	hosts   int
	hours   int
}

func NewSingleGroupby(metrics, hosts, hours int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &SingleGroupby{
			core:    core,
			metrics: metrics,
			hosts:   hosts,
			hours:   hours,
		}
	}
}

func (d *SingleGroupby) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(SingleGroupbyFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.GroupByTime(q, d.hosts, d.metrics, time.Duration(int64(d.hours)*int64(time.Hour)), zipNum, latestNum, newOrOld)
	return q
}
