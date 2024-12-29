package devops

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type MaxAllCPU struct {
	core     utils.QueryGenerator
	hosts    int
	duration time.Duration
}

func NewMaxAllCPU(hosts int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &MaxAllCPU{
			core:     core,
			hosts:    hosts,
			duration: duration,
		}
	}
}

func (d *MaxAllCPU) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(MaxAllFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.MaxAllCPU(q, d.hosts, d.duration)
	return q
}
