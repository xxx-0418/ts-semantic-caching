package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type HighCPU struct {
	core  utils.QueryGenerator
	hosts int
}

func NewHighCPU(hosts int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &HighCPU{
			core:  core,
			hosts: hosts,
		}
	}
}

func (d *HighCPU) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(HighCPUFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.HighCPUForHosts(q, d.hosts)
	return q
}
