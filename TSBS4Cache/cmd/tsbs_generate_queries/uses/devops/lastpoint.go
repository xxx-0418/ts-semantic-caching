package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type LastPointPerHost struct {
	core utils.QueryGenerator
}

func NewLastPointPerHost(core utils.QueryGenerator) utils.QueryFiller {
	return &LastPointPerHost{core}
}

func (d *LastPointPerHost) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(LastPointFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.LastPointPerHost(q)
	return q
}
