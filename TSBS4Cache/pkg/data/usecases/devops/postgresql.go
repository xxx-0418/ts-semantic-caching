package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

var (
	labelPostgresql = []byte("postgresl")
	pgND            = common.ND(5, 1)
	pgHighND        = common.ND(1024, 1)

	postgresqlFields = []common.LabeledDistributionMaker{
		{Label: []byte("numbackends"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("xact_commit"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("xact_rollback"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("blks_read"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("blks_hit"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("tup_returned"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("tup_fetched"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("tup_inserted"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("tup_updated"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("tup_deleted"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("conflicts"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("temp_files"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("temp_bytes"), DistributionMaker: func() common.Distribution { return common.CWD(pgHighND, 0, 1024*1024*1024, 0) }},
		{Label: []byte("deadlocks"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("blk_read_time"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{Label: []byte("blk_write_time"), DistributionMaker: func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
	}
)

type PostgresqlMeasurement struct {
	*common.SubsystemMeasurement
}

func NewPostgresqlMeasurement(start time.Time) *PostgresqlMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, postgresqlFields)
	return &PostgresqlMeasurement{sub}
}

func (m *PostgresqlMeasurement) ToPoint(p *data.Point) {
	m.ToPointAllInt64(p, labelPostgresql, postgresqlFields)
}
