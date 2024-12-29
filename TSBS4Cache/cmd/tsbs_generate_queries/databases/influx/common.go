package influx

import (
	"fmt"
	"net/url"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type BaseGenerator struct {
}

func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, influxql string) {
	v := url.Values{}
	v.Set("q", influxql)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.RawQuery = []byte(influxql)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("POST")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
