package utils

import "github.com/timescale/tsbs/pkg/query"

type QueryGenerator interface {
	GenerateEmptyQuery() query.Query
}

type QueryFiller interface {
	Fill(query.Query, int64, int64, int) query.Query
}

type QueryFillerMaker func(QueryGenerator) QueryFiller
