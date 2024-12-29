package timescaledb

import (
	"bufio"
	"strings"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func newFileDataSource(fileName string) targets.DataSource {
	br := load.GetBufferedReader(fileName)
	return &fileDataSource{scanner: bufio.NewScanner(br)}
}

type fileDataSource struct {
	scanner *bufio.Scanner
	headers *common.GeneratedDataHeaders
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}
	var tags string
	var cols []string
	i := 0
	for {
		var line string
		ok := d.scanner.Scan()
		if !ok && d.scanner.Err() == nil {
			fatal("ended too soon, no tags or cols read")
			return nil
		} else if !ok {
			fatal("scan error: %v", d.scanner.Err())
			return nil
		}
		if i == 0 {
			tags = d.scanner.Text()
			tags = strings.TrimSpace(tags)
		} else {
			line = d.scanner.Text()
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			cols = append(cols, line)
		}
		i++
	}

	tagsarr := strings.Split(tags, ",")
	if tagsarr[0] != tagsKey {
		fatal("input header in wrong format. got '%s', expected 'tags'", tags[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(tagsarr[1:])
	fieldKeys := make(map[string][]string)
	for _, tableDef := range cols {
		columns := strings.Split(tableDef, ",")
		tableName := columns[0]
		colNames := columns[1:]
		fieldKeys[tableName] = colNames
	}
	d.headers = &common.GeneratedDataHeaders{
		TagTypes:  tagTypes,
		TagKeys:   tagNames,
		FieldKeys: fieldKeys,
	}
	return d.headers
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	if d.headers == nil {
		fatal("headers not read before starting to decode points")
		return data.LoadedPoint{}
	}
	newPoint := &insertData{}
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	parts := strings.SplitN(d.scanner.Text(), ",", 2)
	prefix := parts[0]
	if prefix != tagsKey {
		fatal("data file in invalid format; got %s expected %s", prefix, tagsKey)
		return data.LoadedPoint{}
	}
	newPoint.tags = parts[1]

	ok = d.scanner.Scan()
	if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	parts = strings.SplitN(d.scanner.Text(), ",", 2)
	prefix = parts[0]
	newPoint.fields = parts[1]

	return data.NewLoadedPoint(&point{
		hypertable: prefix,
		row:        newPoint,
	})
}
