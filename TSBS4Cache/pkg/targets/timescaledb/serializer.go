package timescaledb

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

type Serializer struct{}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	buf := make([]byte, 0, 256)
	buf = append(buf, []byte("tags")...)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i, v := range tagValues {
		buf = append(buf, ',')
		buf = append(buf, tagKeys[i]...)
		buf = append(buf, '=')
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	buf = make([]byte, 0, 256)
	buf = append(buf, p.MeasurementName()...)
	buf = append(buf, ',')
	buf = append(buf, []byte(fmt.Sprintf("%d", p.Timestamp().UTC().UnixNano()))...)
	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return err
}
