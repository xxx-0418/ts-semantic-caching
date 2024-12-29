package influx

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

type Serializer struct{}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	buf := make([]byte, 0, 1024)
	buf = append(buf, p.MeasurementName()...)

	fakeTags := make([]int, 0)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i := 0; i < len(tagKeys); i++ {
		if tagValues[i] == nil {
			continue
		}
		switch v := tagValues[i].(type) {
		case string:
			buf = append(buf, ',')
			buf = append(buf, tagKeys[i]...)
			buf = append(buf, '=')
			buf = append(buf, []byte(v)...)
		default:
			fakeTags = append(fakeTags, i)
		}
	}
	fieldKeys := p.FieldKeys()
	if len(fakeTags) > 0 || len(fieldKeys) > 0 {
		buf = append(buf, ' ')
	}
	firstFieldFormatted := false
	for i := 0; i < len(fakeTags); i++ {
		tagIndex := fakeTags[i]
		if firstFieldFormatted {
			buf = append(buf, ',')
		}
		firstFieldFormatted = true
		buf = appendField(buf, tagKeys[tagIndex], tagValues[tagIndex])
	}

	fieldValues := p.FieldValues()
	for i := 0; i < len(fieldKeys); i++ {
		value := fieldValues[i]
		if value == nil {
			continue
		}
		if firstFieldFormatted {
			buf = append(buf, ',')
		}
		firstFieldFormatted = true
		buf = appendField(buf, fieldKeys[i], value)
	}

	if !firstFieldFormatted {
		return nil
	}
	buf = append(buf, ' ')
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)
	buf = append(buf, '\n')
	_, err = w.Write(buf)

	return err
}

func appendField(buf, key []byte, v interface{}) []byte {
	buf = append(buf, key...)
	buf = append(buf, '=')

	buf = serialize.FastFormatAppend(v, buf)

	switch v.(type) {
	case int, int64:
		buf = append(buf, 'i')
	}

	return buf
}