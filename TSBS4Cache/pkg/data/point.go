package data

import (
	"bytes"
	"time"
)

type Point struct {
	measurementName []byte
	tagKeys         [][]byte
	tagValues       []interface{}
	fieldKeys       [][]byte
	fieldValues     []interface{}
	timestamp       *time.Time
}

func NewPoint() *Point {
	return &Point{
		measurementName: nil,
		tagKeys:         make([][]byte, 0),
		tagValues:       make([]interface{}, 0),
		fieldKeys:       make([][]byte, 0),
		fieldValues:     make([]interface{}, 0),
		timestamp:       nil,
	}
}

func (p *Point) Copy(from *Point) {
	p.measurementName = from.measurementName
	p.tagKeys = from.tagKeys
	p.tagValues = from.tagValues
	p.fieldKeys = from.fieldKeys
	p.fieldValues = from.fieldValues
	timeCopy := *from.timestamp
	p.timestamp = &timeCopy
}

func (p *Point) Reset() {
	p.measurementName = nil
	p.tagKeys = p.tagKeys[:0]
	p.tagValues = p.tagValues[:0]
	p.fieldKeys = p.fieldKeys[:0]
	p.fieldValues = p.fieldValues[:0]
	p.timestamp = nil
}

func (p *Point) SetTimestamp(t *time.Time) {
	p.timestamp = t
}

func (p *Point) Timestamp() *time.Time {
	return p.timestamp
}

func (p *Point) TimestampInUnixMs() int64 {
	return p.timestamp.UnixNano() / 1000000
}

func (p *Point) SetMeasurementName(s []byte) {
	p.measurementName = s
}

func (p *Point) MeasurementName() []byte {
	return p.measurementName
}

func (p *Point) FieldKeys() [][]byte {
	return p.fieldKeys
}

func (p *Point) AppendField(key []byte, value interface{}) {
	p.fieldKeys = append(p.fieldKeys, key)
	p.fieldValues = append(p.fieldValues, value)
}

func (p *Point) GetFieldValue(key []byte) interface{} {
	if len(p.fieldKeys) != len(p.fieldValues) {
		panic("field keys and field values are out of sync")
	}
	for i, v := range p.fieldKeys {
		if bytes.Equal(v, key) {
			return p.fieldValues[i]
		}
	}
	return nil
}

func (p *Point) FieldValues() []interface{} {
	return p.fieldValues
}

func (p *Point) ClearFieldValue(key []byte) {
	if len(p.fieldKeys) != len(p.fieldValues) {
		panic("field keys and field values are out of sync")
	}
	for i, v := range p.fieldKeys {
		if bytes.Equal(v, key) {
			p.fieldValues[i] = nil
			return
		}
	}
}

func (p *Point) TagKeys() [][]byte {
	return p.tagKeys
}

func (p *Point) AppendTag(key []byte, value interface{}) {
	p.tagKeys = append(p.tagKeys, key)
	p.tagValues = append(p.tagValues, value)
}

func (p *Point) GetTagValue(key []byte) interface{} {
	if len(p.tagKeys) != len(p.tagValues) {
		panic("tag keys and tag values are out of sync")
	}
	for i, v := range p.tagKeys {
		if bytes.Equal(v, key) {
			return p.tagValues[i]
		}
	}
	return nil
}

func (p *Point) TagValues() []interface{} {
	return p.tagValues
}

func (p *Point) ClearTagValue(key []byte) {
	if len(p.tagKeys) != len(p.tagValues) {
		panic("tag keys and tag values are out of sync")
	}
	for i, v := range p.tagKeys {
		if bytes.Equal(v, key) {
			p.tagValues[i] = nil
			return
		}
	}
}

type LoadedPoint struct {
	Data interface{}
}

func NewLoadedPoint(data interface{}) LoadedPoint {
	return LoadedPoint{Data: data}
}
