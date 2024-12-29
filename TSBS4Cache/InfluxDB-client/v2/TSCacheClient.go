package influxdb_client

import (
	"fmt"
	stscache "github.com/timescale/tsbs/InfluxDB-client/memcache"
	"log"
	"time"
)

func TSCacheClient(conn Client, queryString string) (*Response, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString)

	mtx.Lock()

	partialSegment := ""
	fields := ""
	metric := ""
	if ps, ok := QueryTemplateToPartialSegment[queryTemplate]; !ok {
		partialSegment, fields, metric = GetPartialSegmentAndFields(queryString)
		QueryTemplateToPartialSegment[queryTemplate] = partialSegment
		SegmentToFields[partialSegment] = fields
		SegmentToMetric[partialSegment] = metric
	} else {
		partialSegment = ps
		fields = SegmentToFields[partialSegment]
		metric = SegmentToMetric[partialSegment]
	}

	semanticSegment := GetTotalSegment(metric, tags, partialSegment)

	CacheIndex := GetCacheHashValue(fields)
	fields = "time[int64]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	mtx.Unlock()

	values, _, err := STsConnArr[CacheIndex].Get(semanticSegment, startTime, endTime)
	if err != nil {
		q := NewQuery(queryString, DB, "s")
		resp, err := conn.Query(q)
		if err != nil {
			log.Println(queryString)
		}

		if !ResponseIsEmpty(resp) {
			numOfTab := GetNumOfTable(resp)
			remainValues := ResponseToByteArrayWithParams(resp, datatypes, tags, metric, partialSegment)
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: semanticSegment, Value: remainValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
		}
		return resp, byteLength, hitKind
	} else {
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

		if flagNum == 0 {
			hitKind = 2
			return convertedResponse, byteLength, hitKind
		} else {
			hitKind = 1

			remainQueryString, minTime, maxTime := RemainQueryString(queryString, queryTemplate, flagArr, timeRangeArr, tagArr)

			remainTags := make([]string, 0)
			for i, tag := range tagArr {
				if flagArr[i] == 1 {
					remainTags = append(remainTags, fmt.Sprintf("%s=%s", tag[0], tag[1]))
				}
			}

			if maxTime-minTime <= int64(time.Minute.Seconds()) {
				hitKind = 2
				return convertedResponse, byteLength, hitKind
			}

			remainQuery := NewQuery(remainQueryString, DB, "s")
			remainResp, err := conn.Query(remainQuery)
			if err != nil {
				log.Println(remainQueryString)
			}

			if ResponseIsEmpty(remainResp) {
				hitKind = 1
				return convertedResponse, byteLength, hitKind
			}
			remainByteArr := RemainResponseToByteArrayWithParams(remainResp, datatypes, remainTags, metric, partialSegment)
			numOfTableR := len(remainResp.Results)
			err = STsConnArr[CacheIndex].Set(&stscache.Item{
				Key:         semanticSegment,
				Value:       remainByteArr,
				Time_start:  minTime,
				Time_end:    maxTime,
				NumOfTables: int64(numOfTableR),
			})

			totalResp := MergeRemainResponse(remainResp, convertedResponse)

			return totalResp, byteLength, hitKind

		}

	}

}

func TSCacheClientSeg(conn Client, queryString string, semanticSegment string) (*Response, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString)

	partialSegment := ""
	fields := ""
	metric := ""
	partialSegment, fields, metric = SplitPartialSegment(semanticSegment)

	CacheIndex := GetCacheHashValue(fields)
	fields = "time[int64]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	values, _, err := STsConnArr[CacheIndex].Get(semanticSegment, startTime, endTime)
	if err != nil {
		q := NewQuery(queryString, DB, "s")
		resp, err := conn.Query(q)
		if err != nil {
			log.Println(queryString)
		}

		if !ResponseIsEmpty(resp) {
			numOfTab := GetNumOfTable(resp)
			remainValues := ResponseToByteArrayWithParams(resp, datatypes, tags, metric, partialSegment)
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: semanticSegment, Value: remainValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
		} else {
			singleSemanticSegment := GetSingleSegment(metric, partialSegment, tags)
			emptyValues := make([]byte, 0)
			for _, ss := range singleSemanticSegment {
				zero, _ := Int64ToByteArray(int64(0))
				emptyValues = append(emptyValues, []byte(ss)...)
				emptyValues = append(emptyValues, []byte(" ")...)
				emptyValues = append(emptyValues, zero...)
			}
			numOfTab := int64(len(singleSemanticSegment))
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: semanticSegment, Value: emptyValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
		}

		return resp, byteLength, hitKind

	} else {
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)
		if flagNum == 0 {
			hitKind = 2
			return convertedResponse, byteLength, hitKind
		} else {
			hitKind = 1
			remainQueryString, minTime, maxTime := RemainQueryString(queryString, queryTemplate, flagArr, timeRangeArr, tagArr)
			remainTags := make([]string, 0)
			for i, tag := range tagArr {
				if flagArr[i] == 1 {
					remainTags = append(remainTags, fmt.Sprintf("%s=%s", tag[0], tag[1]))
				}
			}
			if maxTime-minTime <= int64(time.Minute.Seconds()) {
				hitKind = 2
				return convertedResponse, byteLength, hitKind
			}
			remainQuery := NewQuery(remainQueryString, DB, "s")
			remainResp, err := conn.Query(remainQuery)
			if err != nil {
				log.Println(remainQueryString)
			}
			if ResponseIsEmpty(remainResp) {
				hitKind = 1

				singleSemanticSegment := GetSingleSegment(metric, partialSegment, remainTags)
				emptyValues := make([]byte, 0)
				for _, ss := range singleSemanticSegment {
					zero, _ := Int64ToByteArray(int64(0))
					emptyValues = append(emptyValues, []byte(ss)...)
					emptyValues = append(emptyValues, []byte(" ")...)
					emptyValues = append(emptyValues, zero...)
				}

				numOfTab := int64(len(singleSemanticSegment))
				err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: semanticSegment, Value: emptyValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
				return convertedResponse, byteLength, hitKind
			}
			remainByteArr := RemainResponseToByteArrayWithParams(remainResp, datatypes, remainTags, metric, partialSegment)
			numOfTableR := len(remainResp.Results)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{
				Key:         semanticSegment,
				Value:       remainByteArr,
				Time_start:  minTime,
				Time_end:    maxTime,
				NumOfTables: int64(numOfTableR),
			})
			totalResp := MergeRemainResponse(remainResp, convertedResponse)

			return totalResp, byteLength, hitKind
		}

	}

}
