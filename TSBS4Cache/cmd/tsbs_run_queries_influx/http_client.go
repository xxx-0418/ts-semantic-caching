package main

import (
	"fmt"
	influxdb_client "github.com/timescale/tsbs/InfluxDB-client/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

var bytesSlash = []byte("/")

type HTTPClient struct {
	client     *http.Client
	Host       []byte
	HostString string
	uri        []byte
}

type HTTPClientDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
	chunkSize            uint64
	database             string
}

var httpClientOnce = sync.Once{}
var httpClient *http.Client

func getHttpClient() *http.Client {
	httpClientOnce.Do(func() {
		tr := &http.Transport{
			MaxIdleConnsPerHost: 1024,
		}
		httpClient = &http.Client{Transport: tr}
	})
	return httpClient
}

func NewHTTPClient(host string) *HTTPClient {
	return &HTTPClient{
		client:     getHttpClient(),
		Host:       []byte(host),
		HostString: host,
		uri:        []byte{},
	}
}

func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions, workerNum int) (float64, uint64, uint8, error) {
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.Host...)
	w.uri = append(w.uri, q.Path...)
	w.uri = append(w.uri, []byte("&db="+url.QueryEscape(opts.database))...)
	if opts.chunkSize > 0 {
		s := fmt.Sprintf("&chunked=true&chunk_size=%d", opts.chunkSize)
		w.uri = append(w.uri, []byte(s)...)
	}

	lag := float64(0)
	byteLength := uint64(0)
	hitKind := uint8(0)
	err := error(nil)

	var resp *influxdb_client.Response

	sss := strings.Split(string(q.RawQuery), ";")
	queryString := sss[0]
	segment := ""
	if len(sss) == 2 {
		segment = sss[1]
	}
	segment += ""
	start := time.Now()
	if strings.EqualFold(influxdb_client.UseCache, "stscache") {
		if len(sss) == 1 {
			_, byteLength, hitKind = influxdb_client.STsCacheClient(DBConn[workerNum%len(DBConn)], queryString)
		} else {
			_, byteLength, hitKind = influxdb_client.STsCacheClientSeg(DBConn[workerNum%len(DBConn)], queryString, segment)
		}

	} else if strings.EqualFold(influxdb_client.UseCache, "tscache") {
		_, byteLength, hitKind = influxdb_client.TSCacheClientSeg(DBConn[workerNum%len(DBConn)], queryString, segment)
	} else {

		qry := influxdb_client.NewQuery(queryString, influxdb_client.DB, "s")
		resp, err = DBConn[workerNum%len(DBConn)].Query(qry)
		if err != nil {
			panic(err)
		}
		for _, result := range resp.Results {
			for _, series := range result.Series {
				totalRowLength += int64(len(series.Values))
			}
		}

	}

	lag = float64(time.Since(start).Nanoseconds()) / 1e6

	rowNum := 0
	if opts.PrettyPrintResponses {
		for _, result := range resp.Results {
			for _, series := range result.Series {
				rowNum += len(series.Values)
			}
		}
		fmt.Println("data row number: ", rowNum)
	}

	return lag, byteLength, hitKind, err
}
