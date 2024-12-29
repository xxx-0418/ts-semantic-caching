package devops

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	allHosts                = "all hosts"
	errNHostsCannotNegative = "nHosts cannot be negative"
	errNoMetrics            = "cannot get 0 metrics"
	errTooManyMetrics       = "too many metrics asked for"

	TableName = "cpu"

	DoubleGroupByDuration = 12 * time.Hour
	HighCPUDuration       = 12 * time.Hour
	MaxAllDuration        = 8 * time.Hour

	LabelSingleGroupby       = "single-groupby"
	LabelDoubleGroupby       = "double-groupby"
	LabelLastpoint           = "lastpoint"
	LabelMaxAll              = "cpu-max-all"
	LabelGroupbyOrderbyLimit = "groupby-orderby-limit"
	LabelHighCPU             = "high-cpu"

	LabelCPUQueries = "cpu-queries"
)

type Core struct {
	*common.Core
}

func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err

}

func (d *Core) GetRandomHosts(nHosts int) ([]string, error) {
	return getRandomHosts(nHosts, d.Scale)
}

func (d *Core) GetContinuousRandomHosts() ([]string, error) {
	return getContinuousRandomHosts()
}

var cpuMetrics = []string{
	"usage_user",
	"usage_system",
	"usage_idle",
	"usage_nice",
	"usage_iowait",
	"usage_irq",
	"usage_softirq",
	"usage_steal",
	"usage_guest",
	"usage_guest_nice",
}

func GetCPUMetricsSlice(numMetrics int) ([]string, error) {
	if numMetrics <= 0 {
		return nil, fmt.Errorf(errNoMetrics)
	}
	if numMetrics > len(cpuMetrics) {
		return nil, fmt.Errorf(errTooManyMetrics)
	}
	return cpuMetrics[:numMetrics], nil
}

type CPUQueriesFiller interface {
	CPUQueries(query.Query, int64, int64, int)
}

func GetAllCPUMetrics() []string {
	return cpuMetrics
}

func GetCPUMetricsLen() int {
	return len(cpuMetrics)
}

type SingleGroupbyFiller interface {
	GroupByTime(query.Query, int, int, time.Duration, int64, int64, int)
}

type DoubleGroupbyFiller interface {
	GroupByTimeAndPrimaryTag(query.Query, int)
}

type LastPointFiller interface {
	LastPointPerHost(query.Query)
}

type MaxAllFiller interface {
	MaxAllCPU(query.Query, int, time.Duration)
}

type GroupbyOrderbyLimitFiller interface {
	GroupByOrderByLimit(query.Query)
}

type HighCPUFiller interface {
	HighCPUForHosts(query.Query, int)
}

func GetDoubleGroupByLabel(dbName string, numMetrics int) string {
	return fmt.Sprintf("%s mean of %d metrics, all hosts, random %s by 1h", dbName, numMetrics, DoubleGroupByDuration)
}

func GetHighCPULabel(dbName string, nHosts int) (string, error) {
	label := dbName + " CPU over threshold, "
	if nHosts > 0 {
		label += fmt.Sprintf("%d host(s)", nHosts)
	} else if nHosts == 0 {
		label += allHosts
	} else {
		return "", fmt.Errorf("nHosts cannot be negative")
	}
	return label, nil
}

func GetMaxAllLabel(dbName string, nHosts int) string {
	return fmt.Sprintf("%s max of all CPU metrics, random %4d hosts, random %s by 1h", dbName, nHosts, MaxAllDuration)
}

func getRandomHosts(numHosts int, totalHosts int) ([]string, error) {
	if numHosts < 1 {
		return nil, fmt.Errorf("number of hosts cannot be < 1; got %d", numHosts)
	}
	if numHosts > totalHosts {
		return nil, fmt.Errorf("number of hosts (%d) larger than total hosts. See --scale (%d)", numHosts, totalHosts)
	}

	randomNumbers, err := common.GetRandomSubsetPerm(numHosts, totalHosts)
	if err != nil {
		return nil, err
	}

	hostnames := []string{}
	for _, n := range randomNumbers {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	return hostnames, nil
}

func getContinuousRandomHosts() ([]string, error) {

	randomNumbers, err := common.GetContinuousRandomSubset()
	if err != nil {
		return nil, err
	}

	hostnames := []string{}
	for _, n := range randomNumbers {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}
	return hostnames, nil
}
