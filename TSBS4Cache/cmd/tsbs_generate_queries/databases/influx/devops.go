package influx

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"slices"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getHostWhereStringAndTagString(metric string, nHosts int) (string, string) {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames), d.getTagStringWithNames(metric, hostnames)
}

func (d *Devops) getContinuousHostWhereStringAndTagString(metric string) (string, string) {
	hostnames, err := d.GetContinuousRandomHosts()
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames), d.getTagStringWithNames(metric, hostnames)
}

func (d *Devops) getTagStringWithNames(metric string, names []string) string {
	tagString := ""
	tagString += "{"
	slices.Sort(names)
	for _, s := range names {
		tagString += fmt.Sprintf("(%s.hostname=%s)", metric, s)
	}
	tagString += "}"
	return tagString
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("Influx %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time < '%s' group by time(1m)", strings.Join(selectClauses, ", "), whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	where := fmt.Sprintf("WHERE time < '%s'", interval.EndString())

	humanLabel := "Influx max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf(`SELECT max(usage_user) from cpu %s group by time(1m) limit 5`, where)
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectClausesAggMetrics("mean", metrics)

	humanLabel := devops.GetDoubleGroupByLabel("Influx", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where time >= '%s' and time < '%s' group by time(1h),hostname", strings.Join(selectClauses, ", "), interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	whereHosts := d.getHostWhereString(nHosts)
	selectClauses := d.getSelectClausesAggMetrics("max", devops.GetAllCPUMetrics())

	humanLabel := devops.GetMaxAllLabel("Influx", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time < '%s' group by time(1h)", strings.Join(selectClauses, ","), whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Influx last row per host"
	humanDesc := humanLabel + ": cpu"
	influxql := "SELECT * from cpu group by \"hostname\" order by time desc limit 1"
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("and %s", d.getHostWhereString(nHosts))
	}

	humanLabel, err := devops.GetHighCPULabel("Influx", nHosts)
	databases.PanicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT * from cpu where usage_user > 90.0 %s and time >= '%s' and time < '%s'", hostWhereClause, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) SimpleCPU(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	influxql = fmt.Sprintf(
		`SELECT mean(usage_nice),mean(usage_steal),mean(usage_guest) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(%s)`,
		d.getHostWhereString(common.TagNum), interval.StartString(), interval.EndString(), duration)

	humanLabel := "Influx Simple CPU"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) ThreeField3(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := "", ""
	if !common.RandomTag {
		hostWhereString, tagString = d.getContinuousHostWhereStringAndTagString("cpu")
	} else {
		hostWhereString, tagString = d.getHostWhereStringAndTagString("cpu", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(usage_system),mean(usage_idle),mean(usage_nice) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(%s)`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx Three Field 3"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) FiveField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := "", ""
	if !common.RandomTag {
		hostWhereString, tagString = d.getContinuousHostWhereStringAndTagString("cpu")
	} else {
		hostWhereString, tagString = d.getHostWhereStringAndTagString("cpu", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(usage_user),mean(usage_system),mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(%s)`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx Five Field 1"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) ThreeField(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := "", ""
	if !common.RandomTag {
		hostWhereString, tagString = d.getContinuousHostWhereStringAndTagString("cpu")
	} else {
		hostWhereString, tagString = d.getHostWhereStringAndTagString("cpu", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(usage_user),mean(usage_system),mean(usage_idle) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(%s)`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx Three Field"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) ThreeField2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := "", ""
	if !common.RandomTag {
		hostWhereString, tagString = d.getContinuousHostWhereStringAndTagString("cpu")
	} else {
		hostWhereString, tagString = d.getHostWhereStringAndTagString("cpu", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(%s)`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{usage_idle[int64],usage_nice[int64],usage_iowait[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx Three Field 2"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) TenField(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := "", ""
	if !common.RandomTag {
		hostWhereString, tagString = d.getContinuousHostWhereStringAndTagString("cpu")
	} else {
		hostWhereString, tagString = d.getHostWhereStringAndTagString("cpu", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(usage_user),mean(usage_system),mean(usage_idle),mean(usage_nice),mean(usage_iowait),mean(usage_irq),mean(usage_softirq),mean(usage_steal),mean(usage_guest),mean(usage_guest_nice) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(%s)`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64],usage_irq[int64],usage_softirq[int64],usage_steal[int64],usage_guest[int64],usage_guest_nice[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx Ten Field "
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) TenFieldWithPredicate(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	hostWhereString, tagString := "", ""
	if !common.RandomTag {
		hostWhereString, tagString = d.getContinuousHostWhereStringAndTagString("cpu")
	} else {
		hostWhereString, tagString = d.getHostWhereStringAndTagString("cpu", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM "cpu" WHERE %s AND usage_user > 90 AND usage_guest > 90 AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname"`,
		hostWhereString, interval.StartString(), interval.EndString())

	influxql += ";"
	influxql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64],usage_irq[int64],usage_softirq[int64],usage_steal[int64],usage_guest[int64],usage_guest_nice[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{empty,empty}", tagString)

	humanLabel := "Influx Ten Field with Predicate"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) CPUQueries(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	switch index % 6 {
	case 0:
		d.ThreeField3(qi, zipNum, latestNum, newOrOld)
		break
	case 1:
		d.FiveField1(qi, zipNum, latestNum, newOrOld)
		break
	case 2:
		d.ThreeField(qi, zipNum, latestNum, newOrOld)
		break
	case 3:
		d.ThreeField2(qi, zipNum, latestNum, newOrOld)
		break
	case 4:
		d.TenField(qi, zipNum, latestNum, newOrOld)
		break
	case 5:
		d.TenFieldWithPredicate(qi, zipNum, latestNum, newOrOld)
		break
	default:
		d.FiveField1(qi, zipNum, latestNum, newOrOld)
		break
	}
	index++
}
