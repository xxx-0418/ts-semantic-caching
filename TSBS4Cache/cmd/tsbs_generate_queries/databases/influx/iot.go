package influx

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"slices"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

type IoT struct {
	*iot.Core
	*BaseGenerator
}

func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	databases.PanicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

func (i *IoT) getTruckWhereStringAndTagString(metric string, nTrucks int) (string, string) {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names), i.getTagStringWithNames(metric, names)
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("\"name\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) getTagStringWithNames(metric string, names []string) string {
	tagString := ""
	tagString += "{"
	slices.Sort(names)
	for _, s := range names {
		tagString += fmt.Sprintf("(%s.name=%s)", metric, s)
	}
	tagString += "}"
	return tagString
}

func (i *IoT) getContinuousTruckWhereString() string {
	names, err := i.GetContinuousRandomTrucks()
	if err != nil {
		panic(err.Error())
	}
	return i.getContinuousTrucksWhereWithNames(names)
}

func (i *IoT) getContinuousTruckWhereStringAndTagString(metric string) (string, string) {
	names, err := i.GetContinuousRandomTrucks()
	if err != nil {
		panic(err.Error())
	}
	return i.getContinuousTrucksWhereWithNames(names), i.getTagStringWithNames(metric, names)
}

func (i *IoT) getContinuousTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("\"name\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int, zipNum int64, latestNum int64, newOrOld int) {
	influxql := fmt.Sprintf(`SELECT "name", "driver", "latitude", "longitude" 
		FROM "readings" 
		WHERE %s 
		ORDER BY "time" 
		LIMIT 1`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "Influx last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) LastLocPerTruck(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {

	influxql := fmt.Sprintf(`SELECT "latitude", "longitude" 
		FROM "readings" 
		WHERE "fleet"='%s' 
		GROUP BY "name","driver" 
		ORDER BY "time" 
		LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "Influx last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) TrucksWithLowFuel(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	influxql := fmt.Sprintf(`SELECT "name", "driver", "fuel_state" 
		FROM "diagnostics" 
		WHERE "fuel_state" <= 0.1 AND "fleet" = '%s' 
		GROUP BY "name" 
		ORDER BY "time" DESC 
		LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "Influx trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) TrucksWithHighLoad(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	influxql := fmt.Sprintf(`SELECT "name", "driver", "current_load", "load_capacity" 
		FROM (SELECT  "current_load", "load_capacity" 
		 FROM "diagnostics" WHERE fleet = '%s' 
		 GROUP BY "name","driver" 
		 ORDER BY "time" DESC 
		 LIMIT 1) 
		WHERE "current_load" >= 0.9 * "load_capacity" 
		GROUP BY "name" 
		ORDER BY "time" DESC`,
		i.GetRandomFleet())

	humanLabel := "Influx trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) StationaryTrucks(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	influxql := fmt.Sprintf(`SELECT "name", "driver" 
		FROM(SELECT mean("velocity") as mean_velocity 
		 FROM "readings" 
		 WHERE time > '%s' AND time <= '%s' 
		 GROUP BY time(10m),"name","driver","fleet"  
		 LIMIT 1) 
		WHERE "fleet" = '%s' AND "mean_velocity" < 1 
		GROUP BY "name"`,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		i.GetRandomFleet())

	humanLabel := "Influx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	influxql := fmt.Sprintf(`SELECT "name","driver" 
		FROM(SELECT count(*) AS ten_min 
		 FROM(SELECT mean("velocity") AS mean_velocity 
		  FROM readings 
		  WHERE "fleet" = '%s' AND time > '%s' AND time <= '%s' 
		  GROUP BY time(10m),"name","driver") 
		 WHERE "mean_velocity" > 1 
		 GROUP BY "name","driver") 
		WHERE ten_min_mean_velocity > %d`,
		i.GetRandomFleet(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "Influx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) TrucksWithLongDailySessions(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	influxql := fmt.Sprintf(`SELECT "name","driver" 
		FROM(SELECT count(*) AS ten_min 
		 FROM(SELECT mean("velocity") AS mean_velocity 
		  FROM readings 
		  WHERE "fleet" = '%s' AND time > '%s' AND time <= '%s' 
		  GROUP BY time(10m),"name","driver") 
		 WHERE "mean_velocity" > 1 
		 GROUP BY "name","driver") 
		WHERE ten_min_mean_velocity > %d`,
		i.GetRandomFleet(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "Influx trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	influxql := `SELECT mean("fuel_consumption") AS "mean_fuel_consumption", mean("nominal_fuel_consumption") AS "nominal_fuel_consumption" 
		FROM "readings" 
		WHERE "velocity" > 1 
		GROUP BY "fleet"`

	humanLabel := "Influx average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) AvgDailyDrivingDuration(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	influxql := fmt.Sprintf(`SELECT count("mv")/6 as "hours driven" 
		FROM (SELECT mean("velocity") as "mv" 
		 FROM "readings" 
		 WHERE time > '%s' AND time < '%s' 
		 GROUP BY time(10m),"fleet", "name", "driver") 
		WHERE time > '%s' AND time < '%s' 
		GROUP BY time(1d),"fleet", "name", "driver"`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "Influx average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) AvgDailyDrivingSession(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	influxql := fmt.Sprintf(`SELECT "elapsed" 
		INTO "random_measure2_1" 
		FROM (SELECT difference("difka"), elapsed("difka", 1m) 
		 FROM (SELECT "difka" 
		  FROM (SELECT difference("mv") AS difka 
		   FROM (SELECT floor(mean("velocity")/10)/floor(mean("velocity")/10) AS "mv" 
		    FROM "readings" 
		    WHERE "name"!='' AND time > '%s' AND time < '%s' 
		    GROUP BY time(10m), "name" fill(0)) 
		   GROUP BY "name") 
		  WHERE "difka"!=0 
		  GROUP BY "name") 
		 GROUP BY "name") 
		WHERE "difference" = -2 
		GROUP BY "name"; 
		SELECT mean("elapsed") 
		FROM "random_measure2_1" 
		WHERE time > '%s' AND time < '%s' 
		GROUP BY time(1d),"name"`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "Influx average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) AvgLoad(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	influxql := `SELECT mean("ml") AS mean_load_percentage 
		FROM (SELECT "current_load"/"load_capacity" AS "ml" 
		 FROM "diagnostics" 
		 GROUP BY "name", "fleet", "model") 
		GROUP BY "fleet", "model"`

	humanLabel := "Influx average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) DailyTruckActivity(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	influxql := fmt.Sprintf(`SELECT count("ms")/144 
		FROM (SELECT mean("status") AS ms 
		 FROM "diagnostics" 
		 WHERE time >= '%s' AND time < '%s' 
		 GROUP BY time(10m), "model", "fleet") 
		WHERE time >= '%s' AND time < '%s' AND "ms"<1 
		GROUP BY time(1d), "model", "fleet"`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "Influx daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) TruckBreakdownFrequency(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	influxql := fmt.Sprintf(`SELECT count("state_changed") 
		FROM (SELECT difference("broken_down") AS "state_changed" 
		 FROM (SELECT floor(2*(sum("nzs")/count("nzs")))/floor(2*(sum("nzs")/count("nzs"))) AS "broken_down" 
		  FROM (SELECT "model", "status"/"status" AS nzs 
		   FROM "diagnostics" 
		   WHERE time >= '%s' AND time < '%s') 
		  WHERE time >= '%s' AND time < '%s' 
		  GROUP BY time(10m),"model") 
		 GROUP BY "model") 
		WHERE "state_changed" = 1 
		GROUP BY "model"`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "Influx truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}

func (i *IoT) OnlyField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(latitude),mean(longitude) FROM "readings" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s' group by time(1m)`,
		interval.StartString(), interval.EndString())
	humanLabel := "Influx Only Field 1 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(1m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 2 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField3(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(fuel_state),mean(fuel_capacity) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(1m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 3 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField4(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state),mean(fuel_capacity) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(1m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 4 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField5(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state),mean(fuel_capacity),mean(load_capacity),mean(nominal_fuel_consumption) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(10m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 5 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField6(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state),mean(fuel_capacity),mean(load_capacity),mean(nominal_fuel_consumption) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(10m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 6 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField7(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state),mean(fuel_capacity),mean(load_capacity),mean(nominal_fuel_consumption) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(10m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 7 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) OnlyField8(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state),mean(fuel_capacity),mean(load_capacity),mean(nominal_fuel_consumption) FROM "diagnostics" WHERE ("name" = 'truck_0') AND TIME >= '%s' AND TIME < '%s'  group by time(10m) `,
		interval.StartString(), interval.EndString())

	humanLabel := "Influx Only Field 8 queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) ReadingsPosition(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "5m"
	} else {
		duration = "15m"
	}

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("readings")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("readings", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(latitude),mean(longitude),mean(elevation) FROM "readings" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)`,
		truckWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{latitude[float64],longitude[float64],elevation[float64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx ReadingsPosition IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) ReadingsPosition2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "5m"
	} else {
		duration = "15m"
	}

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("readings")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("readings", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(latitude),mean(longitude) FROM "readings" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)`,
		truckWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{latitude[float64],longitude[float64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx ReadingsPosition2 IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) DiagnosticsLoad(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "5m"
	} else {
		duration = "15m"
	}

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("diagnostics")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("diagnostics", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(current_load),mean(fuel_state) FROM "diagnostics" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)`,
		truckWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{current_load[float64],fuel_state[float64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx DiagnosticsLoad IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) DiagnosticsPredicate(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("diagnostics")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("diagnostics", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT current_load,fuel_state FROM "diagnostics" WHERE %s AND fuel_state > 0.9 AND current_load > 4500 AND TIME >= '%s' AND TIME < '%s' GROUP BY "name"`,
		truckWhereString, interval.StartString(), interval.EndString())

	influxql += ";"
	influxql += fmt.Sprintf("%s#{current_load[float64],fuel_state[float64]}#{(fuel_state>0.9[float64])(current_load>4500[int64])}#{empty,empty}", tagString)

	humanLabel := "Influx DiagnosticsLoad Predicate IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) ReadingsVelocityAndFuel(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "5m"
	} else {
		duration = "15m"
	}

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("readings")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("readings", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(velocity),mean(fuel_consumption),mean(grade) FROM "readings" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)`,
		truckWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{velocity[float64],fuel_consumption[float64],grade[float64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx ReadingsVelocity IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) ReadingsVelocityAndFuel2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "5m"
	} else {
		duration = "15m"
	}

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("readings")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("readings", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(velocity),mean(fuel_consumption),mean(grade),mean(nominal_fuel_consumption) FROM "readings" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)`,
		truckWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{velocity[float64],fuel_consumption[float64],grade[float64],nominal_fuel_consumption[float64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx ReadingsVelocity 2 IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) ReadingsAvgFuelConsumption(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	duration := ""
	if zipNum < 5 {
		duration = "5m"
	} else {
		duration = "15m"
	}

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("readings")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("readings", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)`,
		truckWhereString, interval.StartString(), interval.EndString(), duration)

	influxql += ";"
	influxql += fmt.Sprintf("%s#{velocity[float64],fuel_consumption[float64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "Influx ReadingsAvgFuelConsumption IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (i *IoT) ReadingsVelocityPredicate(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := i.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	truckWhereString := ""
	tagString := ""
	if !common.RandomTag {
		truckWhereString, tagString = i.getContinuousTruckWhereStringAndTagString("readings")
	} else {
		truckWhereString, tagString = i.getTruckWhereStringAndTagString("readings", common.TagNum)
	}

	influxql = fmt.Sprintf(
		`SELECT velocity,fuel_consumption,grade FROM "readings" WHERE %s AND velocity > 90 AND fuel_consumption > 40 AND TIME >= '%s' AND TIME < '%s' GROUP BY "name"`,
		truckWhereString, interval.StartString(), interval.EndString())

	influxql += ";"
	influxql += fmt.Sprintf("%s#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>90[int64])(fuel_consumption>40[int64])}#{empty,empty}", tagString)

	humanLabel := "Influx ReadingsVelocity with Predicate IoT queries"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

var index int = 0

func (i *IoT) MultiQueries(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {

	switch index % 7 {
	case 0:
		i.ReadingsPosition(qi, zipNum, latestNum, newOrOld)
		break
	case 1:
		i.ReadingsPosition2(qi, zipNum, latestNum, newOrOld)
		break
	case 2:
		i.DiagnosticsLoad(qi, zipNum, latestNum, newOrOld)
		break
	case 3:
		i.ReadingsVelocityAndFuel(qi, zipNum, latestNum, newOrOld)
		break
	case 4:
		i.ReadingsAvgFuelConsumption(qi, zipNum, latestNum, newOrOld)
		break
	case 5:
		i.DiagnosticsPredicate(qi, zipNum, latestNum, newOrOld)
		break
	case 6:
		i.ReadingsVelocityAndFuel2(qi, zipNum, latestNum, newOrOld)
		break
	default:
		i.ReadingsPosition(qi, zipNum, latestNum, newOrOld)
		break
	}

	index++
}

func (i *IoT) IoTQueries(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {

	switch index % 8 {
	case 0:
		i.ReadingsPosition(qi, zipNum, latestNum, newOrOld)
		break
	case 1:
		i.ReadingsPosition2(qi, zipNum, latestNum, newOrOld)
		break
	case 2:
		i.DiagnosticsLoad(qi, zipNum, latestNum, newOrOld)
		break
	case 3:
		i.ReadingsVelocityAndFuel(qi, zipNum, latestNum, newOrOld)
		break
	case 4:
		i.ReadingsAvgFuelConsumption(qi, zipNum, latestNum, newOrOld)
		break
	case 5:
		i.DiagnosticsPredicate(qi, zipNum, latestNum, newOrOld)
		break
	case 6:
		i.ReadingsVelocityAndFuel2(qi, zipNum, latestNum, newOrOld)
		break
	case 7:
		i.ReadingsVelocityPredicate(qi, zipNum, latestNum, newOrOld)
		break
	default:
		i.ReadingsPosition(qi, zipNum, latestNum, newOrOld)
		break
	}

	index++
}

func (i *IoT) OnlyFieldQueries(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {

	switch index % 4 {
	case 0:
		i.OnlyField1(qi, zipNum, latestNum, newOrOld)
		break
	case 1:
		i.OnlyField2(qi, zipNum, latestNum, newOrOld)
		break
	case 2:
		i.OnlyField3(qi, zipNum, latestNum, newOrOld)
		break
	case 3:
		i.OnlyField4(qi, zipNum, latestNum, newOrOld)
		break
	default:
		i.OnlyField1(qi, zipNum, latestNum, newOrOld)
		break
	}

	fmt.Printf("number:\t%d\n", index)
	index++
}
