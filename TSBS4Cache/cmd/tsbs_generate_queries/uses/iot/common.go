package iot

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/iot"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	ReadingsTableName                  = "readings"
	DiagnosticsTableName               = "diagnostics"
	StationaryDuration                 = 10 * time.Minute
	LongDrivingSessionDuration         = 4 * time.Hour
	DailyDrivingDuration               = 24 * time.Hour
	LabelLastLoc                       = "last-loc"
	LabelLastLocSingleTruck            = "single-last-loc"
	LabelLowFuel                       = "low-fuel"
	LabelHighLoad                      = "high-load"
	LabelStationaryTrucks              = "stationary-trucks"
	LabelLongDrivingSessions           = "long-driving-sessions"
	LabelLongDailySessions             = "long-daily-sessions"
	LabelAvgVsProjectedFuelConsumption = "avg-vs-projected-fuel-consumption"
	LabelAvgDailyDrivingDuration       = "avg-daily-driving-duration"
	LabelAvgDailyDrivingSession        = "avg-daily-driving-session"
	LabelAvgLoad                       = "avg-load"
	LabelDailyActivity                 = "daily-activity"
	LabelBreakdownFrequency            = "breakdown-frequency"

	LabelIoTQueries = "iot-queries"
)

type Core struct {
	*common.Core
}

func (c Core) GetRandomFleet() string {
	return iot.FleetChoices[rand.Intn(len(iot.FleetChoices))]
}

func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err

}

func (c *Core) GetRandomTrucks(nTrucks int) ([]string, error) {
	return getRandomTrucks(nTrucks, c.Scale)
}

func (c *Core) GetTDRandomTrucks(metric string, nTrucks int) ([]string, error) {
	return getTDRandomTrucks(metric, nTrucks, c.Scale)
}

func (c *Core) GetContinuousRandomTrucks() ([]string, error) {
	return getContinuousRandomTrucks()
}

func getRandomTrucks(numTrucks int, totalTrucks int) ([]string, error) {
	if numTrucks < 1 {
		return nil, fmt.Errorf("number of trucks cannot be < 1; got %d", numTrucks)
	}
	if numTrucks > totalTrucks {
		return nil, fmt.Errorf("number of trucks (%d) larger than total trucks. See --scale (%d)", numTrucks, totalTrucks)
	}

	randomNumbers, err := common.GetRandomSubsetPerm(numTrucks, totalTrucks)
	if err != nil {
		return nil, err
	}

	truckNames := []string{}
	for _, n := range randomNumbers {
		truckNames = append(truckNames, fmt.Sprintf("truck_%d", n))
	}

	return truckNames, nil
}

func getTDRandomTrucks(metric string, numTrucks int, totalTrucks int) ([]string, error) {
	if numTrucks < 1 {
		return nil, fmt.Errorf("number of trucks cannot be < 1; got %d", numTrucks)
	}
	if numTrucks > totalTrucks {
		return nil, fmt.Errorf("number of trucks (%d) larger than total trucks. See --scale (%d)", numTrucks, totalTrucks)
	}

	randomNumbers, err := common.GetRandomSubsetPerm(numTrucks, totalTrucks)
	if err != nil {
		return nil, err
	}

	truckNames := []string{}
	m := metric[0:1]
	for _, n := range randomNumbers {
		truckNames = append(truckNames, fmt.Sprintf("%s_truck_%d", m, n))
	}

	return truckNames, nil
}

func getContinuousRandomTrucks() ([]string, error) {

	randomNumbers, err := common.GetContinuousRandomSubset()
	if err != nil {
		return nil, err
	}

	truckNames := []string{}
	for _, n := range randomNumbers {
		truckNames = append(truckNames, fmt.Sprintf("truck_%d", n))
	}
	return truckNames, nil
}

type IoTQueriesFiller interface {
	IoTQueries(query.Query, int64, int64, int)
}

type LastLocFiller interface {
	LastLocPerTruck(query.Query, int64, int64, int)
}

type LastLocByTruckFiller interface {
	LastLocByTruck(query.Query, int, int64, int64, int)
}

type TruckLowFuelFiller interface {
	TrucksWithLowFuel(query.Query, int64, int64, int)
}

type TruckHighLoadFiller interface {
	TrucksWithHighLoad(query.Query, int64, int64, int)
}

type StationaryTrucksFiller interface {
	StationaryTrucks(query.Query, int64, int64, int)
}

type TruckLongDrivingSessionFiller interface {
	TrucksWithLongDrivingSessions(query.Query, int64, int64, int)
}

type TruckLongDailySessionFiller interface {
	TrucksWithLongDailySessions(query.Query, int64, int64, int)
}

type AvgVsProjectedFuelConsumptionFiller interface {
	AvgVsProjectedFuelConsumption(query.Query, int64, int64, int)
}

type AvgDailyDrivingDurationFiller interface {
	AvgDailyDrivingDuration(query.Query, int64, int64, int)
}

type AvgDailyDrivingSessionFiller interface {
	AvgDailyDrivingSession(query.Query, int64, int64, int)
}

type AvgLoadFiller interface {
	AvgLoad(query.Query, int64, int64, int)
}

type DailyTruckActivityFiller interface {
	DailyTruckActivity(query.Query, int64, int64, int)
}

type TruckBreakdownFrequencyFiller interface {
	TruckBreakdownFrequency(query.Query, int64, int64, int)
}
