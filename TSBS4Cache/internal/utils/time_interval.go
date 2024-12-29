package utils

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	ErrEndBeforeStart = "end time before start time"

	errWindowTooLargeFmt = "random window equal to or larger than TimeInterval: window %v, interval %v"

	minute = time.Minute
	hour   = time.Hour
	day    = 24 * time.Hour
	week   = 7 * day
	month  = 4*week + 2*day
)

var ZipFianTimeDuration = []time.Duration{
	6 * hour, 12 * hour, 1 * day, 2 * day, 3 * day, 5 * day, 1 * week, 2 * week, 3 * week, 1 * month,
}

type TimeInterval struct {
	start time.Time
	end   time.Time
}

func NewTimeInterval(start, end time.Time) (*TimeInterval, error) {
	if end.Before(start) {
		return nil, fmt.Errorf(ErrEndBeforeStart)
	}
	return &TimeInterval{start.UTC(), end.UTC()}, nil
}

func (ti *TimeInterval) Duration() time.Duration {
	return ti.end.Sub(ti.start)
}

func (ti *TimeInterval) Overlap(other *TimeInterval) bool {
	s1 := ti.Start()
	e1 := ti.End()

	s2 := other.Start()
	e2 := other.End()

	if e1 == s2 || e2 == s1 {
		return false
	}

	if s1.Before(s2) && e1.Before(s2) {
		return false
	}

	if s2.Before(s1) && e2.Before(s1) {
		return false
	}

	return true
}

func (ti *TimeInterval) RandWindow(window time.Duration) (*TimeInterval, error) {
	lower := ti.start.UnixNano()
	upper := ti.end.Add(-window).UnixNano()

	if upper <= lower {
		return nil, fmt.Errorf(errWindowTooLargeFmt, window, ti.end.Sub(ti.start))

	}

	start := lower + rand.Int63n(upper-lower)
	end := start + window.Nanoseconds()

	x, err := NewTimeInterval(time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	} else if x.Duration() != window {
		// Unless the logic above this changes, this should not happen, so
		// we panic in that case.
		panic("generated TimeInterval's duration does not equal window")
	}

	return x, nil
}

func (ti *TimeInterval) MustRandWindow(window time.Duration) *TimeInterval {
	res, err := ti.RandWindow(window)
	if err != nil {
		panic(err.Error())
	}
	return res
}

func (ti *TimeInterval) DistributionRandWithOldData(zipNum int64, latestNum int64, newOrOld int) *TimeInterval {
	duration := ZipFianTimeDuration[zipNum].Nanoseconds()

	if newOrOld == 0 {
		totalStartTime := ti.start.UnixNano()
		totalEndTime := ti.end.UnixNano() - time.Second.Nanoseconds()

		queryEndTime := totalEndTime - ((time.Hour.Nanoseconds() * 3) * (365*2*4 - latestNum - 1))
		queryStartTime := queryEndTime - duration
		if queryStartTime < totalStartTime {
			queryStartTime = totalStartTime
		}

		if queryEndTime <= queryStartTime {
			queryEndTime = totalEndTime
			queryStartTime = queryEndTime - 24*time.Hour.Nanoseconds()
		}

		x, err := NewTimeInterval(time.Unix(0, queryStartTime), time.Unix(0, queryEndTime))
		if err != nil {
			panic(err.Error())
		}

		return x
	} else {
		totalStartTime := ti.start.UnixNano()
		totalEndTime := totalStartTime + time.Hour.Nanoseconds()*24*90

		queryEndTime := totalEndTime - ((time.Hour.Nanoseconds() * 3) * (90*2*4 - latestNum - 1))
		queryStartTime := queryEndTime - duration
		if queryStartTime < totalStartTime {
			queryStartTime = totalStartTime
		}

		if queryEndTime <= queryStartTime {
			queryEndTime = totalEndTime
			queryStartTime = queryEndTime - 24*time.Hour.Nanoseconds()
		}

		x, err := NewTimeInterval(time.Unix(0, queryStartTime), time.Unix(0, queryEndTime))
		if err != nil {
			panic(err.Error())
		}

		return x
	}

}

func (ti *TimeInterval) Start() time.Time {
	return ti.start
}

func (ti *TimeInterval) StartUnixNano() int64 {
	return ti.start.UnixNano()
}

func (ti *TimeInterval) StartUnixMillis() int64 {
	return ti.start.UTC().UnixNano() / int64(time.Millisecond)
}

func (ti *TimeInterval) StartString() string {
	return ti.start.Format(time.RFC3339)
}

func (ti *TimeInterval) End() time.Time {
	return ti.end
}

func (ti *TimeInterval) EndUnixNano() int64 {
	return ti.end.UnixNano()
}

func (ti *TimeInterval) EndUnixMillis() int64 {
	return ti.end.UTC().UnixNano() / int64(time.Millisecond)
}

func (ti *TimeInterval) EndString() string {
	return ti.end.Format(time.RFC3339)
}
