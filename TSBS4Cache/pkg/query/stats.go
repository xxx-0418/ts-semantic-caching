package query

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/HdrHistogram/hdrhistogram-go"
)

var (
	hdrScaleFactor = 1e3
)

type Stat struct {
	label      []byte
	value      float64
	isWarm     bool
	isPartial  bool
	byteLength uint64
	hitKind    uint8
}

var statPool = &sync.Pool{
	New: func() interface{} {
		return &Stat{
			label: make([]byte, 0, 1024),
			value: 0.0,
		}
	},
}

func GetStat() *Stat {
	return statPool.Get().(*Stat).reset()
}

func GetPartialStat() *Stat {
	s := GetStat()
	s.isPartial = true
	return s
}

func (s *Stat) Init(label []byte, value float64) *Stat {
	s.label = s.label[:0]
	s.label = append(s.label, label...)
	s.value = value
	s.isWarm = false
	return s
}

func (s *Stat) InitWithParam(label []byte, value float64, byteLength uint64, hitKind uint8) *Stat {
	s.label = s.label[:0]
	s.label = append(s.label, label...)
	s.value = value
	s.isWarm = false
	s.byteLength = byteLength
	s.hitKind = hitKind
	return s
}

func (s *Stat) reset() *Stat {
	s.label = s.label[:0]
	s.value = 0.0
	s.isWarm = false
	s.isPartial = false
	return s
}

type statGroup struct {
	latencyHDRHistogram *hdrhistogram.Histogram
	sum                 float64
	count               int64
}

func newStatGroup(size uint64) *statGroup {
	lH := hdrhistogram.New(1, 3600000000, 4)
	return &statGroup{
		latencyHDRHistogram: lH,
		sum:                 0,
		count:               0,
	}
}

func (s *statGroup) push(n float64) {
	s.latencyHDRHistogram.RecordValue(int64(n * hdrScaleFactor))
	s.sum += n
	s.count++
}

func (s *statGroup) string() string {
	vl := fmt.Sprintf("min: %8.2fms, med: %8.2fms, mean: %8.2fms, max: %7.2fms, stddev: %8.2fms, sum: %5.1fsec, count: %d",
		s.Min(),
		s.Median(),
		s.Mean(),
		s.Max(),
		s.StdDev(),
		s.sum/hdrScaleFactor,
		s.count)
	vi := ""
	return vl + vi
}

func (s *statGroup) write(w io.Writer) error {
	_, err := fmt.Fprintln(w, s.string())
	return err
}

func (s *statGroup) Median() float64 {
	return float64(s.latencyHDRHistogram.ValueAtQuantile(50.0)) / hdrScaleFactor
}

func (s *statGroup) Mean() float64 {
	return float64(s.latencyHDRHistogram.Mean()) / hdrScaleFactor
}

func (s *statGroup) Max() float64 {
	return float64(s.latencyHDRHistogram.Max()) / hdrScaleFactor
}

func (s *statGroup) Min() float64 {
	return float64(s.latencyHDRHistogram.Min()) / hdrScaleFactor
}

func (s *statGroup) StdDev() float64 {
	return float64(s.latencyHDRHistogram.StdDev()) / hdrScaleFactor
}

func writeStatGroupMap(w io.Writer, statGroups map[string]*statGroup) error {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		paddedKey := k
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}

		_, err := fmt.Fprintf(w, "%s:\n", paddedKey)
		if err != nil {
			return err
		}

		err = v.write(w)
		if err != nil {
			return err
		}
	}
	return nil
}
