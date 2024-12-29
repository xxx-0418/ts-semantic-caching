package common

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalutils "github.com/timescale/tsbs/internal/utils"
)

const (
	errMoreItemsThanScale = "cannot get random permutation with more items than scale"
)

type Core struct {
	Interval *internalutils.TimeInterval
	Scale    int
}

func NewCore(start, end time.Time, scale int) (*Core, error) {
	ti, err := internalutils.NewTimeInterval(start, end)
	if err != nil {
		return nil, err
	}

	return &Core{Interval: ti, Scale: scale}, nil
}

func PanicUnimplementedQuery(dg utils.QueryGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}

func GetRandomSubsetPerm(numItems int, totalItems int) ([]int, error) {
	if numItems > totalItems {
		// Cannot make a subset longer than the original set
		return nil, fmt.Errorf(errMoreItemsThanScale)
	}

	seen := map[int]bool{}
	res := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		for {
			n := rand.Intn(totalItems)
			if !seen[n] {
				seen[n] = true
				res[i] = n
				break
			}
		}
	}
	return res, nil
}

var ResNum int = 10

func GetContinuousRandomSubset() ([]int, error) {
	resNum := ResNum
	result := make([]int, resNum)
	section := rand.Intn(19) * 5 * resNum / 10
	for i := 0; i < resNum; i++ {
		result[i] = section
		section++
	}
	return result, nil
}
