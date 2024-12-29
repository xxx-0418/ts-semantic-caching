package main

import (
	"github.com/timescale/tsbs/zipfian/distributionGenerator"
	"math/rand"
	"time"
)

func main() {
	zipfian := distributionGenerator.NewZipfianWithRange(1, 10, distributionGenerator.ZipfianConstant)
	rz := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		println(zipfian.Next(rz))
	}
}
