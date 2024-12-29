package common

import "math/rand"

func RandomStringSliceChoice(s []string) string {
	return s[rand.Intn(len(s))]
}

func RandomByteStringSliceChoice(s [][]byte) []byte {
	return s[rand.Intn(len(s))]
}

func RandomInt64SliceChoice(s []int64) int64 {
	return s[rand.Intn(len(s))]
}

const (
	UseCaseCPUOnly       = "cpu-only"
	UseCaseCPUSingle     = "cpu-single"
	UseCaseDevops        = "devops"
	UseCaseIoT           = "iot"
	UseCaseDevopsGeneric = "devops-generic"
)

var UseCaseChoices = []string{
	UseCaseCPUOnly,
	UseCaseCPUSingle,
	UseCaseDevops,
	UseCaseIoT,
	UseCaseDevopsGeneric,
}
