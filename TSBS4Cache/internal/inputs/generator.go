package inputs

import (
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type Generator interface {
	Generate(common.GeneratorConfig) error
}
