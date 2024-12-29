package serialize

import (
	"github.com/timescale/tsbs/pkg/data"
	"io"
)

type PointSerializer interface {
	Serialize(p *data.Point, w io.Writer) error
}
