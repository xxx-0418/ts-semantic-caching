package serialize

import (
	"fmt"
	"strconv"
)

func FastFormatAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', -1, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, v.([]byte)...)
		return buf
	case string:
		buf = append(buf, v.(string)...)
		return buf
	case nil:
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
