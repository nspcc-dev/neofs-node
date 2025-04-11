package internal

import (
	"fmt"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/spf13/cast"
)

// ModeHook returns a mapstructure decode hook func that converts a string to a mode.Mode.
func ModeHook() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data any) (any, error) {
		if t != reflect.TypeOf(mode.Mode(0)) {
			return data, nil
		}

		str := cast.ToString(data)
		var m mode.Mode
		switch str {
		case "read-write", "":
			m = mode.ReadWrite
		case "read-only":
			m = mode.ReadOnly
		case "degraded":
			m = mode.Degraded
		case "degraded-read-only":
			m = mode.DegradedReadOnly
		case "disabled":
			m = mode.Disabled
		default:
			return nil, fmt.Errorf("unknown shard mode: %s", str)
		}

		return m, nil
	}
}
