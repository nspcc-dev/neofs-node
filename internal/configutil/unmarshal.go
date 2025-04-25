package configutil

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/mitchellh/mapstructure"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/spf13/viper"
)

type config interface {
	Set(key string)
	Unset(key string)
}

// Unmarshal loads config from Viper into the structure pointed to by cfg.
// Also, assigns all the necessary env variables by `envPrefix`.
// It decodes with custom support for keys.PublicKeys, uint32, and util.Uint160,
// and applies keys.
// Returns an error if decoding fails.
func Unmarshal(v *viper.Viper, cfg config, envPrefix string, decodeFuncs ...mapstructure.DecodeHookFunc) error {
	bindEnvForStruct(v, cfg, envPrefix)

	if err := v.UnmarshalExact(cfg, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			append([]mapstructure.DecodeHookFunc{
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(" "),
				publicKeyHook(),
				uint32StrictHook(),
				uint160Hook(),
			}, decodeFuncs...)...,
		))); err != nil {
		return err
	}
	for _, key := range v.AllKeys() {
		cfg.Set(key)
	}

	return nil
}

// publicKeyHook returns a mapstructure decode hook func that converts a string to a keys.PublicKey.
func publicKeyHook() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data any) (any, error) {
		if t != reflect.TypeOf(keys.PublicKey{}) {
			return data, nil
		}

		str, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("public key field type %T instead of %T", data, str)
		}

		publicKey, err := keys.NewPublicKeyFromString(str)
		if err != nil {
			return nil, fmt.Errorf("public key parsing error: %w", err)
		}
		return publicKey, nil
	}
}

// uint32StrictHook returns a mapstructure decode hook function
// that converts int and int64 to uint32, ensuring they are within the valid range.
func uint32StrictHook() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data any) (any, error) {
		if t != reflect.TypeOf(uint32(0)) {
			return data, nil
		}

		var (
			intVal int64
			err    error
		)
		switch v := data.(type) {
		case int:
			intVal = reflect.ValueOf(v).Int()
		case string:
			intVal, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("field type %T instead of uint32", data)
		}
		if intVal < 0 || intVal > math.MaxUint32 {
			return nil, fmt.Errorf("%d out of allowable range [%d:%d]", intVal, 0, math.MaxUint32)
		}
		return uint32(intVal), nil
	}
}

// uint160Hook returns a mapstructure decode hook func that converts a string to a util.Uint160.
func uint160Hook() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data any) (any, error) {
		if t != reflect.TypeOf(util.Uint160{}) {
			return data, nil
		}

		str, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("hash invalid type %T insted of %T", data, str)
		}
		if str == "" {
			return util.Uint160{}, nil
		}

		addr, err := util.Uint160DecodeStringLE(str)
		if err != nil {
			return nil, fmt.Errorf("can't parse contract address: %w", err)
		}
		return addr, nil
	}
}
