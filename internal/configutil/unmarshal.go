package configutil

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"

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
func Unmarshal(v *viper.Viper, cfg config, envPrefix string) error {
	bindEnvForStruct(v, cfg, envPrefix)

	if err := v.UnmarshalExact(cfg, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			publicKeyHook(),
			uint32StrictHook(),
			uint160Hook(),
		))); err != nil {
		return err
	}
	for _, key := range v.AllKeys() {
		cfg.Set(key)
	}

	return nil
}

// bindEnvForStruct goes through all fields of the structure,
// searches for the corresponding env variables by tags, and calls viper.Set.
func bindEnvForStruct(v *viper.Viper, s any, envPrefix string) {
	bindFields(v, reflect.ValueOf(s), "", envPrefix)
}

func bindFields(v *viper.Viper, val reflect.Value, prefix string, envPrefix string) {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return
	}

	typ := val.Type()
	for i := range val.NumField() {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		tag := field.Tag.Get("mapstructure")
		if tag == "" || tag == "-" {
			continue
		}

		tagParts := strings.Split(tag, ",")
		fieldKey := tagParts[0]
		isSquash := false
		for _, part := range tagParts[1:] {
			if part == "squash" {
				isSquash = true
				break
			}
		}

		var viperKey string
		if !isSquash && prefix != "" {
			viperKey = prefix + "." + fieldKey
		} else if !isSquash {
			viperKey = fieldKey
		} else {
			viperKey = prefix
		}

		if !isSquash && fieldVal.Kind() != reflect.Struct && (fieldVal.Kind() != reflect.Ptr || fieldVal.Elem().Kind() != reflect.Struct) {
			envKey := strings.ToUpper(envPrefix + "_" + strings.ReplaceAll(viperKey, ".", "_"))
			envValue := os.Getenv(envKey)
			if envValue != "" {
				v.Set(viperKey, envValue)
			}
		}

		if fieldVal.Kind() == reflect.Struct || (fieldVal.Kind() == reflect.Ptr && fieldVal.Elem().Kind() == reflect.Struct) {
			bindFields(v, fieldVal, viperKey, envPrefix)
		}
	}
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
		if t.Kind() != reflect.Uint32 {
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
