package configutil

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"

	"github.com/spf13/viper"
)

// bindEnvForStruct goes through all fields of the structure,
// searches for the corresponding env variables by tags, and calls viper.Set.
func bindEnvForStruct(v *viper.Viper, s any, envPrefix string) {
	if envPrefix != "" {
		envPrefix = strings.ToUpper(envPrefix)
	}
	processStruct(v, reflect.ValueOf(s), "", envPrefix)
}

func processStruct(v *viper.Viper, val reflect.Value, prefix, envPrefix string) {
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
		key := tagParts[0]
		isSquash := slices.Contains(tagParts[1:], "squash")

		viperKey := resolveViperKey(prefix, key, isSquash)

		switch {
		case isStructSlice(fieldVal):
			list := processStructSlice(v, fieldVal.Type().Elem(), viperKey, envPrefix)
			if len(list) > 0 {
				v.Set(viperKey, list)
			}
		case isStruct(fieldVal):
			processStruct(v, fieldVal, viperKey, envPrefix)
		case fieldVal.Kind() == reflect.Slice && !isStruct(reflect.New(fieldVal.Type().Elem()).Elem()):
			updated := processPrimitiveSlice(v, viperKey, envPrefix)
			if updated != nil {
				v.Set(viperKey, updated)
			}
			fallthrough
		default:
			envKey := buildEnvKey(envPrefix, makeEnvKey(viperKey))
			if val := os.Getenv(envKey); val != "" {
				v.Set(viperKey, val)
			}
		}
	}
}

func processStructSlice(v *viper.Viper, elemType reflect.Type, baseKey, envPrefix string) []map[string]any {
	var result []map[string]any

	sla := v.Get(baseKey)
	var sl []any
	if sla != nil {
		sl = sla.([]any)
	}
	var updated bool
	for i := 0; ; i++ {
		idxPrefix := fmt.Sprintf("%s_%d", makeEnvKey(baseKey), i)
		item := map[string]any{}
		if i < len(sl) {
			item = sl[i].(map[string]any)
		}
		found := false

		for j := range elemType.NumField() {
			field := elemType.Field(j)
			tag := field.Tag.Get("mapstructure")
			if tag == "" || tag == "-" {
				continue
			}

			envKey := buildEnvKey(envPrefix, idxPrefix, tag)
			viperKey := fmt.Sprintf("%s.%d.%s", baseKey, i, tag)
			fieldType := indirectType(field.Type)

			switch {
			case isStructSliceType(fieldType):
				subList := processStructSlice(v, fieldType.Elem(), viperKey, envPrefix)
				if len(subList) > 0 {
					item[tag] = subList
					found = true
				}
			case fieldType.Kind() == reflect.Struct:
				subStruct := reflect.New(fieldType).Elem()
				if subMap, ok := collectStructFields(v, subStruct, buildEnvKey(envPrefix, idxPrefix, tag)); ok {
					item[tag] = subMap
					found = true
				}
			default:
				if val := os.Getenv(envKey); val != "" {
					item[tag] = val
					found = true
				}
			}
		}

		if !found && i >= len(sl) {
			break
		}
		if found {
			updated = true
		}
		result = append(result, item)
	}

	if updated {
		return result
	}
	return nil
}

func collectStructFields(v *viper.Viper, val reflect.Value, envPrefix string) (map[string]any, bool) {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, false
	}

	result := make(map[string]any)
	found := false

	typ := val.Type()
	for i := range val.NumField() {
		field := typ.Field(i)
		tag := field.Tag.Get("mapstructure")
		if tag == "" || tag == "-" {
			continue
		}

		fieldType := indirectType(field.Type)
		envKey := buildEnvKey(envPrefix, tag)

		switch {
		case isStructSliceType(fieldType):
			var subList []map[string]any
			for j := 0; ; j++ {
				prefix := buildEnvKey(envPrefix, tag, fmt.Sprint(j))
				subStruct := reflect.New(fieldType.Elem()).Elem()
				if subMap, ok := collectStructFields(v, subStruct, prefix); ok {
					subList = append(subList, subMap)
					found = true
				} else {
					break
				}
			}
			if len(subList) > 0 {
				result[tag] = subList
			}
		case fieldType.Kind() == reflect.Struct:
			subStruct := reflect.New(fieldType).Elem()
			if subMap, ok := collectStructFields(v, subStruct, envKey); ok {
				result[tag] = subMap
				found = true
			}
		default:
			if val := os.Getenv(envKey); val != "" {
				result[tag] = val
				found = true
			}
		}
	}

	return result, found
}

func processPrimitiveSlice(v *viper.Viper, key, envPrefix string) []any {
	raw := v.Get(key)
	existing, _ := raw.([]any)

	updated := false
	for i := 0; ; i++ {
		envKey := buildEnvKey(envPrefix, makeEnvKey(key), fmt.Sprint(i))
		val, ok := os.LookupEnv(envKey)
		if !ok {
			if i >= len(existing) {
				break
			}
			continue
		}

		if i >= len(existing) {
			existing = append(existing, val)
		} else {
			existing[i] = val
		}
		updated = true
	}

	if updated {
		return existing
	}
	return nil
}

func isStruct(val reflect.Value) bool {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return val.Kind() == reflect.Struct
}

func isStructSlice(val reflect.Value) bool {
	return val.Kind() == reflect.Slice && isStruct(reflect.New(val.Type().Elem()).Elem())
}

func isStructSliceType(t reflect.Type) bool {
	return t.Kind() == reflect.Slice && indirectType(t.Elem()).Kind() == reflect.Struct
}

func indirectType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

func resolveViperKey(prefix, key string, squash bool) string {
	if squash {
		return prefix
	}
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

func makeEnvKey(key string) string {
	return strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

func buildEnvKey(parts ...string) string {
	var result []string
	for _, part := range parts {
		result = append(result, strings.ToUpper(part))
	}
	return strings.Join(result, "_")
}
