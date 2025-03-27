package configvalidator

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrUnknownField returns when an unknown field appears in the config.
var ErrUnknownField = errors.New("unknown field")

// CheckForUnknownFields validates the config struct for unknown fields with the config map.
// If the field in the config is a map of a key and some value, need to use `mapstructure:,remain` and
// `prefix` tag with the key prefix, even it is empty string.
func CheckForUnknownFields(configMap map[string]any, config any) error {
	return checkForUnknownFields(configMap, config, "")
}

func checkForUnknownFields(configMap map[string]any, config any, currentPath string) error {
	expectedFields := getFieldsFromStruct(config)

	for key, val := range configMap {
		fullPath := key
		if currentPath != "" {
			fullPath = currentPath + "." + key
		}

		_, other := expectedFields[",remain"]
		_, exists := expectedFields[key]
		if !exists && !other {
			return fmt.Errorf("%w: %s", ErrUnknownField, fullPath)
		}

		var fieldVal reflect.Value
		if other && !exists {
			fieldVal = reflect.ValueOf(config).
				FieldByName(getStructFieldByTag(config, "prefix", key, strings.HasPrefix))
		} else {
			fieldVal = reflect.ValueOf(config).
				FieldByName(getStructFieldByTag(config, "mapstructure", key, func(a, b string) bool {
					return a == b
				}))
		}

		switch fieldVal.Kind() {
		case reflect.Slice:
			if fieldVal.Len() == 0 {
				return nil
			}
			fieldVal = fieldVal.Index(0)
		case reflect.Map:
			fieldVal = fieldVal.MapIndex(reflect.ValueOf(key))
		default:
		}

		if !fieldVal.IsValid() {
			return fmt.Errorf("%w: %s", ErrUnknownField, fullPath)
		}

		nestedMap, okMap := val.(map[string]any)
		nestedSlice, okSlice := val.([]any)
		if (okMap || okSlice) && fieldVal.Kind() == reflect.Struct {
			if okSlice {
				for _, slice := range nestedSlice {
					if nestedMap, okMap = slice.(map[string]any); okMap {
						if err := checkForUnknownFields(nestedMap, fieldVal.Interface(), fullPath); err != nil {
							return err
						}
					}
				}
			} else if err := checkForUnknownFields(nestedMap, fieldVal.Interface(), fullPath); err != nil {
				return err
			}
		} else if okMap != (fieldVal.Kind() == reflect.Struct) {
			return fmt.Errorf("%w: %s", ErrUnknownField, fullPath)
		}
	}

	return nil
}

func getFieldsFromStruct(config any) map[string]struct{} {
	fields := make(map[string]struct{})
	t := reflect.TypeOf(config)
	for i := range t.NumField() {
		field := t.Field(i)
		if jsonTag := field.Tag.Get("mapstructure"); jsonTag != "" {
			fields[jsonTag] = struct{}{}
		} else {
			fields[field.Name] = struct{}{}
		}
	}
	return fields
}

func getStructFieldByTag(config any, tagKey, tagValue string, comparison func(a, b string) bool) string {
	t := reflect.TypeOf(config)
	for i := range t.NumField() {
		field := t.Field(i)
		if jsonTag, ok := field.Tag.Lookup(tagKey); comparison(tagValue, jsonTag) && ok {
			return field.Name
		}
	}
	return ""
}
