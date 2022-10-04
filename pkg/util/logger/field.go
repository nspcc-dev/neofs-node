package logger

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Field is a named value of the log record's context.
// Instances MUST be created using Field* methods.
type Field struct {
	f zap.Field
}

// FieldUint constructs named unsigned integer field for the log record.
func FieldUint(name string, val uint64) Field {
	return Field{f: zap.Uint64(name, val)}
}

// FieldInt constructs named signed integer field for the log record.
func FieldInt(name string, val int64) Field {
	return Field{f: zap.Int64(name, val)}
}

// FieldFloat constructs named floating-point number field for the log record.
func FieldFloat(name string, val float64) Field {
	return Field{f: zap.Float64(name, val)}
}

// FieldString constructs named string field for the log record.
func FieldString(name string, val string) Field {
	return Field{f: zap.String(name, val)}
}

// FieldBool constructs named boolean field for the log record.
func FieldBool(name string, val bool) Field {
	return Field{f: zap.Bool(name, val)}
}

// FieldStringer constructs named fmt.Stringer field for the log record.
func FieldStringer(name string, val fmt.Stringer) Field {
	return Field{f: zap.Stringer(name, val)}
}

// FieldSlice constructs named slice field for the log record.
func FieldSlice(name string, val interface{}) Field {
	return Field{f: zap.Any(name, val)}
}

// FieldDuration constructs named duration field for the log record.
func FieldDuration(name string, val time.Duration) Field {
	return Field{f: zap.Duration(name, val)}
}

// FieldError constructs error field for the log record. Error MUST NOT be nil.
func FieldError(val error) Field {
	return Field{f: zap.String("error", val.Error())}
}
