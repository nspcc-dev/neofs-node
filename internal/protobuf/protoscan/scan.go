package protoscan

import (
	"errors"
	"fmt"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"google.golang.org/protobuf/encoding/protowire"
)

// ErrContinue is a continuation error.
var ErrContinue = errors.New("continue")

type commonOptions = struct {
	InterceptUint32 func(protowire.Number, uint32) error
	InterceptUint64 func(protowire.Number, uint64) error
	InterceptEnum   func(protowire.Number, int32) error
	InterceptBool   func(protowire.Number, bool) error
	InterceptString func(protowire.Number, []byte) error

	// If InterceptBytes returns nil, [ScanMessageOrdered] does not verify its
	// specified format. In this case, the caller is responsible for processing the
	// field itself. To continue scanning the field as usual, the function should
	// return [ErrContinue].
	InterceptBytes func(protowire.Number, iprotobuf.BuffersSlice) error
}

// ScanMessageOrderedOptions groups optional [ScanMessageOrdered] parameters.
//
// Interceptors allow to intercept typed field values to handle them
// specifically. If the function returns an error, [ScanMessageOrdered]
// immediately returns it as is generally.
type ScanMessageOrderedOptions struct {
	commonOptions

	// If InterceptNested returns nil, [ScanMessageOrdered] does not scan its
	// argument. In this case, the caller is responsible for processing the field
	// itself. To continue scanning the field as usual, the function should return
	// [ErrContinue].
	InterceptNested func(protowire.Number, iprotobuf.BuffersSlice, bool) (bool, error)
}

// ScanMessageOrdered checks whether buffers contain a complete and valid
// Protocol Buffers V3 message according to the given scheme. It goes over each
// field one-by-one and checks that it is encoded correctly. If the field is
// unknown, ScanMessageOrdered fails. Field repetition is unchecked. Empty
// buffers is not an error.
//
// Boolean return is a flag of direct field order: it states whether fields are
// arranged in ascending numerical order in all messages at all nesting levels.
func ScanMessageOrdered(buffers iprotobuf.BuffersSlice, scheme MessageScheme, opts ScanMessageOrderedOptions) (bool, error) {
	return scanMessageOrdered(buffers, scheme, true, opts)
}

// ScanMessageOptions groups optional [ScanMessage] parameters.
//
// Interceptors allow to intercept typed field values of fields declared in
// message scheme to handle them specifically. If the function returns an error,
// [ScanMessage] immediately returns it as is generally.
type ScanMessageOptions struct {
	commonOptions
	// If InterceptNested returns nil, [ScanMessage] does not scan its argument. In
	// this case, the caller is responsible for processing the field itself. To
	// continue scanning the field as usual, the function should return
	// [ErrContinue].
	InterceptNested func(protowire.Number, iprotobuf.BuffersSlice) error
}

// ScanMessage is an alternative for [ScanMessageOrdered] when field order does
// not matter.
func ScanMessage(buffers iprotobuf.BuffersSlice, scheme MessageScheme, opts ScanMessageOptions) error {
	var orderedOpts ScanMessageOrderedOptions
	orderedOpts.commonOptions = opts.commonOptions
	if opts.InterceptNested != nil {
		orderedOpts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice, _ bool) (bool, error) {
			return false, opts.InterceptNested(num, buffers)
		}
	}

	_, err := scanMessageOrdered(buffers, scheme, false, orderedOpts)
	return err
}

func scanMessageOrdered(buffers iprotobuf.BuffersSlice, scheme MessageScheme, checkOrder bool, opts ScanMessageOrderedOptions) (bool, error) {
	var prevNum protowire.Number

	for !buffers.IsEmpty() {
		num, wireTyp, err := buffers.ParseTag()
		if err != nil {
			return false, fmt.Errorf("parse next tag: %w", err)
		}

		if checkOrder {
			if num < prevNum {
				checkOrder = false
			} else {
				prevNum = num
			}
		}

		// TODO: support all types
		switch f := scheme.Fields[num]; f.typ {
		default:
			return false, iprotobuf.NewUnsupportedFieldError(num, wireTyp)
		case FieldTypeBool:
			v, err := buffers.ParseBoolField(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
			if opts.InterceptBool != nil {
				if err = opts.InterceptBool(num, v); err != nil {
					return false, err
				}
			}
		case FieldTypeUint32:
			v, err := buffers.ParseUint32Field(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
			if opts.InterceptUint32 != nil {
				if err = opts.InterceptUint32(num, v); err != nil {
					return false, err
				}
			}
		case FieldTypeUint64:
			v, err := buffers.ParseUint64Field(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
			if opts.InterceptUint64 != nil {
				if err = opts.InterceptUint64(num, v); err != nil {
					return false, err
				}
			}
		case FieldTypeEnum:
			e, err := buffers.ParseEnumField(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
			if opts.InterceptEnum != nil {
				if err = opts.InterceptEnum(num, e); err != nil {
					return false, err
				}
			}
		case FieldTypeString:
			v, err := buffers.ParseStringField(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
			if opts.InterceptString != nil {
				if err = opts.InterceptString(num, v); err != nil {
					return false, err
				}
			}
		case FieldTypeBytes:
			v, err := buffers.ParseLENField(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
			if opts.InterceptBytes != nil {
				if err = opts.InterceptBytes(num, v); err == nil {
					break
				} else if !errors.Is(err, ErrContinue) {
					return false, err
				}
			}
			if kind, ok := scheme.BinaryFields[num]; ok {
				if err = verifyBinaryField(kind, v); err != nil {
					return false, newParseFieldError(f, fmt.Errorf("invalid binary field of %s kind: %w", kind, err))
				}
			}
		case FieldTypeNestedMessage:
			v, err := buffers.ParseLENField(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}

			if opts.InterceptNested != nil {
				if checkOrder, err = opts.InterceptNested(num, v, checkOrder); err == nil {
					break
				} else if !errors.Is(err, ErrContinue) {
					return false, err
				}
			}

			if num == scheme.RecursiveField {
				if checkOrder, err = scanMessageOrdered(v, scheme, checkOrder, ScanMessageOrderedOptions{}); err != nil {
					return false, newParseFieldError(f, err)
				}
				break
			}

			if nestedScheme, ok := scheme.NestedMessageFields[num]; ok {
				if checkOrder, err = scanMessageOrdered(v, nestedScheme, checkOrder, ScanMessageOrderedOptions{}); err != nil {
					return false, newParseFieldError(f, err)
				}
				break
			}

			if alias, ok := scheme.NestedMessageAliases[num]; ok {
				if checkOrder, err = scanMessageOrdered(v, resolveScheme(alias), checkOrder, ScanMessageOrderedOptions{}); err != nil {
					return false, newParseFieldError(f, err)
				}
				break
			}

			return false, fmt.Errorf("format of nested message field %s is not specified", f)
		case FieldTypeRepeatedEnum:
			err := buffers.SkipRepeatedEnum(num, wireTyp)
			if err != nil {
				return false, newParseFieldError(f, err)
			}
		}
	}

	return checkOrder, nil
}
