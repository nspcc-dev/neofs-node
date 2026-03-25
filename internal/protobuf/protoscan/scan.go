package protoscan

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neofs-node/internal/protobuf"
	"google.golang.org/protobuf/encoding/protowire"
)

// ErrContinue is a continuation error.
var ErrContinue = errors.New("continue")

// ScanMessageOrderedOptions groups optional [ScanMessageOrdered] parameters.
//
// Interceptors allow to intercept typed field values of fields declared in
// message scheme to handle them specifically. If the function returns an error,
// [ScanMessageOrdered] immediately returns it as is generally.
type ScanMessageOrderedOptions struct {
	// If InterceptNested returns nil, [ScanMessageOrdered] does not scan its
	// argument. In this case, the caller is responsible for processing the field
	// itself. To continue scanning the field as usual, the function should return
	// [ErrContinue].
	InterceptNested func(protowire.Number, []byte, bool) (bool, error)
	InterceptUint32 func(protowire.Number, uint32) error
	InterceptUint64 func(protowire.Number, uint64) error
	InterceptBytes  func(protowire.Number, []byte) error
}

// ScanMessageOrdered checks whether buf contains a complete and valid Protocol
// Buffers V3 message according to the given scheme. It goes over each field
// one-by-one and checks that it is encoded correctly. If the field format is
// declared in the schema, ScanMessageOrdered additionally checks for compliance
// with it. If the field is unknown, ScanMessageOrdered simply checks the
// encoding and skips it. Field repetition is not checked.
//
// Package provides NeoFS API protocol schemes. With a zero scheme,
// ScanMessageOrdered verifies that buf is a valid Protocol Buffers V3 message
// overall.
//
// Boolean returns is a flag of direct field order: it states whether fields are
// arranged in ascending numerical order in all messages at all nesting levels.
func ScanMessageOrdered(buf []byte, scheme MessageScheme, opts ScanMessageOrderedOptions) (bool, error) {
	return scanMessageOrdered(buf, scheme, true, opts)
}

// ScanMessageOptions groups optional [ScanMessage] parameters.
//
// Interceptors allow to intercept typed field values of fields declared in
// message scheme to handle them specifically. If the function returns an error,
// [ScanMessage] immediately returns it as is generally.
type ScanMessageOptions struct {
	// If InterceptNested returns nil, [ScanMessage] does not scan its argument. In
	// this case, the caller is responsible for processing the field itself. To
	// continue scanning the field as usual, the function should return
	// [ErrContinue].
	InterceptNested func(protowire.Number, []byte) error
	InterceptUint32 func(protowire.Number, uint32) error
	InterceptUint64 func(protowire.Number, uint64) error
	InterceptBytes  func(protowire.Number, []byte) error
}

// ScanMessage is an alternative for [ScanMessageOrdered] when field order does
// not matter.
func ScanMessage(buf []byte, scheme MessageScheme, opts ScanMessageOptions) error {
	var orderedOpts ScanMessageOrderedOptions
	orderedOpts.InterceptUint32 = opts.InterceptUint32
	orderedOpts.InterceptBytes = opts.InterceptBytes
	if opts.InterceptNested != nil {
		orderedOpts.InterceptNested = func(num protowire.Number, b []byte, _ bool) (bool, error) {
			return false, opts.InterceptNested(num, b)
		}
	}

	_, err := scanMessageOrdered(buf, scheme, false, orderedOpts)
	return err
}

func scanMessageOrdered(buf []byte, scheme MessageScheme, checkOrder bool, opts ScanMessageOrderedOptions) (bool, error) {
	var prevNum protowire.Number

	var off int
	for off != len(buf) {
		num, wireTyp, n, err := protobuf.ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if checkOrder {
			if num < prevNum {
				checkOrder = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch f, _ := scheme.fields[num]; f.typ {
		default:
			if n, err = protobuf.SkipField(buf[off:], num, wireTyp); err != nil {
				return false, err
			}
		case fieldTypeUint32:
			var u uint32
			if u, n, err = protobuf.ParseUint32Field(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}

			if opts.InterceptUint32 != nil {
				if err = opts.InterceptUint32(num, u); err != nil {
					return false, err
				}
			}
		case fieldTypeUint64:
			var u uint64
			if u, n, err = protobuf.ParseUint64Field(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}

			if opts.InterceptUint64 != nil {
				if err = opts.InterceptUint64(num, u); err != nil {
					return false, err
				}
			}
		case fieldTypeEnum:
			if _, n, err = protobuf.ParseEnumField[int32](buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}
		case fieldTypeBool:
			if _, err = protobuf.ParseBoolField(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}
			n = 1
		case fieldTypeRepeatedEnum:
			if n, err = protobuf.SkipRepeatedEnum(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}
		case fieldTypeString:
			var ln int
			if ln, n, err = protobuf.ParseStringField(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}
			n += ln
		case fieldTypeBytes:
			var ln int
			if ln, n, err = protobuf.ParseLENField(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}

			b := buf[off+n:][:ln]

			if opts.InterceptBytes != nil {
				if err = opts.InterceptBytes(num, b); err != nil {
					return false, err
				}
			}

			if kind, ok := scheme.binaryKindFields[num]; ok {
				if err = verifyBinaryField(kind, b); err != nil {
					return false, newParseFieldError(f, fmt.Errorf("invalid binary field of %s kind: %w", kind, err))
				}
			}

			n += ln
		case fieldTypeNestedMessage:
			var ln int
			if ln, n, err = protobuf.ParseLENField(buf[off:], num, wireTyp); err != nil {
				return false, newParseFieldError(f, err)
			}

			b := buf[off+n:][:ln]

			n += ln

			if opts.InterceptNested != nil {
				if checkOrder, err = opts.InterceptNested(num, b, checkOrder); err == nil {
					break
				} else if !errors.Is(err, ErrContinue) {
					return false, err
				}
			}

			if slices.Contains(scheme.recursionFields, num) {
				if checkOrder, err = scanMessageOrdered(b, scheme, checkOrder, ScanMessageOrderedOptions{}); err != nil {
					return false, newParseFieldError(f, err)
				}
				break
			}

			if nestedScheme, ok := scheme.nestedFields[num]; ok {
				if checkOrder, err = scanMessageOrdered(b, nestedScheme, checkOrder, ScanMessageOrderedOptions{}); err != nil {
					return false, newParseFieldError(f, err)
				}
				break
			}

			if alias, ok := scheme.nestedAliases[num]; ok {
				if checkOrder, err = scanMessageOrdered(b, resolveScheme(alias), checkOrder, ScanMessageOrderedOptions{}); err != nil {
					return false, newParseFieldError(f, err)
				}
				break
			}

			panic(fmt.Sprintf("format of nested message field %s is not specified", f))
		}

		off += n
	}

	return checkOrder, nil
}
