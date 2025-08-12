package object

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// ErrAttributeNotFound is returned when some object attribute not found.
var ErrAttributeNotFound = errors.New("attribute not found")

// GetIndexAttribute looks up for specified index attribute in the given object
// header. Returns -1 if the attribute is missing.
//
// GetIndexAttribute ignores all attribute values except the first.
//
// Note that if attribute exists but negative, GetIndexAttribute returns error.
func GetIndexAttribute(hdr object.Object, attr string) (int, error) {
	i, err := GetIntAttribute(hdr, attr)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return -1, nil
		}
		return 0, err
	}

	if i < 0 {
		return 0, fmt.Errorf("negative value %d", i)
	}

	return i, nil
}

// GetIntAttribute looks up for specified int attribute in the given object
// header. Returns [ErrAttributeNotFound] if the attribute is missing.
//
// GetIntAttribute ignores all attribute values except the first.
func GetIntAttribute(hdr object.Object, attr string) (int, error) {
	if s := GetAttribute(hdr, attr); s != "" {
		return strconv.Atoi(s)
	}
	return 0, ErrAttributeNotFound
}

// GetAttribute looks up for specified attribute in the given object header.
// Returns empty string if the attribute is missing.
//
// GetIntAttribute ignores all attribute values except the first.
func GetAttribute(hdr object.Object, attr string) string {
	attrs := hdr.Attributes()
	for i := range attrs {
		if attrs[i].Key() == attr {
			return attrs[i].Value()
		}
	}
	return ""
}

// SetIntAttribute sets int value for the object attribute. If the attribute
// already exists, SetIntAttribute overwrites its value.
func SetIntAttribute(dst *object.Object, attr string, val int) {
	SetAttribute(dst, attr, strconv.Itoa(val))
}

// SetAttribute sets value for the object attribute. If the attribute already
// exists, SetAttribute overwrites its value.
func SetAttribute(dst *object.Object, attr, val string) {
	attrs := dst.Attributes()
	for i := range attrs {
		if attrs[i].Key() == attr {
			attrs[i].SetValue(val)
			dst.SetAttributes(attrs...)
			return
		}
	}

	dst.SetAttributes(append(attrs, object.NewAttribute(attr, val))...)
}
