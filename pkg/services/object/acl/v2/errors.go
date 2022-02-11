package v2

import (
	"errors"
	"fmt"
)

var (
	// ErrMalformedRequest is returned when request contains
	// invalid data.
	ErrMalformedRequest = errors.New("malformed request")
	// ErrUnknownRole is returned when role of the sender is unknown.
	ErrUnknownRole = errors.New("can't classify request sender")
	// ErrUnknownContainer is returned when container fetching errors appeared.
	ErrUnknownContainer = errors.New("can't fetch container info")
)

type accessErr struct {
	RequestInfo

	failedCheckTyp string
}

func (a *accessErr) Error() string {
	return fmt.Sprintf("access to operation %v is denied by %s check", a.operation, a.failedCheckTyp)
}

func basicACLErr(info RequestInfo) error {
	return &accessErr{
		RequestInfo:    info,
		failedCheckTyp: "basic ACL",
	}
}

func eACLErr(info RequestInfo) error {
	return &accessErr{
		RequestInfo:    info,
		failedCheckTyp: "extended ACL",
	}
}
