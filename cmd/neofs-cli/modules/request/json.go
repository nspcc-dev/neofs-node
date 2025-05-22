package request

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type userID user.ID

func (x *userID) UnmarshalText(b []byte) error {
	if err := (*user.ID)(x).DecodeString(string(b)); err != nil {
		return fmt.Errorf("decode user ID: %w", err)
	}
	return nil
}

type basicACL acl.Basic

func (x *basicACL) UnmarshalText(b []byte) error {
	if err := (*acl.Basic)(x).DecodeString(string(b)); err != nil {
		return fmt.Errorf("decode basic ACL: %w", err)
	}
	return nil
}

type protoVersion refs.Version

func (x *protoVersion) UnmarshalText(b []byte) error {
	mjrB, mnrB, ok := bytes.Cut(b, []byte{'.'})
	if !ok {
		return errors.New("invalid proto version: missing dot")
	}

	mjrB = bytes.TrimPrefix(mjrB, []byte{'v'})
	mjr, err := strconv.ParseUint(string(mjrB), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid proto version: invalid major number: %w", err)
	}
	mnr, err := strconv.ParseUint(string(mnrB), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid proto version: invalid minor number: %w", err)
	}

	x.Major, x.Minor = uint32(mjr), uint32(mnr)

	return nil
}

type storagePolicy netmap.PlacementPolicy

func (x *storagePolicy) UnmarshalText(b []byte) error {
	if err := (*netmap.PlacementPolicy)(x).DecodeString(string(b)); err != nil {
		return fmt.Errorf("decode storage policy: %w", err)
	}
	return nil
}
