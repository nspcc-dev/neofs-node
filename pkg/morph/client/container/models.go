package container

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	protonetmap "github.com/nspcc-dev/neofs-sdk-go/proto/netmap"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"google.golang.org/protobuf/proto"
)

func containerFromStackItem(item stackitem.Item) (container.Container, error) {
	var contractStruct containerrpc.ContainerInfo
	if err := contractStruct.FromStackItem(item); err != nil {
		return container.Container{}, err
	}

	mjr, mnr, err := containerVersionFromStruct(contractStruct.Version)
	if err != nil {
		return container.Container{}, err
	}

	if len(contractStruct.Nonce) != 16 {
		return container.Container{}, fmt.Errorf("invalid nonce: invalid len: expected 16, got %d", len(contractStruct.Nonce))
	}

	basicACL, err := toUint32(contractStruct.BasicACL)
	if err != nil {
		return container.Container{}, fmt.Errorf("invalid basic ACL: %w", err)
	}

	attrs := make([]*protocontainer.Container_Attribute, len(contractStruct.Attributes))
	for i := range contractStruct.Attributes {
		if contractStruct.Attributes[i] != nil {
			attrs[i] = &protocontainer.Container_Attribute{
				Key:   contractStruct.Attributes[i].Key,
				Value: contractStruct.Attributes[i].Value,
			}
		}
	}

	owner := user.NewFromScriptHash(contractStruct.Owner)

	// TODO: add version and nonce setter to get rid of proto instance
	m := protocontainer.Container{
		Version:         &protorefs.Version{Major: mjr, Minor: mnr},
		OwnerId:         &protorefs.OwnerID{Value: owner[:]},
		Nonce:           contractStruct.Nonce,
		BasicAcl:        basicACL,
		Attributes:      attrs,
		PlacementPolicy: new(protonetmap.PlacementPolicy),
	}

	if err := proto.Unmarshal(contractStruct.StoragePolicy, m.PlacementPolicy); err != nil {
		return container.Container{}, fmt.Errorf("invalid storage policy binary: %w", err)
	}

	var cnr container.Container
	if err := cnr.FromProtoMessage(&m); err != nil {
		return container.Container{}, fmt.Errorf("invalid container: %w", err)
	}

	return cnr, nil
}

func containerVersionFromStruct(src *containerrpc.ContainerAPIVersion) (uint32, uint32, error) {
	if src == nil {
		return 0, 0, errors.New("missing version")
	}

	mjr, err := toUint32(src.Major)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid major: %w", err)
	}

	mnr, err := toUint32(src.Minor)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minor: %w", err)
	}

	return mjr, mnr, nil
}

func containerToStackItem(cnr container.Container) stackitem.Convertible {
	ver := cnr.Version()
	owner := cnr.Owner()

	var attrs []*containerrpc.ContainerAttribute
	for k, v := range cnr.Attributes() {
		attrs = append(attrs, &containerrpc.ContainerAttribute{Key: k, Value: v})
	}

	return &containerrpc.ContainerInfo{
		Version: &containerrpc.ContainerAPIVersion{
			Major: big.NewInt(int64(ver.Major())),
			Minor: big.NewInt(int64(ver.Minor())),
		},
		Owner:         owner.ScriptHash(),
		Nonce:         cnr.ProtoMessage().Nonce, // TODO: provide and use nonce getter
		BasicACL:      big.NewInt(int64(cnr.BasicACL().Bits())),
		Attributes:    attrs,
		StoragePolicy: cnr.PlacementPolicy().Marshal(),
	}
}
