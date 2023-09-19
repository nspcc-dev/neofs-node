package main

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	aclservice "github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// node is wrapper over cfg providing interface needed by app components.
type node struct {
	c *cfg

	innerRing *cachedIRFetcher
}

func newNode(c *cfg) *node {
	return &node{
		c: c,
		innerRing: newCachedIRFetcher(&innerRingFetcherWithNotary{
			sidechain: c.cfgMorph.client,
		}),
	}
}

func (x *node) GetContainerInfo(id cid.ID) (aclservice.ContainerInfo, error) {
	var res aclservice.ContainerInfo

	cnr, err := x.c.cfgObject.cnrSource.Get(id)
	if err != nil {
		return res, err
	}

	res.Owner = cnr.Value.Owner()
	res.BasicACL = cnr.Value.BasicACL()

	return res, nil
}

func (x *node) GetExtendedACL(cnr cid.ID) (eacl.Table, error) {
	eACL, err := x.c.cfgObject.eaclSource.GetEACL(cnr)
	if err != nil {
		return eacl.Table{}, err
	}

	return *eACL.Value, nil
}

func (x *node) CurrentEpoch() (uint64, error) {
	return x.c.cfgNetmap.state.CurrentEpoch(), nil
}

func (x *node) ResolveUserByPublicKey(bPublicKey []byte) (user.ID, error) {
	buf := io.NewBufBinWriter()
	emit.CheckSig(buf.BinWriter, bPublicKey)
	h := hash.Hash160(buf.Bytes())

	var res user.ID
	res.SetScriptHash(h)

	return res, nil
}

func (x *node) IsUserPublicKey(usr user.ID, bPublicKey []byte) (bool, error) {
	buf := io.NewBufBinWriter()
	emit.CheckSig(buf.BinWriter, bPublicKey)
	h := hash.Hash160(buf.Bytes())

	return bytes.Equal(h.BytesBE(), usr.WalletBytes()[1:1+util.Uint160Size]), nil
}

func (x *node) IsInnerRingPublicKey(bPublicKey []byte) (bool, error) {
	bKeys, err := x.innerRing.InnerRingKeys()
	if err != nil {
		return false, err
	}

	for i := range bKeys {
		if bytes.Equal(bKeys[i], bPublicKey) {
			return true, nil
		}
	}

	return false, nil
}

func (x *node) IsContainerNodePublicKey(cnrID cid.ID, bPublicKey []byte) (bool, error) {
	cnr, err := x.c.cfgObject.cnrSource.Get(cnrID)
	if err != nil {
		return false, fmt.Errorf("read container info: %w", err)
	}

	nm, err := netmap.GetLatestNetworkMap(x.c.netMapSource)
	if err != nil {
		return false, err
	}

	cnrPolicy := cnr.Value.PlacementPolicy()

	ok, err := isContainerNodePublicKey(bPublicKey, *nm, cnrID, cnrPolicy)
	if err != nil {
		return false, err
	} else if ok {
		return true, nil
	}

	// also check with previous epoch netmap: after epoch tick storage node may
	// become out-of-container and try to migrate data
	nm, err = netmap.GetPreviousNetworkMap(x.c.netMapSource)
	if err != nil {
		return false, err
	}

	return isContainerNodePublicKey(bPublicKey, *nm, cnrID, cnrPolicy)
}

func isContainerNodePublicKey(bPublicKey []byte, nm netmapSDK.NetMap, cnrID cid.ID, cnrPolicy netmapSDK.PlacementPolicy) (bool, error) {
	nodeLists, err := nm.ContainerNodes(cnrPolicy, cnrID)
	if err != nil {
		return false, err
	}

	for i := range nodeLists {
		for j := range nodeLists[i] {
			if bytes.Equal(nodeLists[i][j].PublicKey(), bPublicKey) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (x *node) ReadLocalObjectHeaders(cnr cid.ID, id oid.ID) (objectSDK.Object, error) {
	var addr oid.Address
	addr.SetContainer(cnr)
	addr.SetObject(id)

	hdr, err := engine.Head(x.c.cfgObject.cfgLocalStorage.localStorage, addr)
	if err != nil {
		return objectSDK.Object{}, err
	}

	return *hdr, nil
}
