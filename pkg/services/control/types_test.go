package control_test

import (
	"bytes"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
)

func TestNetmap_StableMarshal(t *testing.T) {
	testStableMarshal(t, generateNetmap(), new(control.Netmap), func(m1, m2 protoMessage) bool {
		return equalNetmaps(m1.(*control.Netmap), m2.(*control.Netmap))
	})
}

func generateNetmap() *control.Netmap {
	nm := new(control.Netmap)
	nm.SetEpoch(13)

	const nodeCount = 2

	nodes := make([]*control.NodeInfo, 0, nodeCount)

	for range nodeCount {
		n := new(control.NodeInfo)
		n.SetPublicKey(testData(33))
		n.SetAddresses([]string{testString(), testString()})
		n.SetState(control.NetmapStatus_ONLINE)

		const attrCount = 2

		attrs := make([]*control.NodeInfo_Attribute, 0, attrCount)

		for range attrCount {
			a := new(control.NodeInfo_Attribute)
			a.SetKey(testString())
			a.SetValue(testString())

			const parentsCount = 2

			parents := make([]string, 0, parentsCount)

			for range parentsCount {
				parents = append(parents, testString())
			}

			a.SetParents(parents)

			attrs = append(attrs, a)
		}

		n.SetAttributes(attrs)

		nodes = append(nodes, n)
	}

	nm.SetNodes(nodes)

	return nm
}

func equalNetmaps(nm1, nm2 *control.Netmap) bool {
	if nm1.GetEpoch() != nm2.GetEpoch() {
		return false
	}

	n1, n2 := nm1.GetNodes(), nm2.GetNodes()

	if len(n1) != len(n2) {
		return false
	}

	for i := range n1 {
		if !equalNodeInfos(n1[i], n2[i]) {
			return false
		}
	}

	return true
}

func equalNodeInfos(n1, n2 *control.NodeInfo) bool {
	if !bytes.Equal(n1.GetPublicKey(), n2.GetPublicKey()) ||
		n1.GetState() != n2.GetState() {
		return false
	}

	na1, na2 := n1.GetAddresses(), n2.GetAddresses()

	if len(na1) != len(na2) {
		return false
	}

	if slices.Compare(na1, na2) != 0 {
		return false
	}

	a1, a2 := n1.GetAttributes(), n2.GetAttributes()

	if len(a1) != len(a2) {
		return false
	}

	for i := range a1 {
		if a1[i].GetKey() != a2[i].GetKey() || a1[i].GetValue() != a2[i].GetValue() {
			return false
		}

		p1, p2 := a1[i].GetParents(), a2[i].GetParents()

		if len(p1) != len(p2) {
			return false
		}

		if slices.Compare(p1, p2) != 0 {
			return false
		}
	}

	return true
}

func generateShardInfo(id int) *control.ShardInfo {
	si := new(control.ShardInfo)

	path := "/nice/dir/awesome/files/" + strconv.Itoa(id)

	uid, _ := uuid.NewRandom()
	bin, _ := uid.MarshalBinary()

	si.SetID(bin)
	si.SetMode(control.ShardMode_READ_WRITE)
	si.SetMetabasePath(filepath.Join(path, "meta"))
	si.Blobstor = []*control.BlobstorInfo{
		{Type: fstree.Type, Path: filepath.Join(path, "fstree")},
		{Type: peapod.Type, Path: filepath.Join(path, "peapod.db")}}
	si.SetWriteCachePath(filepath.Join(path, "writecache"))

	return si
}
