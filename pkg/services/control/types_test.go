package control_test

import (
	"bytes"
	"testing"

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

	for i := 0; i < nodeCount; i++ {
		n := new(control.NodeInfo)
		n.SetPublicKey(testData(33))
		n.SetAddress(testString())
		n.SetState(control.HealthStatus_ONLINE)

		const attrCount = 2

		attrs := make([]*control.NodeInfo_Attribute, 0, attrCount)

		for j := 0; j < attrCount; j++ {
			a := new(control.NodeInfo_Attribute)
			a.SetKey(testString())
			a.SetValue(testString())

			const parentsCount = 2

			parents := make([]string, 0, parentsCount)

			for k := 0; k < parentsCount; k++ {
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
		n1.GetAddress() != n2.GetAddress() ||
		n1.GetState() != n2.GetState() {
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

		for j := range p1 {
			if p1[j] != p2[j] {
				return false
			}
		}
	}

	return true
}
