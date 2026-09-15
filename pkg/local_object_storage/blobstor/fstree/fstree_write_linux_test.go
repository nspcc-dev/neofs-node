//go:build linux

package fstree

import (
	"testing"
)

func TestFSTree_InitPut(t *testing.T) {
	testInitPutGeneric(t)
}
