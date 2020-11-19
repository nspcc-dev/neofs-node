package blobstor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type fsTree struct {
	depth int

	dirNameLen int

	Info
}

const dirNameLen = 1 // in bytes

var maxDepth = (sha256.Size - 1) / dirNameLen

var errFileNotFound = errors.New("file not found")

func stringifyAddress(addr *objectSDK.Address) string {
	h := sha256.Sum256([]byte(addr.String()))

	return hex.EncodeToString(h[:])
}

func (t *fsTree) treePath(addr *objectSDK.Address) string {
	sAddr := stringifyAddress(addr)

	dirs := make([]string, 0, t.depth+1+1) // 1 for root, 1 for file
	dirs = append(dirs, t.RootPath)

	for i := 0; i < t.depth; i++ {
		dirs = append(dirs, sAddr[:t.dirNameLen])
		sAddr = sAddr[t.dirNameLen:]
	}

	dirs = append(dirs, sAddr)

	return path.Join(dirs...)
}
