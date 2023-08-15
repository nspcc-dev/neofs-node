//go:build !windows

package main

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func totalBytes(paths []string) (uint64, error) {
	var stat unix.Statfs_t
	var total uint64 // bytes
	fsIDMap := make(map[unix.Fsid]struct{})

	for _, path := range paths {
		err := unix.Statfs(path, &stat)
		if err != nil {
			return 0, fmt.Errorf("get FS stat by '%s' path: %w", path, err)
		}

		if _, handled := fsIDMap[stat.Fsid]; handled {
			continue
		}

		total += stat.Blocks * uint64(stat.Bsize)
		fsIDMap[stat.Fsid] = struct{}{}
	}

	return total, nil
}
