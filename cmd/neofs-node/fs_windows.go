//go:build windows

package main

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func totalBytes(paths []string) (uint64, error) {
	var total uint64 // bytes
	diskMap := make(map[string]struct{}, len(paths))

	for _, path := range paths {
		disk := filepath.VolumeName(path)
		if _, handled := diskMap[disk]; handled {
			continue
		}

		var availB uint64
		var totalB uint64
		var freeTotalB uint64
		var pathP = windows.StringToUTF16Ptr(path)

		err := windows.GetDiskFreeSpaceEx(pathP, &availB, &totalB, &freeTotalB)
		if err != nil {
			return 0, fmt.Errorf("get disk stat by '%s' path: %w", path, err)
		}

		total += totalB
		diskMap[disk] = struct{}{}
	}

	return total, nil
}
