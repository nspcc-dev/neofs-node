package config

import (
	"os"
	"path/filepath"
	"strings"
)

// ResolveHomePath replaces leading `~`
// with home directory.
//
// Does nothing if path does not start
// with contain `~`.
func ResolveHomePath(path string) string {
	homeDir, _ := os.UserHomeDir()

	if path == "~" {
		path = homeDir
	} else if strings.HasPrefix(path, "~/") {
		path = filepath.Join(homeDir, path[2:])
	}

	return path
}
