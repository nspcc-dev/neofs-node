package misc

import (
	"fmt"
	"runtime"
)

// These variables are changed in compile time.
var (
	// Version is an application version.
	Version = "dev"

	// Debug is an application debug mode flag.
	Debug = "false"
)

// BuildInfo returns human-readable information about this binary.
func BuildInfo(component string) string {
	return fmt.Sprintf("%s\nVersion: %s \nGoVersion: %s\nDebug: %s\n",
		component,
		Version,
		runtime.Version(),
		Debug,
	)
}
