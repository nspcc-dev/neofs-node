package misc

const (
	// Prefix is a neofs node application prefix.
	Prefix = "neofs"

	// InnerRingPrefix is an inner ring application prefix.
	InnerRingPrefix = "neofs_ir"
)

// These variables are changed in compile time.
var (
	// Build is an application build time.
	Build = "now"

	// Version is an application version.
	Version = "dev"

	// Debug is an application debug mode flag.
	Debug = "false"
)
