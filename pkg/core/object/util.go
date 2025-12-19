package objectcore

import "github.com/nspcc-dev/neofs-sdk-go/version"

func equalProtoVersions(a, b *version.Version) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	return *a == *b
}

func stringifyVersion(v *version.Version) string {
	if v != nil {
		return v.String()
	}
	return "unset"
}
