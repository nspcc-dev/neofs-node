package commonflags

const (
	// ExpireAt is a flag for setting last epoch of an object or a token.
	ExpireAt = "expire-at"
	// ExpireAtTime is a flag for setting object expiration time in RFC3339 format.
	ExpireAtTime = "expire-at-time"
	// Lifetime is a flag for setting the lifetime of an object or a token,
	// starting from the current epoch.
	Lifetime = "lifetime"
)
