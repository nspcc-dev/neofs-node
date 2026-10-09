package time

import "time"

// Provider supplies current FS chain time without calling the chain.
// It should be updated from block header subscriptions and return time
// based on the latest observed header timestamp.
type Provider interface {
	Now() time.Time
}
