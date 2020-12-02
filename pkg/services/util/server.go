package util

import (
	"context"
)

// ServerStream is an interface of server-side stream v2.
type ServerStream interface {
	Context() context.Context
}
