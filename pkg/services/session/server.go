package session

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

// Server is an interface of the NeoFS API Session service server
type Server interface {
	Create(context.Context, *session.CreateRequest) (*session.CreateResponse, error)
}
