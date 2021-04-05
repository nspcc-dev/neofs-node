package reputationrpc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
)

// Server is an interface of the NeoFS API v2 Reputation service server.
type Server interface {
	SendLocalTrust(context.Context, *reputation.SendLocalTrustRequest) (*reputation.SendLocalTrustResponse, error)
	SendIntermediateResult(context.Context, *reputation.SendIntermediateResultRequest) (*reputation.SendIntermediateResultResponse, error)
}
