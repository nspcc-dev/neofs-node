package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
)

// Server is an interface of the NeoFS API Accounting service server
type Server interface {
	Balance(context.Context, *accounting.BalanceRequest) (*accounting.BalanceResponse, error)
}
