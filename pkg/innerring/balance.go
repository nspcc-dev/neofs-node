package innerring

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"go.uber.org/zap"
)

// processBalanceNotification handles notification spawned by the NeoFS Balance
// contract if notification.name is supported or writes warning to the log
// otherwise.
//
// Supported notifications:
//   - 'Lock'
func (x *node) processBalanceNotification(n notification) {
	switch n.name {
	default:
		x.log.Warn("unsupported notification of the Balance contract", zap.String("name", n.name))
	case "Lock":
		x.processLockedAssetsNotification(n)
	}
}

// processBalanceNotification handles notaryRequest related to the NeoFS Balance
// contract if notaryRequest.method is supported or writes warning to the log
// otherwise.
//
// No requests are currently supported.
func (x *node) processBalanceNotaryRequest(n notaryRequest) {
	x.log.Warn("unsupported notary request of the Balance contract", zap.String("method", n.method))
}

// processLockedAssetsNotification handles 'Lock' notification spawned by the
// NeoFS Balance contract via underlying balance.Processor. Writes any
// processing problems to the log.
func (x *node) processLockedAssetsNotification(n notification) {
	ev, err := lockedAssetsEventFromNotification(n)
	if err != nil {
		x.log.Error("failed to decode notification with locked assets event", zap.Error(err))
		return
	}

	x.processors.balance.ProcessLockAssetsEvent(ev)
}

// lockedAssetsEventFromNotification decodes models.LockAssetsEvent from the raw
// 'Lock' notification spawned by the NeoFS Balance contract.
func lockedAssetsEventFromNotification(n notification) (ev models.LockAssetsEvent, err error) {
	// FIXME: here is a temp hack to not change function signature
	var e event.Event

	e, err = balance.ParseLock(&state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(n.items),
		},
	})
	if err == nil {
		v := e.(balance.Lock) // TODO: strengthen type

		ev.WithdrawTx, err = util.Uint256DecodeBytesBE(v.ID())
		if err != nil {
			return ev, fmt.Errorf("decode big-endian withdrawal transaction ID: %w", err)
		}

		ev.UserAccount = v.User()
		ev.Amount = uint64(v.Amount())
	}

	return
}
