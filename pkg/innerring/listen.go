package innerring

import (
	"go.uber.org/zap"
)

// listenBlockchain runs asynchronous process which listens to the NeoFS-related
// events thrown by the NeoFS blockchain network and schedules them for handling
// by the local Inner Ring node. The background process is aborted by external
// signal read from the provided channel.
func (x *node) listenBlockchain(chDone <-chan struct{}) {
	go x.listenNotifications(chDone)
	go x.listenNotaryRequests(chDone)
}

// listenNotaryRequests listens to notary requests received from Notary service
// of the NeoFS blockchain network, distills them to notaryRequest type
// instances and schedules them for handling by the local Inner Ring
// node according to related NeoFS smart contracts.
//
// The listenNotaryRequests is blocking: it's aborted by breaking the flow of
// requests from the blockchain or by external signal read from the provided
// channel.
func (x *node) listenNotaryRequests(chDone <-chan struct{}) {
	x.log.Info("listening notary requests...")

	chNotaryRequests := x.subscribeToNotaryRequests(chDone)

	for {
		select {
		case <-chDone:
			x.log.Info("stopping listener of distilled notary requests (external signal)")
			return
		case req, ok := <-chNotaryRequests:
			if !ok {
				x.log.Info("stopping listener of distilled notary requests (channel of notary requests was closed)")
				return
			}

			x.log.Info("notary request from the blockchain",
				zap.Stringer("contract", req.contract),
				zap.String("method", req.method),
				zap.Int("op num", len(req.ops)),
			)

			switch req.contract {
			default:
				x.log.Info("ignore unsupported notary request",
					zap.Stringer("contract", req.contract),
					zap.String("method", req.method),
				)
			case contractContainer:
				x.processContainerNotaryRequest(req)
			case contractBalance:
				x.processBalanceNotaryRequest(req)
				// case contractAudit:
				// case contractNetmap:
				// case contractSubnet:
				// case contractNeoFSID:
				// case contractProxy:
				// case contractReputation:
			}
		}
	}
}

// listenNotifications listens to notifications from the NeoFS blockchain
// network, distills them to notification type instances and schedules them for
// handling by the local Inner Ring node according to the NeoFS smart contracts
// which thrown the notifications.
//
// The listenNotifications is blocking: it's aborted by breaking the flow of
// notifications from the blockchain or by external signal read from the
// provided channel.
func (x *node) listenNotifications(chDone <-chan struct{}) {
	chNotifications := x.subscribeToNotifications(chDone)

	for {
		select {
		case <-chDone:
			x.log.Info("stopping listener of distilled notifications (external signal)")
			return
		case n, ok := <-chNotifications:
			if !ok {
				x.log.Info("stopping listener of distilled notifications (notification channel closed)")
				return
			}

			x.log.Info("notification from blockchain",
				zap.String("name", n.name),
				zap.Stringer("contract", n.contract),
				zap.Int("item num", len(n.items)),
			)

			switch n.contract {
			default:
				x.log.Info("ignore unsupported notification",
					zap.Stringer("contract", n.contract),
					zap.String("name", n.name),
				)
			case contractContainer:
				x.processContainerNotification(n)
			case contractBalance:
				x.processBalanceNotification(n)
				// case contractAudit:
				// case contractNetmap:
				// case contractSubnet:
				// case contractNeoFSID:
				// case contractProxy:
				// case contractReputation:
			}
		}
	}
}
