package innerring

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

// processContainerNotification handles notification spawned by the NeoFS
// Container contract if notification.name is supported or writes warning to the
// log otherwise.
//
// Supported notifications:
//   - 'containerPut'
//   - 'containerDelete'
//   - 'setEACL'
func (x *node) processContainerNotification(n notification) {
	switch n.name {
	default:
		x.log.Warn("unsupported notification of the Container contract", zap.String("name", n.name))
	case "containerPut":
		x.processContainerCreationNotification(n)
	case "containerDelete":
		x.processContainerRemovalNotification(n)
	case "setEACL":
		x.processContainerSetEACLNotification(n)
	}
}

// processContainerNotaryRequest handles notaryRequest related to the NeoFS
// Container contract if notaryRequest.method is supported or writes warning to
// the log otherwise.
//
// Supported methods:
//   - 'put'
//   - 'delete'
//   - 'setEACL'
func (x *node) processContainerNotaryRequest(n notaryRequest) {
	switch n.method {
	default:
		x.log.Warn("unsupported notary request of the Container contract", zap.String("method", n.method))
	case "put":
		x.processContainerCreationNotaryRequest(n)
	case "delete":
		x.processContainerRemovalNotaryRequest(n)
	case "setEACL":
		x.processContainerSetEACLNotaryRequest(n)
	}
}

// processContainerCreationNotification handles 'put' notification spawned by
// the NeoFS Container contract via underlying container.Processor. If container
// creation request is approved, 'put' method of the Container contract is
// called with untouched data from the notification on behalf of the local Inner
// Ring. Writes any processing problems to the log.
func (x *node) processContainerCreationNotification(n notification) {
	req, err := containerCreationRequestFromNotification(n)
	if err != nil {
		x.log.Error("failed to decode notification with container creation request", zap.Error(err))
		return
	}

	if x.processors.container.ProcessContainerCreation(req) {
		x.log.Info("container creation request from notification verified, sending approval...")
		x.callNamedSidechainContractMethod(contractContainer, "put", req.Container, req.Signature, req.PublicKey, req.Session)
	} else {
		x.log.Info("container creation request from notification is denied")
	}
}

// processContainerCreationNotification handles notaryRequest related to the
// 'put' method of the NeoFS Container contract via underlying
// container.Processor. If container creation request is approved, received
// notaryRequest is signed by the local Inner Ring node's account and sent to
// the NeoFS blockchain network. Writes any processing problems to the log.
func (x *node) processContainerCreationNotaryRequest(n notaryRequest) {
	req, err := containerCreationRequestFromNotaryRequest(n)
	if err != nil {
		x.log.Error("failed to decode notary request with container creation request", zap.Error(err))
		return
	}

	if x.processors.container.ProcessContainerCreation(req) {
		x.log.Info("container creation request from notary request verified, sending approval...")
		x.signAndSendVerifiedNotaryRequest(n)
	} else {
		x.log.Info("container creation request from notary request is denied")
	}
}

// containerCreationRequestFromNotification decodes
// models.ContainerCreationRequest from the raw 'put' notification spawned by
// the NeoFS Container contract.
func containerCreationRequestFromNotification(n notification) (req models.ContainerCreationRequest, err error) {
	// FIXME: here is a temp hack to not change function signature
	var e event.Event

	e, err = container.ParsePut(&state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(n.items),
		},
	})
	if err == nil {
		v := e.(container.Put) // TODO: strengthen type

		req.Container = v.Container()
		req.Signature = v.Signature()
		req.PublicKey = v.PublicKey()
		req.Session = v.SessionToken()
	}

	return
}

// containerCreationRequestFromNotaryRequest decodes
// models.ContainerCreationRequest from the raw notaryRequest sent for 'put'
// method of the NeoFS Container contract.
func containerCreationRequestFromNotaryRequest(n notaryRequest) (req models.ContainerCreationRequest, err error) {
	const expectedItemNum = 4
	if len(n.ops) != expectedItemNum {
		return req, fmt.Errorf("unsupported number of ops: %d != %d", len(n.ops), expectedItemNum)
	}

	for i := 0; i < expectedItemNum; i++ {
		if c := n.ops[i].Code(); c < opcode.PUSHDATA1 || c > opcode.PUSHDATA4 {
			return req, fmt.Errorf("unsupported opcode of item #%d: %s", i, c)
		}
	}

	req.Session = n.ops[0].Param()
	req.PublicKey = n.ops[1].Param()
	req.Signature = n.ops[2].Param()
	req.Container = n.ops[3].Param()

	return
}

// processContainerRemovalNotification handles 'delete' notification spawned by
// the NeoFS Container contract via underlying container.Processor. If container
// removal request is approved, 'delete' method of the Container contract is
// called with untouched data from the notification on behalf of the local Inner
// Ring. Writes any processing problems to the log.
func (x *node) processContainerRemovalNotification(n notification) {
	req, err := containerRemovalRequestFromNotification(n)
	if err != nil {
		x.log.Error("failed to decode notification with container removal request", zap.Error(err))
		return
	}

	if x.processors.container.ProcessContainerRemoval(req) {
		x.log.Info("container removal request from notification verified, sending approval...")
		x.callNamedSidechainContractMethod(contractContainer, "delete", req.Container, req.Signature, req.Session)
	} else {
		x.log.Info("container removal request from notification is denied")
	}
}

// processContainerRemovalNotaryRequest handles notaryRequest related to the
// 'delete' method of the NeoFS Container contract via underlying
// container.Processor. If container removal request is approved, received
// notaryRequest is signed by the local Inner Ring node's account and sent to
// the NeoFS blockchain network. Writes any processing problems to the log.
func (x *node) processContainerRemovalNotaryRequest(n notaryRequest) {
	req, err := containerRemovalRequestFromNotaryRequest(n)
	if err != nil {
		x.log.Error("failed to decode notary request with container removal request", zap.Error(err))
		return
	}

	if x.processors.container.ProcessContainerRemoval(req) {
		x.log.Info("container removal request from notary request verified, sending approval...")
		x.signAndSendVerifiedNotaryRequest(n)
	} else {
		x.log.Info("container removal request from notary request is denied")
	}
}

// containerRemovalRequestFromNotification decodes
// models.ContainerRemovalRequest from the raw 'delete' notification spawned by
// the NeoFS Container contract.
func containerRemovalRequestFromNotification(n notification) (req models.ContainerRemovalRequest, err error) {
	// FIXME: here is a temp hack to not change function signature
	var e event.Event

	e, err = container.ParseDelete(&state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(n.items),
		},
	})
	if err == nil {
		v := e.(container.Delete) // TODO: strengthen type

		req.Container = v.ContainerID()
		req.Signature = v.Signature()
		req.Session = v.SessionToken()
	}

	return
}

// containerRemovalRequestFromNotaryRequest decodes
// models.ContainerRemovalRequest from the raw notaryRequest sent for 'delete'
// method of the NeoFS Container contract.
func containerRemovalRequestFromNotaryRequest(n notaryRequest) (req models.ContainerRemovalRequest, err error) {
	const expectedItemNum = 3
	if len(n.ops) != expectedItemNum {
		return req, fmt.Errorf("unsupported number of ops: %d != %d", len(n.ops), expectedItemNum)
	}

	for i := 0; i < expectedItemNum; i++ {
		if c := n.ops[i].Code(); c < opcode.PUSHDATA1 || c > opcode.PUSHDATA4 {
			return req, fmt.Errorf("unsupported opcode of item #%d: %s", i, c)
		}
	}

	req.Session = n.ops[0].Param()
	req.Signature = n.ops[1].Param()
	req.Container = n.ops[2].Param()

	return
}

// processContainerRemovalNotification handles 'setEACL' notification spawned by
// the NeoFS Container contract via underlying container.Processor. If container
// eACL setting request is approved, 'setEACH' method of the Container contract
// is called with untouched data from the notification on behalf of the local
// Inner Ring. Writes any processing problems to the log.
func (x *node) processContainerSetEACLNotification(n notification) {
	req, err := containerSetEACLRequestFromNotification(n)
	if err != nil {
		x.log.Error("failed to decode notification with set container eACL request", zap.Error(err))
		return
	}

	if x.processors.container.ProcessSetContainerExtendedACLRequest(req) {
		x.log.Info("set container eACL request from notification verified, sending approval...")
		x.callNamedSidechainContractMethod(contractContainer, "setEACL", req.ExtendedACL, req.Signature, req.PublicKey, req.Session)
	} else {
		x.log.Info("set container eACL request from notification is denied")
	}
}

// processContainerSetEACLNotaryRequest handles notaryRequest related to the
// 'setEACL' method of the NeoFS Container contract via underlying
// container.Processor. If container eACL setting request is approved, received
// notaryRequest is signed by the local Inner Ring node's account and sent to
// the NeoFS blockchain network. Writes any processing problems to the log.
func (x *node) processContainerSetEACLNotaryRequest(n notaryRequest) {
	req, err := containerSetEACLRequestFromNotaryRequest(n)
	if err != nil {
		x.log.Error("failed to decode notary request with set container eACL request", zap.Error(err))
		return
	}

	if x.processors.container.ProcessSetContainerExtendedACLRequest(req) {
		x.log.Info("set container eACL request from notary request verified, sending approval...")
		x.signAndSendVerifiedNotaryRequest(n)
	} else {
		x.log.Info("set container eACL request from notary request is denied")
	}
}

// containerSetEACLRequestFromNotification decodes
// models.SetContainerExtendedACLRequest from the raw 'setEACL' notification
// spawned by the NeoFS Container contract.
func containerSetEACLRequestFromNotification(n notification) (req models.SetContainerExtendedACLRequest, err error) {
	// FIXME: here is a temp hack to not change function signature
	var e event.Event

	e, err = container.ParseSetEACL(&state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(n.items),
		},
	})
	if err == nil {
		v := e.(container.SetEACL) // TODO: strengthen type

		req.ExtendedACL = v.Table()
		req.Signature = v.Signature()
		req.PublicKey = v.PublicKey()
		req.Session = v.SessionToken()
	}

	return
}

// containerSetEACLRequestFromNotaryRequest decodes
// models.SetContainerExtendedACLRequest from the raw notaryRequest sent for
// 'setEACL' method of the NeoFS Container contract.
func containerSetEACLRequestFromNotaryRequest(n notaryRequest) (req models.SetContainerExtendedACLRequest, err error) {
	const expectedItemNum = 4
	if len(n.ops) != expectedItemNum {
		return req, fmt.Errorf("unsupported number of ops: %d != %d", len(n.ops), expectedItemNum)
	}

	for i := 0; i < expectedItemNum; i++ {
		if c := n.ops[i].Code(); c < opcode.PUSHDATA1 || c > opcode.PUSHDATA4 {
			return req, fmt.Errorf("unsupported opcode of item #%d: %s", i, c)
		}
	}

	req.Session = n.ops[0].Param()
	req.PublicKey = n.ops[1].Param()
	req.Signature = n.ops[2].Param()
	req.ExtendedACL = n.ops[3].Param()

	return
}
