package event

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	util2 "github.com/nspcc-dev/neofs-node/pkg/util"
	"go.uber.org/zap"
)

type scriptHashValue struct {
	hash util.Uint160
}

type typeValue struct {
	typ Type
}

type scriptHashWithType struct {
	scriptHashValue
	typeValue
}

type notaryRequestTypes struct {
	notaryRequestMempoolType
	notaryScriptWithHash
}

type notaryScriptWithHash struct {
	notaryRequestType
	scriptHashValue
}

type notaryRequestMempoolType struct {
	mempoolTyp mempoolevent.Type
}

type notaryRequestType struct {
	notaryType NotaryType
}

// GetMempoolType is a notary request mempool type getter.
func (n notaryRequestMempoolType) GetMempoolType() mempoolevent.Type {
	return n.mempoolTyp
}

// SetMempoolType is a notary request mempool type setter.
func (n *notaryRequestMempoolType) SetMempoolType(typ mempoolevent.Type) {
	n.mempoolTyp = typ
}

// RequestType is a notary request type getter.
func (n notaryRequestType) RequestType() NotaryType {
	return n.notaryType
}

// SetRequestType is a notary request type setter.
func (n *notaryRequestType) SetRequestType(typ NotaryType) {
	n.notaryType = typ
}

// SetScriptHash is a script hash setter.
func (s *scriptHashValue) SetScriptHash(v util.Uint160) {
	s.hash = v
}

// ScriptHash is a script hash getter.
func (s scriptHashValue) ScriptHash() util.Uint160 {
	return s.hash
}

// SetType is an event type setter.
func (s *typeValue) SetType(v Type) {
	s.typ = v
}

// GetType is an event type getter.
func (s typeValue) GetType() Type {
	return s.typ
}

// WorkerPoolHandler sets closure over worker pool w with passed handler h.
func WorkerPoolHandler(w util2.WorkerPool, h Handler, log *zap.Logger) Handler {
	return func(e Event) {
		err := w.Submit(func() {
			h(e)
		})

		if err != nil {
			log.Warn("could not Submit handler to worker pool",
				zap.Error(err),
			)
		}
	}
}

var errEmptyStackArray = errors.New("stack item array is empty")

// ParseStackArray parses stack array from raw notification
// event received from neo-go RPC node.
func ParseStackArray(event *state.ContainedNotificationEvent) ([]stackitem.Item, error) {
	arr, err := client.ArrayFromStackItem(event.Item)
	if err != nil {
		return nil, fmt.Errorf("stack item is not an array type: %w", err)
	} else if len(arr) == 0 {
		return nil, errEmptyStackArray
	}

	return arr, nil
}
