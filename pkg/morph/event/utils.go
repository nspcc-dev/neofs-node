package event

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
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

// SetScriptHash is a script hash setter.
func (s *scriptHashValue) SetScriptHash(v util.Uint160) {
	s.hash = v
}

// ScriptHash is script hash getter.
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
				zap.String("error", err.Error()),
			)
		}
	}
}
