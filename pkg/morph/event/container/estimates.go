package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// StartEstimation structure of container.StartEstimation notification from
// morph chain.
type StartEstimation struct {
	epoch uint64
}

// StopEstimation structure of container.StopEstimation notification from
// morph chain.
type StopEstimation struct {
	epoch uint64
}

// MorphEvent implements Neo:Morph Event interface.
func (StartEstimation) MorphEvent() {}

// MorphEvent implements Neo:Morph Event interface.
func (StopEstimation) MorphEvent() {}

// Epoch returns epoch value for which to start container size estimation.
func (s StartEstimation) Epoch() uint64 { return s.epoch }

// Epoch returns epoch value for which to stop container size estimation.
func (s StopEstimation) Epoch() uint64 { return s.epoch }

// ParseStartEstimation from notification into container event structure.
func ParseStartEstimation(e *state.ContainedNotificationEvent) (event.Event, error) {
	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	epoch, err := parseEstimation(params)
	if err != nil {
		return nil, err
	}

	return StartEstimation{epoch: epoch}, nil
}

// ParseStopEstimation from notification into container event structure.
func ParseStopEstimation(e *state.ContainedNotificationEvent) (event.Event, error) {
	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	epoch, err := parseEstimation(params)
	if err != nil {
		return nil, err
	}

	return StopEstimation{epoch: epoch}, nil
}

func parseEstimation(params []stackitem.Item) (uint64, error) {
	if ln := len(params); ln != 1 {
		return 0, event.WrongNumberOfParameters(1, ln)
	}

	// parse container
	epoch, err := client.IntFromStackItem(params[0])
	if err != nil {
		return 0, fmt.Errorf("could not get estimation epoch: %w", err)
	}

	return uint64(epoch), nil
}
