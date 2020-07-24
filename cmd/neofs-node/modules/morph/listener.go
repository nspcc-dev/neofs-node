package morph

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type eventListenerParams struct {
	dig.In

	Viper *viper.Viper

	Logger *zap.Logger
}

var listenerPrefix = optPath(prefix, "listener")

const (
	listenerEndpointOpt = "endpoint"

	listenerDialTimeoutOpt = "dial_timeout"
)

// ListenerEndpointOptPath returns the config path to event listener's endpoint.
func ListenerEndpointOptPath() string {
	return optPath(listenerPrefix, listenerEndpointOpt)
}

// ListenerDialTimeoutOptPath returns the config path to event listener's dial timeout.
func ListenerDialTimeoutOptPath() string {
	return optPath(listenerPrefix, listenerDialTimeoutOpt)
}

func newEventListener(p eventListenerParams) (event.Listener, error) {
	sub, err := subscriber.New(context.Background(), &subscriber.Params{
		Log:         p.Logger,
		Endpoint:    p.Viper.GetString(ListenerEndpointOptPath()),
		DialTimeout: p.Viper.GetDuration(ListenerDialTimeoutOptPath()),
	})
	if err != nil {
		return nil, err
	}

	return event.NewListener(event.ListenerParams{
		Logger:     p.Logger,
		Subscriber: sub,
	})
}
