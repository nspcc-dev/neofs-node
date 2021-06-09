package control_test

import (
	"testing"

	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type protoMessage interface {
	StableMarshal([]byte) ([]byte, error)
	proto.Message
}

func testStableMarshal(t *testing.T, m1, m2 protoMessage, cmp func(m1, m2 protoMessage) bool) {
	data, err := m1.StableMarshal(nil)
	require.NoError(t, err)

	require.NoError(t, proto.Unmarshal(data, m2))

	require.True(t, cmp(m1, m2))
}

func TestHealthCheckResponse_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateHealthCheckResponseBody(),
		new(control.HealthCheckResponse_Body),
		func(m1, m2 protoMessage) bool {
			return equalHealthCheckResponseBodies(
				m1.(*control.HealthCheckResponse_Body),
				m2.(*control.HealthCheckResponse_Body),
			)
		},
	)
}

func generateHealthCheckResponseBody() *control.HealthCheckResponse_Body {
	body := new(control.HealthCheckResponse_Body)
	body.SetHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	return body
}

func equalHealthCheckResponseBodies(b1, b2 *control.HealthCheckResponse_Body) bool {
	return b1.GetHealthStatus() == b2.GetHealthStatus()
}
