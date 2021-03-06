package control_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
)

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
	body.SetNetmapStatus(control.NetmapStatus_ONLINE)
	body.SetHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	return body
}

func equalHealthCheckResponseBodies(b1, b2 *control.HealthCheckResponse_Body) bool {
	return b1.GetNetmapStatus() == b2.GetNetmapStatus() &&
		b1.GetHealthStatus() == b2.GetHealthStatus()
}

func TestNetmapSnapshotResponse_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateNetmapSnapshotResponseBody(),
		new(control.NetmapSnapshotResponse_Body),
		func(m1, m2 protoMessage) bool {
			return equalNetmapSnapshotResponseBodies(
				m1.(*control.NetmapSnapshotResponse_Body),
				m2.(*control.NetmapSnapshotResponse_Body),
			)
		},
	)
}

func generateNetmapSnapshotResponseBody() *control.NetmapSnapshotResponse_Body {
	body := new(control.NetmapSnapshotResponse_Body)
	body.SetNetmap(generateNetmap())

	return body
}

func equalNetmapSnapshotResponseBodies(b1, b2 *control.NetmapSnapshotResponse_Body) bool {
	return equalNetmaps(b1.GetNetmap(), b2.GetNetmap())
}

func TestSetNetmapStatusRequest_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateSetNetmapStatusRequestBody(),
		new(control.SetNetmapStatusRequest_Body),
		func(m1, m2 protoMessage) bool {
			return equalSetnetmapStatusRequestBodies(
				m1.(*control.SetNetmapStatusRequest_Body),
				m2.(*control.SetNetmapStatusRequest_Body),
			)
		},
	)
}

func generateSetNetmapStatusRequestBody() *control.SetNetmapStatusRequest_Body {
	body := new(control.SetNetmapStatusRequest_Body)
	body.SetStatus(control.NetmapStatus_ONLINE)

	return body
}

func equalSetnetmapStatusRequestBodies(b1, b2 *control.SetNetmapStatusRequest_Body) bool {
	return b1.GetStatus() == b2.GetStatus()
}
