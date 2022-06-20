package control

// SetBody sets health check request body.
func (x *HealthCheckRequest) SetBody(v *HealthCheckRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetHealthStatus sets health status of the IR application.
func (x *HealthCheckResponse_Body) SetHealthStatus(v HealthStatus) {
	if x != nil {
		x.HealthStatus = v
	}
}

// SetBody sets health check response body.
func (x *HealthCheckResponse) SetBody(v *HealthCheckResponse_Body) {
	if x != nil {
		x.Body = v
	}
}
