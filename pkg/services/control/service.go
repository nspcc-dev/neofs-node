package control

// SetBody sets health check request body.
func (m *HealthCheckRequest) SetBody(v *HealthCheckRequest_Body) {
	if m != nil {
		m.Body = v
	}
}

// SetSignature sets signature of the health check request body.
func (m *HealthCheckRequest) SetSignature(body *Signature) {
	if m != nil {
		m.Signature = body
	}
}

// ReadSignedData marshals request body to buf.
func (m *HealthCheckRequest) ReadSignedData(buf []byte) ([]byte, error) {
	_, err := m.GetBody().MarshalTo(buf)

	return buf, err
}

// SignedDataSize returns binary size of the request body.
func (m *HealthCheckRequest) SignedDataSize() int {
	return m.GetBody().Size()
}

// SetStatus sets health status of storage node.
func (m *HealthCheckResponse_Body) SetStatus(v HealthStatus) {
	if m != nil {
		m.Status = v
	}
}

// SetBody sets health check response body.
func (m *HealthCheckResponse) SetBody(v *HealthCheckResponse_Body) {
	if m != nil {
		m.Body = v
	}
}

// SetSignature sets signature of the health check response body.
func (m *HealthCheckResponse) SetSignature(v *Signature) {
	if m != nil {
		m.Signature = v
	}
}

// ReadSignedData marshals response body to buf.
func (m *HealthCheckResponse) ReadSignedData(buf []byte) ([]byte, error) {
	_, err := m.GetBody().MarshalTo(buf)

	return buf, err
}

// SignedDataSize returns binary size of the response body.
func (m *HealthCheckResponse) SignedDataSize() int {
	return m.GetBody().Size()
}
