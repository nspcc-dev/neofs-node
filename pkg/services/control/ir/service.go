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

// SetBody sets network list request body.
func (x *NetworkListRequest) SetBody(v *NetworkListRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetHashes sets list of hashes of the IR notary requests.
func (x *NetworkListResponse_Body) SetHashes(v []string) {
	if x != nil {
		x.Hashes = v
	}
}

// SetBody sets network list response body.
func (x *NetworkListResponse) SetBody(v *NetworkListResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets network epoch tick request body.
func (x *NetworkEpochTickRequest) SetBody(v *NetworkEpochTickRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetHash sets hash of the IR notary request to tick epoch.
func (x *NetworkEpochTickResponse_Body) SetHash(v string) {
	if x != nil {
		x.Hash = v
	}
}

// SetBody sets network epoch tick response body.
func (x *NetworkEpochTickResponse) SetBody(v *NetworkEpochTickResponse_Body) {
	if x != nil {
		x.Body = v
	}
}
