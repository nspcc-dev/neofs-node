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

// SetBody sets notary list request body.
func (x *NotaryListRequest) SetBody(v *NotaryListRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetTransactions sets list of transactions of the IR notary requests.
func (x *NotaryListResponse_Body) SetTransactions(v []*TransactionInfo) {
	if x != nil {
		x.Transactions = v
	}
}

// SetBody sets network list response body.
func (x *NotaryListResponse) SetBody(v *NotaryListResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetMethod sets notary transaction method to request body.
func (x *NotaryRequestRequest_Body) SetMethod(v string) {
	if x != nil {
		x.Method = v
	}
}

// SetArgs sets notary transaction args to request body.
func (x *NotaryRequestRequest_Body) SetArgs(v [][]byte) {
	if x != nil {
		x.Args = v
	}
}

// SetBody sets notary request request body.
func (x *NotaryRequestRequest) SetBody(v *NotaryRequestRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetHash sets hash of the IR notary request.
func (x *NotaryRequestResponse_Body) SetHash(v []byte) {
	if x != nil {
		x.Hash = v
	}
}

// SetBody sets network epoch tick response body.
func (x *NotaryRequestResponse) SetBody(v *NotaryRequestResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetHash sets hash of the IR notary request to tick epoch.
func (x *NotarySignRequest_Body) SetHash(v []byte) {
	if x != nil {
		x.Hash = v
	}
}

// SetBody sets notary sign request body.
func (x *NotarySignRequest) SetBody(v *NotarySignRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets notary sign response body.
func (x *NotarySignResponse) SetBody(v *NotarySignResponse_Body) {
	if x != nil {
		x.Body = v
	}
}
