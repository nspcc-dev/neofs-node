package control

// SetBody sets health check request body.
func (x *HealthCheckRequest) SetBody(v *HealthCheckRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetNetmapStatus sets status of the storage node in NeoFS network map.
func (x *HealthCheckResponse_Body) SetNetmapStatus(v NetmapStatus) {
	if x != nil {
		x.NetmapStatus = v
	}
}

// SetHealthStatus sets health status of the storage node application.
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

// SetStatus sets new storage node status in NeoFS network map.
func (x *SetNetmapStatusRequest_Body) SetStatus(v NetmapStatus) {
	if x != nil {
		x.Status = v
	}
}

// SetForceMaintenance sets force_maintenance flag in the message.
func (x *SetNetmapStatusRequest_Body) SetForceMaintenance() {
	x.ForceMaintenance = true
}

// SetBody sets body of the set netmap status request .
func (x *SetNetmapStatusRequest) SetBody(v *SetNetmapStatusRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets set body of the netmap status response.
func (x *SetNetmapStatusResponse) SetBody(v *SetNetmapStatusResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetAddressList sets list of objects to be removed in NeoFS API binary format.
func (x *DropObjectsRequest_Body) SetAddressList(v [][]byte) {
	if x != nil {
		x.AddressList = v
	}
}

// SetBody sets body of the set "Drop objects" request.
func (x *DropObjectsRequest) SetBody(v *DropObjectsRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets set body of the "Drop objects" response.
func (x *DropObjectsResponse) SetBody(v *DropObjectsResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets list shards request body.
func (x *ListShardsRequest) SetBody(v *ListShardsRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetShards sets shards of the storage node.
func (x *ListShardsResponse_Body) SetShards(v []*ShardInfo) {
	if x != nil {
		x.Shards = v
	}
}

// SetBody sets list shards response body.
func (x *ListShardsResponse) SetBody(v *ListShardsResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetShardIDList sets shard ID whose mode is requested to be set.
func (x *SetShardModeRequest_Body) SetShardIDList(v [][]byte) {
	if v != nil {
		x.Shard_ID = v
	}
}

// SetMode sets mode of the shard.
func (x *SetShardModeRequest_Body) SetMode(v ShardMode) {
	x.Mode = v
}

// ClearErrorCounter sets flag signifying whether error counter for shard should be cleared.
func (x *SetShardModeRequest_Body) ClearErrorCounter(reset bool) {
	x.ResetErrorCounter = reset
}

// SetBody sets request body.
func (x *SetShardModeRequest) SetBody(v *SetShardModeRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets body of the set shard mode response.
func (x *SetShardModeResponse) SetBody(v *SetShardModeResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetShardID sets shard ID for the dump shard request.
func (x *DumpShardRequest_Body) SetShardID(id []byte) {
	x.Shard_ID = id
}

// SetFilepath sets filepath for the dump shard request.
func (x *DumpShardRequest_Body) SetFilepath(p string) {
	x.Filepath = p
}

// SetIgnoreErrors sets ignore errors flag for the dump shard request.
func (x *DumpShardRequest_Body) SetIgnoreErrors(ignore bool) {
	x.IgnoreErrors = ignore
}

// SetBody sets request body.
func (x *DumpShardRequest) SetBody(v *DumpShardRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets response body.
func (x *DumpShardResponse) SetBody(v *DumpShardResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetShardID sets shard ID for the restore shard request.
func (x *RestoreShardRequest_Body) SetShardID(id []byte) {
	x.Shard_ID = id
}

// SetFilepath sets filepath for the restore shard request.
func (x *RestoreShardRequest_Body) SetFilepath(p string) {
	x.Filepath = p
}

// SetIgnoreErrors sets ignore errors flag for the restore shard request.
func (x *RestoreShardRequest_Body) SetIgnoreErrors(ignore bool) {
	x.IgnoreErrors = ignore
}

// SetBody sets request body.
func (x *RestoreShardRequest) SetBody(v *RestoreShardRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetBody sets response body.
func (x *RestoreShardResponse) SetBody(v *RestoreShardResponse_Body) {
	if x != nil {
		x.Body = v
	}
}
