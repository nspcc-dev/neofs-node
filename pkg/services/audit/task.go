package audit

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Task groups groups the container audit parameters.
type Task struct {
	reporter Reporter

	auditContext context.Context

	idCnr cid.ID

	cnr container.Container

	nm *netmap.NetMap

	cnrNodes [][]netmap.NodeInfo

	sgList []storagegroup.StorageGroup
}

// WithReporter sets audit report writer.
func (t *Task) WithReporter(r Reporter) *Task {
	if t != nil {
		t.reporter = r
	}

	return t
}

// Reporter returns audit report writer.
func (t *Task) Reporter() Reporter {
	return t.reporter
}

// WithAuditContext sets context of the audit of the current epoch.
func (t *Task) WithAuditContext(ctx context.Context) *Task {
	if t != nil {
		t.auditContext = ctx
	}

	return t
}

// AuditContext returns context of the audit of the current epoch.
func (t *Task) AuditContext() context.Context {
	return t.auditContext
}

// WithContainerID sets identifier of the container under audit.
func (t *Task) WithContainerID(cnr cid.ID) *Task {
	if t != nil {
		t.idCnr = cnr
	}

	return t
}

// ContainerID returns identifier of the container under audit.
func (t *Task) ContainerID() cid.ID {
	return t.idCnr
}

// WithContainerStructure sets structure of the container under audit.
func (t *Task) WithContainerStructure(cnr container.Container) *Task {
	if t != nil {
		t.cnr = cnr
	}

	return t
}

// ContainerStructure returns structure of the container under audit.
func (t *Task) ContainerStructure() container.Container {
	return t.cnr
}

// WithContainerNodes sets nodes in the container under audit.
func (t *Task) WithContainerNodes(cnrNodes [][]netmap.NodeInfo) *Task {
	if t != nil {
		t.cnrNodes = cnrNodes
	}

	return t
}

// NetworkMap returns network map of audit epoch.
func (t *Task) NetworkMap() *netmap.NetMap {
	return t.nm
}

// WithNetworkMap sets network map of audit epoch.
func (t *Task) WithNetworkMap(nm *netmap.NetMap) *Task {
	if t != nil {
		t.nm = nm
	}

	return t
}

// ContainerNodes returns nodes in the container under audit.
func (t *Task) ContainerNodes() [][]netmap.NodeInfo {
	return t.cnrNodes
}

// WithStorageGroupList sets a list of storage groups from container under audit.
func (t *Task) WithStorageGroupList(sgList []storagegroup.StorageGroup) *Task {
	if t != nil {
		t.sgList = sgList
	}

	return t
}

// StorageGroupList returns list of storage groups from container under audit.
func (t *Task) StorageGroupList() []storagegroup.StorageGroup {
	return t.sgList
}
