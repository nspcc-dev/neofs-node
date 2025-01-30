package audit

import (
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoaudit "github.com/nspcc-dev/neofs-sdk-go/proto/audit"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/proto"
)

// Result groups results of the data audit performed by the Inner Ring member.
//
// New instances must be constructed using [NewResult].
type Result struct {
	// NeoFS epoch when the data audit was performed.
	AuditEpoch uint64
	// Container which data was audited.
	Container cid.ID
	// Binary-encoded public key of the Inner Ring member who conducted the audit.
	AuditorPublicKey []byte
	// Completion flag.
	Completed bool
	// Proof-of-Retrievability check results.
	PoR struct {
		// Number of requests made to get all headers of the objects inside storage
		// groups.
		Requests uint32
		// Number of retries.
		Retries uint32
		// Storage groups for which the check was passed.
		PassedStorageGroups []oid.ID
		// Storage groups for which the check was failed.
		FailedStorageGroups []oid.ID
	}
	// Proof-of-Placement check results.
	PoP struct {
		// Number of sampled objects placed in an optimal way compliant with the
		// Container storage policy.
		Hits uint32
		// Number of objects placed in a suboptimal way according to the Container
		// placement policy, but still at a satisfactory replication level.
		Misses uint32
		// Number of objects stored in a way not confirming to the Container storage
		// policy.
		Failures uint32
	}
	// Proof-of-Data-Possession check results.
	PDP struct {
		// Binary-encoded public keys of sStorage nodes that passed at least one check.
		PassedStorageNodes [][]byte
		// Binary-encoded public keys of sStorage nodes that failed at least one check.
		FailedStorageNodes [][]byte
	}

	versionSet bool
	version    version.Version
}

// NewResult constructs new audit result for the container performed by subject
// authenticated with specified binary-encoded public key at the given epoch.
func NewResult(auditorPubKey []byte, epoch uint64, cnr cid.ID) Result {
	var res Result
	res.AuditorPublicKey = auditorPubKey
	res.AuditEpoch = epoch
	res.Container = cnr
	res.version, res.versionSet = version.Current(), true
	return res
}

// Marshal encodes Result into a Protocol Buffers V3 binary format.
func (r Result) Marshal() []byte {
	var ver *refs.Version
	if r.versionSet {
		ver = r.version.ProtoMessage()
	}
	cnr := r.Container.ProtoMessage()
	var passSG []*refs.ObjectID
	if r.PoR.PassedStorageGroups != nil {
		passSG = make([]*refs.ObjectID, len(r.PoR.PassedStorageGroups))
		for i := range r.PoR.PassedStorageGroups {
			passSG[i] = r.PoR.PassedStorageGroups[i].ProtoMessage()
		}
	}
	var failSG []*refs.ObjectID
	if r.PoR.FailedStorageGroups != nil {
		failSG = make([]*refs.ObjectID, len(r.PoR.FailedStorageGroups))
		for i := range r.PoR.FailedStorageGroups {
			failSG[i] = r.PoR.FailedStorageGroups[i].ProtoMessage()
		}
	}
	var m = protoaudit.DataAuditResult{
		Version:     ver,
		ContainerId: cnr,
		AuditEpoch:  r.AuditEpoch,
		PublicKey:   r.AuditorPublicKey,
		Complete:    r.Completed,
		Requests:    r.PoR.Requests,
		Retries:     r.PoR.Retries,
		PassSg:      passSG,
		FailSg:      failSG,
		Hit:         r.PoP.Hits,
		Miss:        r.PoP.Misses,
		Fail:        r.PoP.Failures,
		PassNodes:   r.PDP.PassedStorageNodes,
		FailNodes:   r.PDP.FailedStorageNodes,
	}

	var b = make([]byte, m.MarshaledSize())
	m.MarshalStable(b)
	return b
}

// Unmarshal decodes Protocol Buffers V3 binary data into the Result. Returns an
// error describing a format violation of the specified fields. Unmarshal does
// not check presence of the required fields and, at the same time, checks
// format of the presented ones.
func (r *Result) Unmarshal(data []byte) error {
	var m protoaudit.DataAuditResult
	if err := proto.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("decode protobuf: %w", err)
	}

	if m.ContainerId == nil {
		return errors.New("missing container")
	} else if err := r.Container.FromProtoMessage(m.ContainerId); err != nil {
		return fmt.Errorf("invalid container: %w", err)
	}
	r.versionSet = m.Version != nil
	if r.versionSet {
		if err := r.version.FromProtoMessage(m.Version); err != nil {
			return fmt.Errorf("invalid protocol version: %w", err)
		}
	}
	if len(m.PassSg) > 0 {
		r.PoR.PassedStorageGroups = make([]oid.ID, len(m.PassSg))
		for i := range m.PassSg {
			if err := r.PoR.PassedStorageGroups[i].FromProtoMessage(m.PassSg[i]); err != nil {
				return fmt.Errorf("invalid passed storage group #%d: %w", i, err)
			}
		}
	} else {
		r.PoR.PassedStorageGroups = nil
	}
	if len(m.FailSg) > 0 {
		r.PoR.FailedStorageGroups = make([]oid.ID, len(m.FailSg))
		for i := range m.FailSg {
			if err := r.PoR.FailedStorageGroups[i].FromProtoMessage(m.FailSg[i]); err != nil {
				return fmt.Errorf("invalid failed storage group #%d: %w", i, err)
			}
		}
	} else {
		r.PoR.FailedStorageGroups = nil
	}
	r.AuditEpoch = m.GetAuditEpoch()
	r.AuditorPublicKey = m.GetPublicKey()
	r.Completed = m.GetComplete()
	r.PoR.Requests = m.GetRequests()
	r.PoR.Retries = m.GetRetries()
	r.PoP.Hits = m.GetHit()
	r.PoP.Misses = m.GetMiss()
	r.PoP.Failures = m.GetFail()
	r.PDP.PassedStorageNodes = m.GetPassNodes()
	r.PDP.FailedStorageNodes = m.GetFailNodes()

	return nil
}
