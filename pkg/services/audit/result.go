package audit

import (
	"errors"
	"fmt"

	apiaudit "github.com/nspcc-dev/neofs-api-go/v2/audit"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
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
		ver = new(refs.Version)
		r.version.WriteToV2(ver)
	}
	cnr := new(refs.ContainerID)
	r.Container.WriteToV2(cnr)
	var passSG []refs.ObjectID
	if r.PoR.PassedStorageGroups != nil {
		passSG = make([]refs.ObjectID, len(r.PoR.PassedStorageGroups))
		for i := range r.PoR.PassedStorageGroups {
			r.PoR.PassedStorageGroups[i].WriteToV2(&passSG[i])
		}
	}
	var failSG []refs.ObjectID
	if r.PoR.FailedStorageGroups != nil {
		failSG = make([]refs.ObjectID, len(r.PoR.FailedStorageGroups))
		for i := range r.PoR.FailedStorageGroups {
			r.PoR.FailedStorageGroups[i].WriteToV2(&failSG[i])
		}
	}
	var m apiaudit.DataAuditResult
	m.SetVersion(ver)
	m.SetContainerID(cnr)
	m.SetAuditEpoch(r.AuditEpoch)
	m.SetPublicKey(r.AuditorPublicKey)
	m.SetComplete(r.Completed)
	m.SetRequests(r.PoR.Requests)
	m.SetRetries(r.PoR.Retries)
	m.SetPassSG(passSG)
	m.SetFailSG(failSG)
	m.SetHit(r.PoP.Hits)
	m.SetMiss(r.PoP.Misses)
	m.SetFail(r.PoP.Failures)
	m.SetPassNodes(r.PDP.PassedStorageNodes)
	m.SetFailNodes(r.PDP.FailedStorageNodes)

	return m.StableMarshal(nil)
}

// Unmarshal decodes Protocol Buffers V3 binary data into the Result. Returns an
// error describing a format violation of the specified fields. Unmarshal does
// not check presence of the required fields and, at the same time, checks
// format of the presented ones.
func (r *Result) Unmarshal(data []byte) error {
	var m apiaudit.DataAuditResult
	if err := m.Unmarshal(data); err != nil {
		return fmt.Errorf("decode protobuf: %w", err)
	}

	if cnr := m.GetContainerID(); cnr == nil {
		return errors.New("missing container")
	} else if err := r.Container.ReadFromV2(*cnr); err != nil {
		return fmt.Errorf("invalid container: %w", err)
	}
	ver := m.GetVersion()
	if r.versionSet = ver != nil; r.versionSet {
		if err := r.version.ReadFromV2(*ver); err != nil {
			return fmt.Errorf("invalid protocol version: %w", err)
		}
	}
	passSG := m.GetPassSG()
	if len(passSG) > 0 {
		r.PoR.PassedStorageGroups = make([]oid.ID, len(passSG))
		for i := range passSG {
			if err := r.PoR.PassedStorageGroups[i].ReadFromV2(passSG[i]); err != nil {
				return fmt.Errorf("invalid passed storage group #%d: %w", i, err)
			}
		}
	} else {
		r.PoR.PassedStorageGroups = nil
	}
	failSG := m.GetFailSG()
	if len(failSG) > 0 {
		r.PoR.FailedStorageGroups = make([]oid.ID, len(failSG))
		for i := range failSG {
			if err := r.PoR.FailedStorageGroups[i].ReadFromV2(failSG[i]); err != nil {
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
