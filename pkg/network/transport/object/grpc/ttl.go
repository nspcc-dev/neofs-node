package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// ttlPreProcessor is an implementation of requestPreProcessor interface used in Object service production.
	ttlPreProcessor struct {
		// List of static TTL conditions.
		staticCond []service.TTLCondition

		// List of TTL condition constructors.
		condPreps []ttlConditionPreparer

		// Processing function.
		fProc func(service.TTLSource, ...service.TTLCondition) error
	}

	// ttlConditionPreparer is an interface of TTL condition constructor.
	ttlConditionPreparer interface {
		// prepareTTLCondition creates TTL condition instance based on passed request.
		prepareTTLCondition(context.Context, object.Request) service.TTLCondition
	}

	// coreTTLCondPreparer is an implementation of ttlConditionPreparer interface used in Object service production.
	coreTTLCondPreparer struct {
		curAffChecker  containerAffiliationChecker
		prevAffChecker containerAffiliationChecker
	}

	containerAffiliationResult int

	// containerAffiliationChecker is an interface of container membership validator.
	containerAffiliationChecker interface {
		// Checks local node is affiliated with container with passed ID.
		affiliated(context.Context, CID) containerAffiliationResult
	}

	// corePlacementUtil is an implementation of containerAffiliationChecker interface used in Object service production.
	corePlacementUtil struct {
		// Previous network map flag.
		prevNetMap bool

		// Local node net address store.
		localAddrStore storage.AddressStore

		// Container nodes membership maintainer.
		placementBuilder Placer

		// Logging component.
		log *zap.Logger
	}
)

// decTTLPreProcessor is an implementation of requestPreProcessor.
type decTTLPreProcessor struct {
}

const (
	_ containerAffiliationResult = iota
	affUnknown
	affNotFound
	affPresence
	affAbsence
)

const (
	lmSelfAddrRecvFail = "could not receive local network address"
)

var (
	_ containerAffiliationChecker = (*corePlacementUtil)(nil)
	_ ttlConditionPreparer        = (*coreTTLCondPreparer)(nil)
	_ requestPreProcessor         = (*ttlPreProcessor)(nil)

	_ service.TTLCondition = validTTLCondition

	_ requestPreProcessor = (*decTTLPreProcessor)(nil)
)

// requestPreProcessor method implementation.
//
// Panics with pmEmptyServiceRequest on empty request.
//
// Constructs set of TTL conditions via internal constructors.
// Returns result of internal TTL conditions processing function.
func (s *ttlPreProcessor) preProcess(ctx context.Context, req serviceRequest) error {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	dynamicCond := make([]service.TTLCondition, len(s.condPreps))

	for i := range s.condPreps {
		dynamicCond[i] = s.condPreps[i].prepareTTLCondition(ctx, req)
	}

	return s.fProc(req, append(s.staticCond, dynamicCond...)...)
}

// ttlConditionPreparer method implementation.
//
// Condition returns ErrNotLocalContainer if and only if request is non-forwarding and local node is not presented
// in placement vector corresponding to request.
func (s *coreTTLCondPreparer) prepareTTLCondition(ctx context.Context, req object.Request) service.TTLCondition {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	return func(ttl uint32) error {
		// check forwarding assumption
		if ttl >= service.SingleForwardingTTL {
			// container affiliation doesn't matter
			return nil
		}

		// get group container ID from request body
		cid := req.CID()

		// check local node affiliation to container
		aff := s.curAffChecker.affiliated(ctx, cid)

		if aff == affAbsence && req.AllowPreviousNetMap() {
			// request can be forwarded to container members from previous epoch
			aff = s.prevAffChecker.affiliated(ctx, cid)
		}

		switch aff {
		case affUnknown:
			return errContainerAffiliationProblem
		case affNotFound:
			return &detailedError{
				error: errContainerNotFound,
				d:     containerDetails(cid, descContainerNotFound),
			}
		case affAbsence:
			return &detailedError{
				error: errNotLocalContainer,
				d:     containerDetails(cid, descNotLocalContainer),
			}
		}

		return nil
	}
}

// containerAffiliationChecker method implementation.
//
// If local network address store returns error, logger writes error and affUnknown returns.
// If placement builder returns error
//   - caused by ErrNotFound, affNotFound returns;
//   - status error with NotFound code, affNotFound returns;
//   - any other, affUnknown returns,
// Otherwise, if placement builder returns
//   - true, affPresence returns;
//   - false, affAbsence returns.
func (s *corePlacementUtil) affiliated(ctx context.Context, cid CID) containerAffiliationResult {
	selfAddr, err := s.localAddrStore.SelfAddr()
	if err != nil {
		s.log.Error(lmSelfAddrRecvFail, zap.Error(err))
		return affUnknown
	}

	aff, err := s.placementBuilder.IsContainerNode(ctx, selfAddr, cid, s.prevNetMap)
	if err != nil {
		if err := errors.Cause(err); errors.Is(err, container.ErrNotFound) {
			return affNotFound
		}

		return affUnknown
	}

	if !aff {
		return affAbsence
	}

	return affPresence
}

func processTTLConditions(req service.TTLSource, cs ...service.TTLCondition) error {
	ttl := req.GetTTL()

	for i := range cs {
		if err := cs[i](ttl); err != nil {
			return err
		}
	}

	return nil
}

func validTTLCondition(ttl uint32) error {
	if ttl < service.NonForwardingTTL {
		return errInvalidTTL
	}

	return nil
}

func (s *decTTLPreProcessor) preProcess(_ context.Context, req serviceRequest) error {
	req.SetTTL(req.GetTTL() - 1)
	return nil
}
