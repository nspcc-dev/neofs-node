package object

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/service"
	"go.uber.org/zap"
)

type (
	// requestPreProcessor is an interface of Object service request installer.
	requestPreProcessor interface {
		// Performs preliminary request validation and preparation.
		preProcess(context.Context, serviceRequest) error
	}

	// complexPreProcessor is an implementation of requestPreProcessor interface.
	complexPreProcessor struct {
		// Sequence of requestPreProcessor instances.
		list []requestPreProcessor
	}

	signingPreProcessor struct {
		preProc requestPreProcessor
		key     *ecdsa.PrivateKey

		log *zap.Logger
	}
)

const pmEmptyServiceRequest = "empty service request"

var (
	_ requestPreProcessor = (*signingPreProcessor)(nil)
	_ requestPreProcessor = (*complexPreProcessor)(nil)
)

// requestPreProcessor method implementation.
//
// Passes request through internal requestPreProcessor.
// If internal requestPreProcessor returns non-nil error, this error returns.
// Returns result of signRequest function.
func (s *signingPreProcessor) preProcess(ctx context.Context, req serviceRequest) (err error) {
	if err = s.preProc.preProcess(ctx, req); err != nil {
		return
	} else if err = signRequest(s.key, req); err != nil {
		s.log.Error("could not re-sign request",
			zap.Error(err),
		)
		err = errReSigning
	}

	return
}

// requestPreProcessor method implementation.
//
// Panics with pmEmptyServiceRequest on nil request argument.
//
// Passes request through the sequence of requestPreProcessor instances.
// Any non-nil error returned by some instance returns.
//
// Warn: adding instance to list itself provoke endless recursion.
func (s *complexPreProcessor) preProcess(ctx context.Context, req serviceRequest) error {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	for i := range s.list {
		if err := s.list[i].preProcess(ctx, req); err != nil {
			return err
		}
	}

	return nil
}

// Creates requestPreProcessor based on Params.
//
// Uses complexPreProcessor instance as a result implementation.
//
// Adds to next preprocessors to list:
//  * verifyPreProcessor;
//  * ttlPreProcessor;
//  * epochPreProcessor, if CheckEpochSync flag is set in params.
//  * aclPreProcessor, if CheckAcl flag is set in params.
func newPreProcessor(p *Params) requestPreProcessor {
	preProcList := make([]requestPreProcessor, 0)

	if p.CheckACL {
		preProcList = append(preProcList, &aclPreProcessor{
			log: p.Logger,

			aclInfoReceiver: p.aclInfoReceiver,

			reqActionCalc: p.requestActionCalculator,

			localStore: p.LocalStore,

			extACLSource: p.ExtendedACLSource,

			bearerVerifier: &complexBearerVerifier{
				items: []bearerTokenVerifier{
					&bearerActualityVerifier{
						epochRecv: p.EpochReceiver,
					},
					new(bearerSignatureVerifier),
					&bearerOwnershipVerifier{
						cnrStorage: p.ContainerStorage,
					},
				},
			},
		})
	}

	preProcList = append(preProcList,
		&verifyPreProcessor{
			fVerify: requestVerifyFunc,
		},

		&ttlPreProcessor{
			staticCond: []service.TTLCondition{
				validTTLCondition,
			},
			condPreps: []ttlConditionPreparer{
				&coreTTLCondPreparer{
					curAffChecker: &corePlacementUtil{
						prevNetMap:       false,
						localAddrStore:   p.AddressStore,
						placementBuilder: p.Placer,
						log:              p.Logger,
					},
					prevAffChecker: &corePlacementUtil{
						prevNetMap:       true,
						localAddrStore:   p.AddressStore,
						placementBuilder: p.Placer,
						log:              p.Logger,
					},
				},
			},
			fProc: processTTLConditions,
		},

		&tokenPreProcessor{
			staticVerifier: newComplexTokenVerifier(
				&tokenEpochsVerifier{
					epochRecv: p.EpochReceiver,
				},
			),
		},

		new(decTTLPreProcessor),
	)

	return &signingPreProcessor{
		preProc: &complexPreProcessor{list: preProcList},
		key:     p.Key,
	}
}
