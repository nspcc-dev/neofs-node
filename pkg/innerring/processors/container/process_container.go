package container

import (
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	neofsid "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"go.uber.org/zap"
)

// putEvent is a common interface of Put and PutNamed event.
type putEvent interface {
	event.Event
	Container() []byte
	PublicKey() []byte
	Signature() []byte
	SessionToken() []byte
	NotaryRequest() *payload.P2PNotaryRequest
}

type putContainerContext struct {
	e putEvent

	name, zone string // from container structure
}

// Process new container from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerPut(put putEvent) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container put")
		return
	}

	ctx := &putContainerContext{
		e: put,
	}

	err := cp.checkPutContainer(ctx)
	if err != nil {
		cp.log.Error("put container check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approvePutContainer(ctx)
}

func (cp *Processor) checkPutContainer(ctx *putContainerContext) error {
	e := ctx.e

	// verify signature
	key, err := keys.NewPublicKeyFromBytes(e.PublicKey(), elliptic.P256())
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	binCnr := e.Container()
	tableHash := sha256.Sum256(binCnr)

	if !key.Verify(e.Signature(), tableHash[:]) {
		return errors.New("invalid signature")
	}

	// unmarshal container structure
	cnr := containerSDK.New()

	err = cnr.Unmarshal(binCnr)
	if err != nil {
		return fmt.Errorf("invalid binary container: %w", err)
	}

	// check native name and zone
	err = checkNNS(ctx, cnr)
	if err != nil {
		return fmt.Errorf("NNS: %w", err)
	}

	// perform format check
	err = container.CheckFormat(cnr)
	if err != nil {
		return fmt.Errorf("incorrect container format: %w", err)
	}

	// unmarshal session token if presented
	tok, err := tokenFromEvent(e)
	if err != nil {
		return err
	}

	if tok != nil {
		// check token context
		err = checkTokenContext(tok, func(c *session.ContainerContext) bool {
			return c.IsForPut()
		})
		if err != nil {
			return err
		}
	}

	cnr.SetSessionToken(tok)

	return cp.checkKeyOwnership(cnr, key)
}

func (cp *Processor) approvePutContainer(ctx *putContainerContext) {
	e := ctx.e

	var err error

	if nr := e.NotaryRequest(); nr != nil {
		// put event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// put event was received via notification service
		err = cp.cnrClient.Put(e.Container(), e.PublicKey(), e.Signature(), e.SessionToken(), ctx.name, ctx.zone)
	}
	if err != nil {
		cp.log.Error("could not approve put container",
			zap.String("error", err.Error()),
		)
	}
}

// Process delete container operation from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerDelete(delete *containerEvent.Delete) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container delete")
		return
	}

	err := cp.checkDeleteContainer(delete)
	if err != nil {
		cp.log.Error("delete container check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approveDeleteContainer(delete)
}

func (cp *Processor) checkDeleteContainer(e *containerEvent.Delete) error {
	binCID := e.ContainerID()

	// receive owner of the related container
	cnr, err := cp.cnrClient.Get(binCID)
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	token, err := tokenFromEvent(e)
	if err != nil {
		return err
	}

	var checkKeys keys.PublicKeys

	if token != nil {
		// check token context
		// TODO: think how to avoid version casts
		idV2 := new(refs.ContainerID)
		idV2.SetValue(binCID)

		id := cid.NewFromV2(idV2)

		err = checkTokenContextWithCID(token, id, func(c *session.ContainerContext) bool {
			return c.IsForDelete()
		})
		if err != nil {
			return err
		}

		key, err := keys.NewPublicKeyFromBytes(token.SessionKey(), elliptic.P256())
		if err != nil {
			return fmt.Errorf("invalid session key: %w", err)
		}

		// check token ownership
		err = cp.checkKeyOwnershipWithToken(cnr, key, token)
		if err != nil {
			return err
		}

		checkKeys = keys.PublicKeys{key}
	} else {
		prm := neofsid.AccountKeysPrm{}
		prm.SetID(cnr.OwnerID())

		// receive all owner keys from NeoFS ID contract
		checkKeys, err = cp.idClient.AccountKeys(prm)
		if err != nil {
			return fmt.Errorf("could not received owner keys %s: %w", cnr.OwnerID(), err)
		}
	}

	// verify signature
	cidHash := sha256.Sum256(binCID)
	sig := e.Signature()

	for _, key := range checkKeys {
		if key.Verify(sig, cidHash[:]) {
			return nil
		}
	}

	return errors.New("signature verification failed on all owner keys ")
}

func (cp *Processor) approveDeleteContainer(e *containerEvent.Delete) {
	var err error

	if nr := e.NotaryRequest(); nr != nil {
		// delete event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// delete event was received via notification service
		err = cp.cnrClient.Delete(e.ContainerID(), e.Signature(), e.SessionToken())
	}
	if err != nil {
		cp.log.Error("could not approve delete container",
			zap.String("error", err.Error()),
		)
	}
}

func checkNNS(ctx *putContainerContext, cnr *containerSDK.Container) error {
	// fetch native name and zone
	ctx.name, ctx.zone = containerSDK.GetNativeNameWithZone(cnr)

	// if PutNamed event => check if values in container correspond to args
	if named, ok := ctx.e.(interface {
		Name() string
		Zone() string
	}); ok {
		if name := named.Name(); name != ctx.name {
			return fmt.Errorf("names differ %s/%s", name, ctx.name)
		}

		if zone := named.Zone(); zone != ctx.zone {
			return fmt.Errorf("zones differ %s/%s", zone, ctx.zone)
		}
	}

	return nil
}
