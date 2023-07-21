package intermediate

import (
	"crypto/ecdsa"
	"fmt"

	repClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"go.uber.org/zap"
)

// FinalWriterProviderPrm groups the required parameters of the FinalWriterProvider's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type FinalWriterProviderPrm struct {
	PrivatKey *ecdsa.PrivateKey
	PubKey    []byte
	Client    *repClient.Client
}

// NewFinalWriterProvider creates a new instance of the FinalWriterProvider.
//
// Panics if at least one value of the parameters is invalid.
//
// The created FinalWriterProvider does not require additional
// initialization and is completely ready for work.
func NewFinalWriterProvider(prm FinalWriterProviderPrm, opts ...FinalWriterOption) *FinalWriterProvider {
	o := defaultFinalWriterOptionsOpts()

	for i := range opts {
		opts[i](o)
	}

	return &FinalWriterProvider{
		prm:  prm,
		opts: o,
	}
}

// FinalWriterProvider is an implementation of the reputation.eigentrust.calculator
// IntermediateWriterProvider interface. It inits FinalWriter.
type FinalWriterProvider struct {
	prm  FinalWriterProviderPrm
	opts *finalWriterOptions
}

func (fwp FinalWriterProvider) InitIntermediateWriter(
	_ eigentrustcalc.Context) (eigentrustcalc.IntermediateWriter, error) {
	return &FinalWriter{
		privatKey: fwp.prm.PrivatKey,
		pubKey:    fwp.prm.PubKey,
		client:    fwp.prm.Client,
		l:         fwp.opts.log,
	}, nil
}

// FinalWriter is an implementation of the reputation.eigentrust.calculator IntermediateWriter
// interface that writes GlobalTrust to contract directly.
type FinalWriter struct {
	privatKey *ecdsa.PrivateKey
	pubKey    []byte
	client    *repClient.Client

	l *zap.Logger
}

func (fw FinalWriter) WriteIntermediateTrust(t eigentrust.IterationTrust) error {
	fw.l.Debug("start writing global trusts to contract")

	args := repClient.PutPrm{}

	apiTrustedPeerID := t.Peer()

	var apiTrust apireputation.Trust
	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(t.Peer())

	var managerPublicKey [33]byte
	copy(managerPublicKey[:], fw.pubKey)

	var apiMangerPeerID apireputation.PeerID
	apiMangerPeerID.SetPublicKey(managerPublicKey[:])

	var gTrust apireputation.GlobalTrust
	gTrust.SetTrust(apiTrust)
	gTrust.SetManager(apiMangerPeerID)

	err := gTrust.Sign(neofsecdsa.Signer(*fw.privatKey))
	if err != nil {
		fw.l.Debug(
			"failed to sign global trust",
			zap.Error(err),
		)
		return fmt.Errorf("failed to sign global trust: %w", err)
	}

	args.SetEpoch(t.Epoch())
	args.SetValue(gTrust)
	args.SetPeerID(apiTrustedPeerID)

	err = fw.client.Put(
		args,
	)
	if err != nil {
		fw.l.Debug(
			"failed to write global trust to contract",
			zap.Error(err),
		)
		return fmt.Errorf("failed to write global trust to contract: %w", err)
	}

	fw.l.Debug(
		"sent global trust to contract",
		zap.Uint64("epoch", t.Epoch()),
		zap.Float64("value", t.Value().Float64()),
		zap.Stringer("peer", t.Peer()),
	)

	return nil
}

type finalWriterOptions struct {
	log *zap.Logger
}

type FinalWriterOption func(*finalWriterOptions)

func defaultFinalWriterOptionsOpts() *finalWriterOptions {
	return &finalWriterOptions{
		log: zap.L(),
	}
}

func FinalWriterWithLogger(l *zap.Logger) FinalWriterOption {
	return func(o *finalWriterOptions) {
		if l != nil {
			o.log = l
		}
	}
}
