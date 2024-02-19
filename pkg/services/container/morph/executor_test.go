package container_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	containerSvcMorph "github.com/nspcc-dev/neofs-node/pkg/services/container/morph"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	containertest "github.com/nspcc-dev/neofs-sdk-go/container/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	sessionsdk "github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type mock struct {
	cnr containerSDK.Container
}

func (m mock) Get(_ cid.ID) (*containerCore.Container, error) {
	return &containerCore.Container{Value: m.cnr}, nil
}

func (m mock) GetEACL(_ cid.ID) (*containerCore.EACL, error) {
	return nil, nil
}

func (m mock) List(_ *user.ID) ([]cid.ID, error) {
	return nil, nil
}

func (m mock) Put(_ containerCore.Container) (*cid.ID, error) {
	return new(cid.ID), nil
}

func (m mock) Delete(_ containerCore.RemovalWitness) error {
	return nil
}

func (m mock) PutEACL(_ containerCore.EACL) error {
	return nil
}

func TestExecutor(t *testing.T) {
	cnr := cidtest.ID()

	var cnrV2 refs.ContainerID
	cnr.WriteToV2(&cnrV2)

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)

	signer := user.NewAutoIDSigner(priv.PrivateKey)

	sign := func(reqBody interface {
		StableMarshal([]byte) []byte
		SetSignature(signature *refs.Signature)
	}) {
		var sig neofscrypto.Signature
		require.NoError(t, sig.Calculate(signer, reqBody.StableMarshal(nil)))

		var sigV2 refs.Signature
		sig.WriteToV2(&sigV2)
		reqBody.SetSignature(&sigV2)
	}

	tok := sessiontest.Container()
	tok.ApplyOnlyTo(cnr)
	require.NoError(t, tok.Sign(signer))

	var tokV2 session.Token
	tok.WriteToV2(&tokV2)

	realContainer := containertest.Container(t)
	realContainer.SetOwner(tok.Issuer())

	m := mock{cnr: realContainer}
	e := containerSvcMorph.NewExecutor(m, m)

	tests := []struct {
		name string
		op   func(e containerSvc.ServiceExecutor, tokV2 *session.Token) error
	}{
		{
			name: "put",
			op: func(e containerSvc.ServiceExecutor, tokV2 *session.Token) (err error) {
				var reqBody container.PutRequestBody

				cnr := containertest.Container(t)

				var cnrV2 container.Container
				cnr.WriteToV2(&cnrV2)

				reqBody.SetContainer(&cnrV2)
				sign(&reqBody)

				_, err = e.Put(context.TODO(), tokV2, &reqBody)
				return
			},
		},
		{
			name: "delete",
			op: func(e containerSvc.ServiceExecutor, tokV2 *session.Token) (err error) {
				var reqBody container.DeleteRequestBody
				reqBody.SetContainerID(&cnrV2)
				sign(&reqBody)

				cc, ok := tokV2.GetBody().GetContext().(*session.ContainerSessionContext)
				if ok {
					cc.SetVerb(session.ContainerVerbDelete)
					signV2Token(t, signer, tokV2)
				}

				_, err = e.Delete(context.TODO(), tokV2, &reqBody)
				return
			},
		},
		{
			name: "setEACL",
			op: func(e containerSvc.ServiceExecutor, tokV2 *session.Token) (err error) {
				var reqBody container.SetExtendedACLRequestBody
				reqBody.SetSignature(new(refs.Signature))
				sign(&reqBody)

				cc, ok := tokV2.GetBody().GetContext().(*session.ContainerSessionContext)
				if ok {
					cc.SetVerb(session.ContainerVerbSetEACL)
					signV2Token(t, signer, tokV2)
				}

				_, err = e.SetExtendedACL(context.TODO(), tokV2, &reqBody)
				return
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tok := generateToken(t, new(session.ObjectSessionContext), signer)
			require.Error(t, test.op(e, tok))

			require.NoError(t, test.op(e, &tokV2))

			require.NoError(t, test.op(e, nil))
		})
	}
}

func TestValidateToken(t *testing.T) {
	cID := cidtest.ID()
	var cIDV2 refs.ContainerID
	cID.WriteToV2(&cIDV2)

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)

	signer := user.NewAutoIDSigner(priv.PrivateKey)

	tok := sessiontest.Container()
	tok.ApplyOnlyTo(cID)
	tok.ForVerb(sessionsdk.VerbContainerDelete)
	require.NoError(t, tok.Sign(signer))

	cnr := containertest.Container(t)
	cnr.SetOwner(tok.Issuer())

	var cnrV2 container.Container
	cnr.WriteToV2(&cnrV2)

	m := mock{cnr: cnr}
	e := containerSvcMorph.NewExecutor(m, m)

	t.Run("non-container token", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tokV2 session.Token
		objectSession := sessiontest.Object()
		require.NoError(t, objectSession.Sign(signer))

		objectSession.WriteToV2(&tokV2)

		_, err = e.Delete(context.TODO(), &tokV2, &reqBody)
		require.Error(t, err)

		return
	})

	t.Run("wrong verb token", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tokCopy sessionsdk.Container
		tok.CopyTo(&tokCopy)
		tokCopy.ForVerb(sessionsdk.VerbContainerPut)

		var tokV2 session.Token
		tokCopy.WriteToV2(&tokV2)

		_, err = e.Delete(context.TODO(), &tokV2, &reqBody)
		require.Error(t, err)

		return
	})

	t.Run("incorrect cID", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tokV2 session.Token
		var cIDV2 refs.ContainerID
		cc := new(session.ContainerSessionContext)
		b := new(session.TokenBody)

		cIDV2.SetValue(make([]byte, sha256.Size+1))
		cc.SetContainerID(&cIDV2)
		b.SetContext(cc)
		tokV2.SetBody(b)

		_, err = e.Delete(context.TODO(), &tokV2, &reqBody)
		require.Error(t, err)
	})

	t.Run("different container ID", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tokCopy sessionsdk.Container
		tok.CopyTo(&tokCopy)
		tokCopy.ApplyOnlyTo(cidtest.ID())

		require.NoError(t, tokCopy.Sign(signer))

		var tokV2 session.Token
		tokCopy.WriteToV2(&tokV2)

		_, err = e.Delete(context.TODO(), &tokV2, &reqBody)
		require.Error(t, err)
	})

	t.Run("different issuer", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tokV2 session.Token
		tok.WriteToV2(&tokV2)

		var ownerV2Wrong refs.OwnerID
		ownerWrong := usertest.ID(t)
		ownerWrong.WriteToV2(&ownerV2Wrong)

		tokV2.GetBody().SetOwnerID(&ownerV2Wrong)

		_, err = e.Delete(context.TODO(), &tokV2, &reqBody)
		require.Error(t, err)
	})

	t.Run("incorrect signature", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tokV2 session.Token
		tok.WriteToV2(&tokV2)

		var wrongSig refs.Signature
		tokV2.SetSignature(&wrongSig)

		_, err = e.Delete(context.TODO(), &tokV2, &reqBody)
		require.Error(t, err)
	})

	t.Run("wildcard support", func(t *testing.T) {
		var reqBody container.DeleteRequestBody
		reqBody.SetContainerID(&cIDV2)

		var tok sessionsdk.Container

		priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		tok.SetExp(11)
		tok.SetNbf(22)
		tok.SetIat(33)
		tok.ForVerb(sessionsdk.VerbContainerDelete)
		tok.SetID(uuid.New())
		tok.SetAuthKey((*neofsecdsa.PublicKey)(&priv.PublicKey))
		require.NoError(t, tok.Sign(signer))

		var tokV2 session.Token
		tok.WriteToV2(&tokV2)

		m := &mock{cnr: cnr}
		e := containerSvcMorph.NewExecutor(m, m)

		t.Run("wrong owner", func(t *testing.T) {
			m.cnr = containertest.Container(t)

			_, err := e.Delete(context.TODO(), &tokV2, &reqBody)
			require.Error(t, err)
		})

		t.Run("correct owner", func(t *testing.T) {
			m.cnr = cnr

			_, err := e.Delete(context.TODO(), &tokV2, &reqBody)
			require.NoError(t, err)
		})
	})
}

func generateToken(t *testing.T, ctx session.TokenContext, signer user.Signer) *session.Token {
	body := new(session.TokenBody)
	body.SetContext(ctx)

	tok := new(session.Token)
	tok.SetBody(body)

	signV2Token(t, signer, tok)

	return tok
}

func signV2Token(t *testing.T, signer user.Signer, tokV2 *session.Token) {
	sig, err := signer.Sign(tokV2.GetBody().StableMarshal(nil))
	require.NoError(t, err)

	var sigV2 refs.Signature
	sigV2.SetKey(neofscrypto.PublicKeyBytes(signer.Public()))
	sigV2.SetScheme(refs.SignatureScheme(signer.Scheme()))
	sigV2.SetSign(sig)

	tokV2.SetSignature(&sigV2)
}
