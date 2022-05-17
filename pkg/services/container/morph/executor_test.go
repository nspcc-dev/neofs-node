package container_test

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	containerSvcMorph "github.com/nspcc-dev/neofs-node/pkg/services/container/morph"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/stretchr/testify/require"
)

type mock struct {
	containerSvcMorph.Reader
}

func (m mock) Put(_ *containerSDK.Container) (*cid.ID, error) {
	return new(cid.ID), nil
}

func (m mock) Delete(_ containerCore.RemovalWitness) error {
	return nil
}

func (m mock) PutEACL(_ *eacl.Table) error {
	return nil
}

func TestInvalidToken(t *testing.T) {
	m := mock{}
	e := containerSvcMorph.NewExecutor(m, m)

	cnr := cidtest.ID()

	var cnrV2 refs.ContainerID
	cnr.WriteToV2(&cnrV2)

	tests := []struct {
		name string
		op   func(e containerSvc.ServiceExecutor, ctx containerSvc.ContextWithToken) error
	}{
		{
			name: "put",
			op: func(e containerSvc.ServiceExecutor, ctx containerSvc.ContextWithToken) (err error) {
				var reqBody container.PutRequestBody
				reqBody.SetSignature(new(refs.Signature))

				_, err = e.Put(ctx, &reqBody)
				return
			},
		},
		{
			name: "delete",
			op: func(e containerSvc.ServiceExecutor, ctx containerSvc.ContextWithToken) (err error) {
				var reqBody container.DeleteRequestBody
				reqBody.SetContainerID(&cnrV2)

				_, err = e.Delete(ctx, &reqBody)
				return
			},
		},
		{
			name: "setEACL",
			op: func(e containerSvc.ServiceExecutor, ctx containerSvc.ContextWithToken) (err error) {
				var reqBody container.SetExtendedACLRequestBody
				reqBody.SetSignature(new(refs.Signature))

				_, err = e.SetExtendedACL(ctx, &reqBody)
				return
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := containerSvc.ContextWithToken{
				Context:      context.Background(),
				SessionToken: generateToken(new(session.ObjectSessionContext)),
			}
			require.Error(t, test.op(e, ctx), containerSvcMorph.ErrInvalidContext)

			ctx.SessionToken = generateToken(new(session.ContainerSessionContext))
			require.NoError(t, test.op(e, ctx))

			ctx.SessionToken = nil
			require.NoError(t, test.op(e, ctx))
		})
	}
}

func generateToken(ctx session.TokenContext) *session.Token {
	body := new(session.TokenBody)
	body.SetContext(ctx)

	tok := new(session.Token)
	tok.SetBody(body)

	return tok
}
