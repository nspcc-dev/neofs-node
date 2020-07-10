package state

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"expvar"
	"os"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/state"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var requestSignFunc = service.SignRequestData

func Test_nonForwarding(t *testing.T) {
	cases := []struct {
		err  error
		ttl  uint32
		name string
	}{
		{
			name: "ZeroTTL",
			ttl:  service.ZeroTTL,
			err:  status.Error(codes.InvalidArgument, service.ErrInvalidTTL.Error()),
		},
		{
			name: "SingleForwardingTTL",
			ttl:  service.SingleForwardingTTL,
			err:  status.Error(codes.InvalidArgument, service.ErrInvalidTTL.Error()),
		},
		{
			name: "NonForwardingTTL",
			ttl:  service.NonForwardingTTL,
			err:  nil,
		},
	}

	for i := range cases {
		tt := cases[i]
		t.Run(tt.name, func(t *testing.T) {
			err := nonForwarding(tt.ttl)
			switch tt.err {
			case nil:
				require.NoError(t, err, tt.name)
			default:
				require.EqualError(t, err, tt.err.Error())
			}
		})
	}
}

func Test_fetchOwners(t *testing.T) {
	l := test.NewTestLogger(false)

	t.Run("from config options", func(t *testing.T) {
		key0 := test.DecodeKey(0)
		require.NotEmpty(t, key0)

		data0 := crypto.MarshalPublicKey(&key0.PublicKey)
		hKey0 := hex.EncodeToString(data0)

		owner0, err := refs.NewOwnerID(&key0.PublicKey)
		require.NoError(t, err)

		v := viper.New()
		v.SetDefault("node.rpc.owners", []string{hKey0})

		owners := fetchOwners(l, v)
		require.Len(t, owners, 1)
		require.Contains(t, owners, owner0)
	})

	t.Run("from environment and config options", func(t *testing.T) {
		key0 := test.DecodeKey(0)
		require.NotEmpty(t, key0)

		data0 := crypto.MarshalPublicKey(&key0.PublicKey)
		hKey0 := hex.EncodeToString(data0)

		owner0, err := refs.NewOwnerID(&key0.PublicKey)
		require.NoError(t, err)

		key1 := test.DecodeKey(1)
		require.NotEmpty(t, key1)

		owner1, err := refs.NewOwnerID(&key1.PublicKey)
		require.NoError(t, err)

		data1 := crypto.MarshalPublicKey(&key1.PublicKey)
		hKey1 := hex.EncodeToString(data1)

		require.NoError(t, os.Setenv("NEOFS_NODE_RPC_OWNERS_0", hKey1))

		v := viper.New()
		v.AutomaticEnv()
		v.SetEnvPrefix("NeoFS")
		v.SetConfigType("yaml")
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		v.SetDefault("node.rpc.owners", []string{hKey0})

		require.NoError(t, v.ReadConfig(strings.NewReader("")))

		owners := fetchOwners(l, v)

		require.Len(t, owners, 2)
		require.Contains(t, owners, owner0)
		require.Contains(t, owners, owner1)
	})
}

func TestStateService_DumpConfig(t *testing.T) {
	cases := []struct {
		err  error
		ttl  uint32
		name string
		key  *ecdsa.PrivateKey
	}{
		{
			err:  nil,
			name: "allow",
			key:  test.DecodeKey(0),
			ttl:  service.NonForwardingTTL,
		},
		{
			name: "wrong ttl",
			key:  test.DecodeKey(0),
			ttl:  service.SingleForwardingTTL,
			err:  status.Error(codes.InvalidArgument, service.ErrInvalidTTL.Error()),
		},
	}
	key := test.DecodeKey(0)
	require.NotEmpty(t, key)

	owner, err := refs.NewOwnerID(&key.PublicKey)
	require.NoError(t, err)

	owners := map[refs.OwnerID]struct{}{
		owner: {},
	}

	viper.SetDefault("test", true)

	svc := stateService{
		owners: owners,
		config: viper.GetViper(),
	}

	for i := range cases {
		tt := cases[i]
		t.Run(tt.name, func(t *testing.T) {
			req := new(state.DumpRequest)

			req.SetTTL(tt.ttl)
			if tt.key != nil {
				require.NoError(t, requestSignFunc(tt.key, req))
			}

			res, err := svc.DumpConfig(context.Background(), req)
			switch tt.err {
			case nil:
				require.NoError(t, err, tt.name)
				require.NotEmpty(t, res)
				require.NotEmpty(t, res.Config)
			default:
				require.EqualError(t, err, tt.err.Error())
				require.Empty(t, res)
			}
		})
	}
}

func TestStateService_DumpVars(t *testing.T) {
	cases := []struct {
		err  error
		ttl  uint32
		name string
		key  *ecdsa.PrivateKey
	}{
		{
			err:  nil,
			name: "allow",
			key:  test.DecodeKey(0),
			ttl:  service.NonForwardingTTL,
		},
		{
			name: "wrong ttl",
			key:  test.DecodeKey(0),
			ttl:  service.SingleForwardingTTL,
			err:  status.Error(codes.InvalidArgument, service.ErrInvalidTTL.Error()),
		},
	}
	key := test.DecodeKey(0)
	require.NotEmpty(t, key)

	owner, err := refs.NewOwnerID(&key.PublicKey)
	require.NoError(t, err)

	owners := map[refs.OwnerID]struct{}{
		owner: {},
	}

	svc := stateService{owners: owners}

	expvar.NewString("test1").Set("test1")
	expvar.NewString("test2").Set("test2")

	for i := range cases {
		tt := cases[i]
		t.Run(tt.name, func(t *testing.T) {
			req := new(state.DumpVarsRequest)

			req.SetTTL(tt.ttl)
			if tt.key != nil {
				require.NoError(t, requestSignFunc(tt.key, req))
			}

			res, err := svc.DumpVars(nil, req)
			switch tt.err {
			case nil:
				require.NoError(t, err, tt.name)
				require.NotEmpty(t, res)
				require.NotEmpty(t, res.Variables)

				dump := make(map[string]interface{})
				require.NoError(t, json.Unmarshal(res.Variables, &dump))

				require.Contains(t, dump, "test1")
				require.Equal(t, dump["test1"], "test1")

				require.Contains(t, dump, "test2")
				require.Equal(t, dump["test2"], "test2")
			default:
				require.EqualError(t, err, tt.err.Error())
				require.Empty(t, res)
			}
		})
	}
}
