package request

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/internal/uriutil"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type createContainerBody struct {
	Container *struct {
		Version       *protoVersion
		Nonce         *uuid.UUID
		Owner         *userID
		BasicACL      *basicACL
		StoragePolicy *storagePolicy
		Attributes    []*protocontainer.Container_Attribute
	}
	Witness *transaction.Witness
}

var createContainerCmd = &cobra.Command{
	Use:   "create-container body endpoint",
	Short: "Create container",
	Long:  "Request container creation in NeoFS",
	Args:  cobra.ExactArgs(2),
	RunE:  createContainer,
}

type awaitContainerCreationError int64 // seconds

func (x awaitContainerCreationError) Error() string {
	return fmt.Sprintf("container not saved within %ds", x)
}

func (x awaitContainerCreationError) Unwrap() error { return common.ErrAwaitTimeout }

func createContainer(cmd *cobra.Command, args []string) error {
	addr, isTLS, err := uriutil.Parse(args[1])
	if err != nil {
		return fmt.Errorf("invalid endpoint URI %s: %w", args[1], err)
	}

	bodyFile, err := os.Open(args[0])
	if err != nil {
		return fmt.Errorf("failed to open request body file: %w", err)
	}
	defer bodyFile.Close()
	bodyDec := json.NewDecoder(bodyFile)
	bodyDec.DisallowUnknownFields()
	var body createContainerBody
	if err := bodyDec.Decode(&body); err != nil {
		return fmt.Errorf("failed to decode request body JSON: %w", err)
	}
	if body.Container == nil {
		return errors.New("invalid request body JSON: missing container")
	}
	if body.Container.Version == nil {
		return errors.New("invalid request body JSON: invalid container: missing API version")
	}
	if body.Container.Nonce == nil {
		return errors.New("invalid request body JSON: invalid container: missing nonce")
	}
	if body.Container.Owner == nil {
		return errors.New("invalid request body JSON: invalid container: missing owner")
	}
	if body.Container.BasicACL == nil {
		return errors.New("invalid request body JSON: invalid container: missing basic ACL")
	}
	if body.Container.StoragePolicy == nil {
		return errors.New("invalid request body JSON: invalid container: missing storage policy")
	}
	if body.Witness == nil {
		return errors.New("invalid request body JSON: missing witness")
	}
	if len(body.Witness.InvocationScript) == 0 {
		return errors.New("invalid request body JSON: invalid witness: missing invocation script")
	}
	if len(body.Witness.VerificationScript) == 0 {
		return errors.New("invalid request body JSON: invalid witness: missing verification script")
	}

	pk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate ECDSA private key: %w", err)
	}
	req := &protocontainer.PutRequest{
		Body: &protocontainer.PutRequest_Body{
			Container: &protocontainer.Container{
				Version:         (*refs.Version)(body.Container.Version),
				OwnerId:         (*user.ID)(body.Container.Owner).ProtoMessage(),
				Nonce:           body.Container.Nonce[:],
				BasicAcl:        (*acl.Basic)(body.Container.BasicACL).Bits(),
				Attributes:      body.Container.Attributes,
				PlacementPolicy: (*netmap.PlacementPolicy)(body.Container.StoragePolicy).ProtoMessage(),
			},
			Signature: &refs.SignatureRFC6979{
				Key:  body.Witness.VerificationScript,
				Sign: body.Witness.InvocationScript,
			},
		},
	}
	if req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer((*neofsecdsa.Signer)(pk), req, nil); err != nil {
		return fmt.Errorf("failed to sign request with generated ECDSA private key: %w", err)
	}

	var transportCreds credentials.TransportCredentials
	if isTLS {
		transportCreds = credentials.NewTLS(nil)
	} else {
		transportCreds = insecure.NewCredentials()
	}

	cmd.Printf("Dialing %s...\n", addr)

	dialCtx, cancel := context.WithTimeout(cmd.Context(), defaultDialTimeout)
	defer cancel()
	// FIXME: inlined from https://github.com/nspcc-dev/neofs-sdk-go/blob/e9a774f505257dc48f005073162713805bdefebc/client/client.go#L178
	//  use new approach
	//nolint:staticcheck
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithReturnConnectionError(),
		grpc.FailOnNonTempDialError(true),
	)
	if err != nil {
		return fmt.Errorf("dial endpoint URI with gRPC: %w", err)
	}
	defer conn.Close()

	cmd.Println("Connected. Sending request...")

	reqCtx, cancel := context.WithTimeout(cmd.Context(), defaultRequestTimeout)
	defer cancel()
	cli := protocontainer.NewContainerServiceClient(conn)
	resp, err := cli.Put(reqCtx, req)
	if err != nil {
		return fmt.Errorf("transport failure: %w", err)
	}

	cmd.Println("Response received. Checking signatures...")

	if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
		return fmt.Errorf("failed to verify response signatures: %w", err)
	}

	cmd.Println("Signatures are valid. Checking status...")

	if err := apistatus.ToError(resp.GetMetaHeader().GetStatus()); err != nil {
		return fmt.Errorf("status failure: %w", err)
	}

	cmd.Println("Status OK. Checking body...")

	if resp.Body == nil {
		return errors.New("missing response body")
	}
	if resp.Body.ContainerId == nil {
		return errors.New("invalid response body: missing container ID field")
	}
	var id cid.ID
	if err := id.FromProtoMessage(resp.Body.ContainerId); err != nil {
		return fmt.Errorf("invalid response body: invalid container ID field: %w", err)
	}

	defer cmd.Printf("Container ID: %s\n", id)

	cmd.Println("Request accepted for processing. Polling success...")

	getReq := &protocontainer.GetRequest{
		Body: &protocontainer.GetRequest_Body{
			ContainerId: resp.Body.ContainerId,
		},
	}
	if getReq.VerifyHeader, err = neofscrypto.SignRequestWithBuffer((*neofsecdsa.Signer)(pk), getReq, nil); err != nil {
		return fmt.Errorf("failed to sign Get request with generated ECDSA private key: %w", err)
	}

	pollStart := time.Now()
	for attempt := 1; ; attempt++ {
		cmd.Println("Requesting container...")

		reqCtx, cancel := context.WithTimeout(cmd.Context(), defaultRequestTimeout)
		resp, err := cli.Get(reqCtx, getReq)
		cancel()
		if err != nil {
			return fmt.Errorf("transport failure: %w", err)
		}

		cmd.Println("Response received. Checking signatures...")

		if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
			return fmt.Errorf("failed to verify response signatures: %w", err)
		}

		cmd.Println("Signatures are valid. Checking status...")

		if err := apistatus.ToError(resp.GetMetaHeader().GetStatus()); err == nil {
			cmd.Println("Status OK. Operation succeeded.")
			return nil
		} else if !errors.Is(err, apistatus.ErrContainerNotFound) {
			return fmt.Errorf("status failure: %w", err)
		}

		const maxAttempts = 15
		if attempt == maxAttempts {
			break
		}

		const pollInterval = time.Second
		cmd.Printf("Container is still missing. Attempts left: %d. Retrying after %s...\n", maxAttempts-attempt, pollInterval)
		time.Sleep(pollInterval)
	}

	return awaitContainerCreationError(time.Since(pollStart) / time.Second) // to not see nanos
}
