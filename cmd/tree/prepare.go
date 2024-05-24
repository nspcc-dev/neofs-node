package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/message"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

func prepareLoadMocks(args []string) {
	numOfNodes := flag.Uint("node-number", 0, "Number of nodes that takes part in testing")
	_ = flag.CommandLine.Parse(args)

	if numOfNodes == nil || *numOfNodes == 0 {
		panic("node-number must be greater than zero")
	}

	var kk []*keys.PrivateKey
	for i := 0; i < int(*numOfNodes); i++ {
		k, err := keys.NewPrivateKey()
		if err != nil {
			panic(fmt.Errorf("generating private key: %w", err))
		}

		kk = append(kk, k)
		fileName := fmt.Sprintf("key-%d", i)

		f, err := os.Create(fileName)
		if err != nil {
			panic(fmt.Errorf("creating file for private key: %w", err))
		}

		_, err = f.Write(k.Bytes())
		if err != nil {
			panic(fmt.Errorf("writing private key: %w", err))
		}

		fmt.Printf("Wrote private key to file %q\n", fileName)
	}

	var nodes []netmap.NodeInfo
	for _, k := range kk {
		var node netmap.NodeInfo
		node.SetPublicKey(k.PublicKey().Bytes())

		nodes = append(nodes, node)
	}

	var mockedNetmap netmap.NetMap
	mockedNetmap.SetNodes(nodes)

	var nmV2 netmapV2.NetMap
	mockedNetmap.WriteToV2(&nmV2)

	netmapFile := "netmap.json"

	f, err := os.Create(netmapFile)
	if err != nil {
		panic(fmt.Errorf("creating file for netmap: %w", err))
	}

	nmRaw, err := message.MarshalJSON(&nmV2)
	if err != nil {
		panic(fmt.Errorf("marshaling netmap template: %w", err))
	}

	_, err = f.Write(nmRaw)
	if err != nil {
		panic(fmt.Errorf("writing netmap: %w", err))
	}

	fmt.Printf("Wrote netmap to file %q, do not forget to fill addresses\n", netmapFile)
}
