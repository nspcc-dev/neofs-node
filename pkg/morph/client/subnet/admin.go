package morphsubnet

import "github.com/nspcc-dev/neofs-node/pkg/morph/client"

// ManageAdminsPrm groups parameters of administer methods of Subnet contract.
//
// Zero value adds node admin. Subnet, key and group must be specified via setters.
type ManageAdminsPrm struct {
	// remove or add admin
	rm bool

	// client or node admin
	client bool

	subnet []byte

	admin []byte

	group []byte
}

// SetRemove marks admin to be removed. By default, admin is added.
func (x *ManageAdminsPrm) SetRemove() {
	x.rm = true
}

// SetClient switches to client admin. By default, node admin is modified.
func (x *ManageAdminsPrm) SetClient() {
	x.client = true
}

// SetSubnet sets identifier of the subnet in a binary NeoFS API protocol format.
func (x *ManageAdminsPrm) SetSubnet(id []byte) {
	x.subnet = id
}

// SetAdmin sets admin's public key in a binary format.
func (x *ManageAdminsPrm) SetAdmin(key []byte) {
	x.admin = key
}

// SetGroup sets identifier of the client group in a binary NeoFS API protocol format.
// Makes sense only for client admins (see ManageAdminsPrm.SetClient).
func (x *ManageAdminsPrm) SetGroup(id []byte) {
	x.group = id
}

// ManageAdminsRes groups resulting values of node administer methods of Subnet contract.
type ManageAdminsRes struct{}

// ManageAdmins manages admin list of the NeoFS subnet through Subnet contract calls.
func (x Client) ManageAdmins(prm ManageAdminsPrm) (*ManageAdminsPrm, error) {
	var method string

	args := make([]interface{}, 1, 3)
	args[0] = prm.subnet

	if prm.client {
		args = append(args, prm.group, prm.admin)

		if prm.rm {
			method = "removeClientAdmin"
		} else {
			method = "addClientAdmin"
		}
	} else {
		args = append(args, prm.admin)

		if prm.rm {
			method = "removeNodeAdmin"
		} else {
			method = "addNodeAdmin"
		}
	}

	var prmInvoke client.InvokePrm

	prmInvoke.SetMethod(method)
	prmInvoke.SetArgs(args...)

	err := x.client.Invoke(prmInvoke)
	if err != nil {
		return nil, err
	}

	return new(ManageAdminsPrm), nil
}
