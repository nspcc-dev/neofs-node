package subnetevents

import subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"

type idEvent struct {
	id subnetid.ID

	idErr error
}

func (x idEvent) ReadID(id *subnetid.ID) error {
	if x.idErr != nil {
		return x.idErr
	}

	*id = x.id

	return nil
}
