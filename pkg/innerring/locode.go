package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	irlocode "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/locode"
	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	locodebolt "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/boltdb"
	"github.com/spf13/viper"
)

func (s *Server) newLocodeValidator(cfg *viper.Viper) (netmap.NodeValidator, error) {
	locodeDB := locodebolt.New(locodebolt.Prm{
		Path: cfg.GetString("locode.db.path"),
	},
		locodebolt.ReadOnly(),
	)

	s.registerStarter(locodeDB.Open)
	s.registerIOCloser(locodeDB)

	return irlocode.New(irlocode.Prm{
		DB: (*locodeBoltDBWrapper)(locodeDB),
	}), nil
}

type locodeBoltEntryWrapper struct {
	*locodedb.Key
	*locodedb.Record
}

func (l *locodeBoltEntryWrapper) LocationName() string {
	return l.Record.CityName()
}

type locodeBoltDBWrapper locodebolt.DB

func (l *locodeBoltDBWrapper) Get(lc *locode.LOCODE) (irlocode.Record, error) {
	key, err := locodedb.NewKey(*lc)
	if err != nil {
		return nil, err
	}

	rec, err := (*locodebolt.DB)(l).Get(*key)
	if err != nil {
		return nil, err
	}

	return &locodeBoltEntryWrapper{
		Key:    key,
		Record: rec,
	}, nil
}
