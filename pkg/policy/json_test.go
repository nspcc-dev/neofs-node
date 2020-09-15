package policy

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/stretchr/testify/require"
)

func TestToJSON(t *testing.T) {
	check := func(t *testing.T, p *netmap.PlacementPolicy, json string) {
		data, err := ToJSON(p)
		require.NoError(t, err)
		require.JSONEq(t, json, string(data))

		np, err := FromJSON(data)
		require.NoError(t, err)
		require.Equal(t, p, np)
	}
	t.Run("SimpleREP", func(t *testing.T) {
		p := new(netmap.PlacementPolicy)
		p.SetReplicas([]*netmap.Replica{newReplica("", 3)})
		check(t, p, `{"replicas":[{"count":3}]}`)
	})
	t.Run("REPWithCBF", func(t *testing.T) {
		p := new(netmap.PlacementPolicy)
		p.SetReplicas([]*netmap.Replica{newReplica("", 3)})
		p.SetContainerBackupFactor(3)
		check(t, p, `{"replicas":[{"count":3}],"container_backup_factor":3}`)
	})
	t.Run("REPFromSelector", func(t *testing.T) {
		p := new(netmap.PlacementPolicy)
		p.SetReplicas([]*netmap.Replica{newReplica("Nodes", 3)})
		p.SetContainerBackupFactor(3)
		p.SetSelectors([]*netmap.Selector{
			newSelector(1, netmap.Distinct, "City", "", "Nodes"),
		})
		check(t, p, `{
			"replicas":[{"count":3,"selector":"Nodes"}],
			"container_backup_factor":3,
			"selectors": [{
				"name":"Nodes",
				"attribute":"City",
				"clause":"distinct",
				"count":1
			}]}`)
	})
	t.Run("FilterOps", func(t *testing.T) {
		p := new(netmap.PlacementPolicy)
		p.SetReplicas([]*netmap.Replica{newReplica("Nodes", 3)})
		p.SetContainerBackupFactor(3)
		p.SetSelectors([]*netmap.Selector{
			newSelector(1, netmap.Same, "City", "Good", "Nodes"),
		})
		p.SetFilters([]*netmap.Filter{
			newFilter("GoodRating", "Rating", "5", netmap.GE),
			newFilter("Good", "", "", netmap.OR,
				newFilter("GoodRating", "", "", netmap.UnspecifiedOperation),
				newFilter("", "Attr1", "Val1", netmap.EQ),
				newFilter("", "Attr2", "Val2", netmap.NE),
				newFilter("", "", "", netmap.AND,
					newFilter("", "Attr4", "2", netmap.LT),
					newFilter("", "Attr5", "3", netmap.LE)),
				newFilter("", "Attr3", "1", netmap.GT)),
		})
		check(t, p, `{
			"replicas":[{"count":3,"selector":"Nodes"}],
			"container_backup_factor":3,
			"selectors": [{"name":"Nodes","attribute":"City","clause":"same","count":1,"filter":"Good"}],
			"filters": [
				{"name":"GoodRating","key":"Rating","op":"GE","value":"5"},
				{"name":"Good","op":"OR","filters":[
					{"name":"GoodRating"},
					{"key":"Attr1","op":"EQ","value":"Val1"},
					{"key":"Attr2","op":"NE","value":"Val2"},
					{"op":"AND","filters":[
						{"key":"Attr4","op":"LT","value":"2"},
						{"key":"Attr5","op":"LE","value":"3"}
					]},
					{"key":"Attr3","op":"GT","value":"1"}
				]}
			]}`)
	})
}
