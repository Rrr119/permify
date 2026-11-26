package snapshot

import (
	"github.com/hashicorp/go-memdb"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/internal/storage/memory/constants"
)

// State is an immutable view of relationship and attribute data at one
// committed snapshot.
type State struct {
	Tuples     []storage.RelationTuple
	Attributes []storage.Attribute
}

// Capture creates an immutable state from the current memdb contents.
func Capture(db *memdb.MemDB) *State {
	state := &State{}
	if db == nil {
		return state
	}

	txn := db.Txn(false)
	defer txn.Abort()

	if iterator, err := txn.Get(constants.RelationTuplesTable, "id"); err == nil {
		for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
			if tuple, ok := raw.(storage.RelationTuple); ok {
				state.Tuples = append(state.Tuples, tuple)
			}
		}
	}

	if iterator, err := txn.Get(constants.AttributesTable, "id"); err == nil {
		for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
			if attribute, ok := raw.(storage.Attribute); ok {
				state.Attributes = append(state.Attributes, attribute)
			}
		}
	}

	return state
}
