package cache

import (
	"context"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/bundle"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
)

// InvalidatingDataWriter evicts affected permission cache entries after a
// successful relationship write or delete.
type InvalidatingDataWriter struct {
	delegate    storage.DataWriter
	invalidator *Invalidator
}

// NewInvalidatingDataWriter wraps a data writer with precise cache
// invalidation.
func NewInvalidatingDataWriter(delegate storage.DataWriter, invalidator *Invalidator) *InvalidatingDataWriter {
	return &InvalidatingDataWriter{
		delegate:    delegate,
		invalidator: invalidator,
	}
}

// Write persists relationships and attributes, then invalidates checks that
// depend on the changed relationships.
func (w *InvalidatingDataWriter) Write(
	ctx context.Context,
	tenantID string,
	tupleCollection *database.TupleCollection,
	attributeCollection *database.AttributeCollection,
) (token.EncodedSnapToken, error) {
	snap, err := w.delegate.Write(ctx, tenantID, tupleCollection, attributeCollection)
	if err == nil && tupleCollection != nil {
		w.invalidator.InvalidateTuples(tenantID, tupleCollection.GetTuples()...)
	}
	return snap, err
}

// Delete removes data and invalidates checks whose relationship scopes overlap
// the delete filter.
func (w *InvalidatingDataWriter) Delete(
	ctx context.Context,
	tenantID string,
	tupleFilter *base.TupleFilter,
	attributeFilter *base.AttributeFilter,
) (token.EncodedSnapToken, error) {
	snap, err := w.delegate.Delete(ctx, tenantID, tupleFilter, attributeFilter)
	if err == nil && tupleFilter != nil && !isTupleFilterEmpty(tupleFilter) {
		w.invalidator.InvalidateFilter(tenantID, tupleFilter)
	}
	return snap, err
}

// RunBundle executes a bundle and invalidates every relationship touched by its
// write and delete operations.
func (w *InvalidatingDataWriter) RunBundle(
	ctx context.Context,
	tenantID string,
	arguments map[string]string,
	dataBundle *base.DataBundle,
) (token.EncodedSnapToken, error) {
	snap, err := w.delegate.RunBundle(ctx, tenantID, arguments, dataBundle)
	if err != nil {
		return snap, err
	}

	for _, operation := range dataBundle.GetOperations() {
		tupleBundle, _, operationErr := bundle.Operation(arguments, operation)
		if operationErr != nil {
			w.invalidator.InvalidateFilter(tenantID, &base.TupleFilter{})
			continue
		}
		w.invalidator.InvalidateTuples(tenantID, tupleBundle.Write.GetTuples()...)
		w.invalidator.InvalidateTuples(tenantID, tupleBundle.Delete.GetTuples()...)
	}

	return snap, nil
}

func isTupleFilterEmpty(filter *base.TupleFilter) bool {
	return filter.GetEntity().GetType() == "" &&
		len(filter.GetEntity().GetIds()) == 0 &&
		filter.GetRelation() == "" &&
		filter.GetSubject().GetType() == "" &&
		len(filter.GetSubject().GetIds()) == 0 &&
		filter.GetSubject().GetRelation() == ""
}
