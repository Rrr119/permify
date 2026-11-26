package storage

import (
	"context"
	"sync"

	base "github.com/Permify/permify/pkg/pb/base/v1"
)

// RelationshipDependency describes a relationship query that contributed to a
// cached permission check. The filter is retained so the cache can decide
// whether a later relationship change can affect the cached result.
type RelationshipDependency struct {
	TenantID string
	Filter   *base.TupleFilter
}

type dependencyRecorderContextKey struct{}

// DependencyRecorder collects relationship query scopes for a permission
// check. A single recorder is shared by nested checks so the top-level cache
// entry knows every relationship read that contributed to its result.
type DependencyRecorder struct {
	mu           sync.Mutex
	dependencies map[string]RelationshipDependency
}

// NewDependencyRecorder creates an empty dependency recorder.
func NewDependencyRecorder() *DependencyRecorder {
	return &DependencyRecorder{
		dependencies: make(map[string]RelationshipDependency),
	}
}

// Record stores a relationship query scope. Duplicate scopes are coalesced.
func (r *DependencyRecorder) Record(tenantID string, filter *base.TupleFilter) {
	if r == nil || filter == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	cloned := filter.CloneVT()
	r.dependencies[tenantID+"\x00"+cloned.String()] = RelationshipDependency{
		TenantID: tenantID,
		Filter:   cloned,
	}
}

// Dependencies returns a stable copy of all recorded relationship scopes.
func (r *DependencyRecorder) Dependencies() []RelationshipDependency {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	dependencies := make([]RelationshipDependency, 0, len(r.dependencies))
	for _, dependency := range r.dependencies {
		dependencies = append(dependencies, RelationshipDependency{
			TenantID: dependency.TenantID,
			Filter:   dependency.Filter.CloneVT(),
		})
	}
	return dependencies
}

// WithDependencyRecorder attaches a recorder to the context.
func WithDependencyRecorder(ctx context.Context, recorder *DependencyRecorder) context.Context {
	return context.WithValue(ctx, dependencyRecorderContextKey{}, recorder)
}

// DependencyRecorderFromContext returns the recorder attached to the context,
// if any.
func DependencyRecorderFromContext(ctx context.Context) (*DependencyRecorder, bool) {
	recorder, ok := ctx.Value(dependencyRecorderContextKey{}).(*DependencyRecorder)
	return recorder, ok && recorder != nil
}

// RecordRelationshipDependency records a relationship query scope when a
// dependency recorder is present in the context.
func RecordRelationshipDependency(ctx context.Context, tenantID string, filter *base.TupleFilter) {
	if recorder, ok := DependencyRecorderFromContext(ctx); ok {
		recorder.Record(tenantID, filter)
	}
}
