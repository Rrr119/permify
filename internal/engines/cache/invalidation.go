package cache

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/cache"
	base "github.com/Permify/permify/pkg/pb/base/v1"
)

const (
	scopeSeparator = "\x00"
	scopeWildcard  = "*"
)

type invalidatorEntry struct {
	scopes  []string
	filters []*base.TupleFilter
}

// Invalidator keeps a reverse index from relationship query scopes to cache
// keys. It allows a write to evict only the checks that could have observed the
// changed relationships instead of flushing the whole permission cache.
type Invalidator struct {
	mu          sync.Mutex
	cache       cache.Cache
	entries     map[string]*invalidatorEntry
	byScope     map[string]map[string]struct{}
	invalidated atomic.Int64
}

type invalidatedCacheValue struct{}

// NewInvalidator creates a dependency-aware invalidator for the given cache.
func NewInvalidator(c cache.Cache) *Invalidator {
	return &Invalidator{
		cache:   c,
		entries: make(map[string]*invalidatorEntry),
		byScope: make(map[string]map[string]struct{}),
	}
}

// Register associates a cache key with the relationship scopes read while
// producing it. Registering the same key again replaces its previous scopes.
func (i *Invalidator) Register(key string, dependencies []storage.RelationshipDependency) {
	if i == nil || key == "" {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	i.removeLocked(key)
	if len(dependencies) == 0 {
		return
	}

	entry := &invalidatorEntry{
		filters: make([]*base.TupleFilter, 0, len(dependencies)),
	}

	for _, dependency := range dependencies {
		if dependency.Filter == nil {
			continue
		}
		entry.filters = append(entry.filters, dependency.Filter.CloneVT())
		for _, scope := range relationshipScopes(dependency.TenantID, dependency.Filter) {
			if _, ok := i.byScope[scope]; !ok {
				i.byScope[scope] = make(map[string]struct{})
			}
			i.byScope[scope][key] = struct{}{}
			entry.scopes = append(entry.scopes, scope)
		}
	}

	if len(entry.scopes) == 0 {
		return
	}
	entry.scopes = compactStrings(entry.scopes)
	i.entries[key] = entry
}

// InvalidateTuples evicts cache entries whose relationship queries could have
// observed any of the changed tuples.
func (i *Invalidator) InvalidateTuples(tenantID string, tuples ...*base.Tuple) int {
	if i == nil || len(tuples) == 0 {
		return 0
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	keys := make(map[string]struct{})
	for _, tuple := range tuples {
		if tuple == nil {
			continue
		}
		entity := tuple.GetEntity()
		for _, scope := range relationshipProbeScopes(tenantID, entity.GetType(), entity.GetId(), tuple.GetRelation()) {
			for key := range i.byScope[scope] {
				entry := i.entries[key]
				if entry == nil {
					continue
				}
				if anyFilterMatches(entry.filters, tuple) {
					keys[key] = struct{}{}
				}
			}
		}
	}

	return i.invalidateKeysLocked(keys)
}

// InvalidateFilter evicts cache entries that may have observed rows removed by
// a relationship delete filter. Delete requests can affect multiple tuples, so
// this operation compares the delete filter with each recorded query scope.
func (i *Invalidator) InvalidateFilter(tenantID string, filter *base.TupleFilter) int {
	if i == nil {
		return 0
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	keys := make(map[string]struct{})
	prefix := tenantID + scopeSeparator
	for scope, scopedKeys := range i.byScope {
		if !strings.HasPrefix(scope, prefix) {
			continue
		}
		for key := range scopedKeys {
			entry := i.entries[key]
			if entry == nil {
				continue
			}
			if anyFilterOverlaps(entry.filters, filter) {
				keys[key] = struct{}{}
			}
		}
	}

	return i.invalidateKeysLocked(keys)
}

// Invalidated returns the number of relationship changes that caused cache
// evictions.
func (i *Invalidator) Invalidated() int64 {
	if i == nil {
		return 0
	}
	return i.invalidated.Load()
}

func (i *Invalidator) invalidateKeysLocked(keys map[string]struct{}) int {
	for key := range keys {
		i.removeLocked(key)
		if evicting, ok := i.cache.(interface{ Delete(string) bool }); ok {
			evicting.Delete(key)
		} else if i.cache != nil {
			i.cache.Set(key, invalidatedCacheValue{}, 1)
		}
	}

	if len(keys) > 0 {
		i.invalidated.Add(int64(len(keys)))
	}
	return len(keys)
}

func (i *Invalidator) removeLocked(key string) {
	entry, ok := i.entries[key]
	if !ok {
		return
	}
	for _, scope := range entry.scopes {
		delete(i.byScope[scope], key)
		if len(i.byScope[scope]) == 0 {
			delete(i.byScope, scope)
		}
	}
	delete(i.entries, key)
}

func relationshipScopes(tenantID string, filter *base.TupleFilter) []string {
	if filter == nil {
		return nil
	}

	entityType := filter.GetEntity().GetType()
	relation := filter.GetRelation()
	if entityType == "" {
		entityType = scopeWildcard
	}
	if relation == "" {
		relation = scopeWildcard
	}
	ids := filter.GetEntity().GetIds()
	if len(ids) == 0 {
		return []string{scopeKey(tenantID, entityType, scopeWildcard, relation)}
	}

	scopes := make([]string, 0, len(ids))
	for _, id := range ids {
		scopes = append(scopes, scopeKey(tenantID, entityType, id, relation))
	}
	return scopes
}

func relationshipProbeScopes(tenantID, entityType, entityID, relation string) []string {
	types := []string{entityType, scopeWildcard}
	ids := []string{entityID, scopeWildcard}
	relations := []string{relation, scopeWildcard}

	scopes := make([]string, 0, len(types)*len(ids)*len(relations))
	for _, typ := range types {
		for _, id := range ids {
			for _, rel := range relations {
				scopes = append(scopes, scopeKey(tenantID, typ, id, rel))
			}
		}
	}
	return scopes
}

func scopeKey(tenantID, entityType, entityID, relation string) string {
	return tenantID + scopeSeparator + entityType + scopeSeparator + entityID + scopeSeparator + relation
}

func anyFilterMatches(filters []*base.TupleFilter, tuple *base.Tuple) bool {
	for _, filter := range filters {
		if filterMatchesTuple(filter, tuple) {
			return true
		}
	}
	return false
}

func filterMatchesTuple(filter *base.TupleFilter, tuple *base.Tuple) bool {
	if filter == nil || tuple == nil {
		return false
	}

	if entityType := filter.GetEntity().GetType(); entityType != "" && entityType != tuple.GetEntity().GetType() {
		return false
	}
	if ids := filter.GetEntity().GetIds(); len(ids) > 0 && !slices.Contains(ids, tuple.GetEntity().GetId()) {
		return false
	}
	if relation := filter.GetRelation(); relation != "" && relation != tuple.GetRelation() {
		return false
	}
	if subjectType := filter.GetSubject().GetType(); subjectType != "" && subjectType != tuple.GetSubject().GetType() {
		return false
	}
	if ids := filter.GetSubject().GetIds(); len(ids) > 0 && !slices.Contains(ids, tuple.GetSubject().GetId()) {
		return false
	}
	if subjectRelation := filter.GetSubject().GetRelation(); subjectRelation != "" && subjectRelation != tuple.GetSubject().GetRelation() {
		return false
	}

	return true
}

func anyFilterOverlaps(filters []*base.TupleFilter, other *base.TupleFilter) bool {
	for _, filter := range filters {
		if filtersOverlap(filter, other) {
			return true
		}
	}
	return false
}

func filtersOverlap(a, b *base.TupleFilter) bool {
	if a == nil || b == nil {
		return false
	}

	if !valuesOverlap(a.GetEntity().GetType(), b.GetEntity().GetType()) {
		return false
	}
	if !idsOverlap(a.GetEntity().GetIds(), b.GetEntity().GetIds()) {
		return false
	}
	if !valuesOverlap(a.GetRelation(), b.GetRelation()) {
		return false
	}
	if !valuesOverlap(a.GetSubject().GetType(), b.GetSubject().GetType()) {
		return false
	}
	if !idsOverlap(a.GetSubject().GetIds(), b.GetSubject().GetIds()) {
		return false
	}
	return valuesOverlap(a.GetSubject().GetRelation(), b.GetSubject().GetRelation())
}

func valuesOverlap(a, b string) bool {
	return a == "" || b == "" || a == b
}

func idsOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, left := range a {
		if slices.Contains(b, left) {
			return true
		}
	}
	return false
}

func compactStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
