package cache

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/otel/metric"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/engines"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/cache"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

type checkCacheEntry struct {
	result base.CheckResult
}

// CheckCacheStats contains the process-local permission cache statistics.
type CheckCacheStats struct {
	Hits          int64
	Misses        int64
	Invalidations int64
	HitRate       float64
}

// CheckEngineWithCache is a struct that holds an instance of a cache.Cache for managing engine cache.
type CheckEngineWithCache struct {
	// schemaReader is responsible for reading schema information
	schemaReader storage.SchemaReader
	checker      invoke.Check
	cache        cache.Cache
	invalidator  *Invalidator

	// Metrics
	cacheHitHistogram metric.Int64Histogram
	cacheHits         metric.Int64Counter
	cacheMisses       metric.Int64Counter
	cacheHitRatio     metric.Float64Gauge

	hits   atomic.Int64
	misses atomic.Int64
}

// CheckEngineOption configures the cached check engine.
type CheckEngineOption func(*CheckEngineWithCache)

// WithInvalidator shares a dependency-aware invalidator with the check engine.
func WithInvalidator(invalidator *Invalidator) CheckEngineOption {
	return func(engine *CheckEngineWithCache) {
		engine.invalidator = invalidator
	}
}

// NewCheckEngineWithCache creates a new instance of EngineKeyManager by initializing an EngineKeys
// struct with the provided cache.Cache instance.
func NewCheckEngineWithCache(
	checker invoke.Check,
	schemaReader storage.SchemaReader,
	cache cache.Cache,
	opts ...CheckEngineOption,
) invoke.Check {
	return newCheckEngineWithCache(checker, schemaReader, cache, opts...)
}

// NewCheckEngineWithCacheWithInvalidator creates a cached check engine and
// exposes its statistics and invalidator.
func NewCheckEngineWithCacheWithInvalidator(
	checker invoke.Check,
	schemaReader storage.SchemaReader,
	cache cache.Cache,
	opts ...CheckEngineOption,
) *CheckEngineWithCache {
	return newCheckEngineWithCache(checker, schemaReader, cache, opts...)
}

func newCheckEngineWithCache(
	checker invoke.Check,
	schemaReader storage.SchemaReader,
	cache cache.Cache,
	opts ...CheckEngineOption,
) *CheckEngineWithCache {
	engine := &CheckEngineWithCache{
		schemaReader:      schemaReader,
		checker:           checker,
		cache:             cache,
		cacheHitHistogram: telemetry.NewHistogram(internal.Meter, "cache_hit", "amount", "Number of cache hits"),
		cacheHits:         telemetry.NewCounter(internal.Meter, "cache_hits_total", "Number of permission cache hits"),
		cacheMisses:       telemetry.NewCounter(internal.Meter, "cache_misses_total", "Number of permission cache misses"),
		cacheHitRatio:     telemetry.NewFloat64Gauge(internal.Meter, "cache_hit_ratio", "Permission cache hit ratio"),
	}
	for _, opt := range opts {
		opt(engine)
	}
	if engine.invalidator == nil {
		engine.invalidator = NewInvalidator(cache)
	}
	return engine
}

// Invalidator returns the invalidator shared by this cache engine.
func (c *CheckEngineWithCache) Invalidator() *Invalidator {
	return c.invalidator
}

// Stats returns a consistent snapshot of cache hit-rate statistics.
func (c *CheckEngineWithCache) Stats() CheckCacheStats {
	hits := c.hits.Load()
	misses := c.misses.Load()
	stats := CheckCacheStats{
		Hits:          hits,
		Misses:        misses,
		Invalidations: c.invalidator.Invalidated(),
	}
	if hits+misses > 0 {
		stats.HitRate = float64(hits) / float64(hits+misses)
	}
	return stats
}

// Check performs a permission check for a given request, using the cached results if available.
func (c *CheckEngineWithCache) Check(ctx context.Context, request *base.PermissionCheckRequest) (response *base.PermissionCheckResponse, err error) {
	// Retrieve entity definition
	var en *base.EntityDefinition
	en, _, err = c.schemaReader.ReadEntityDefinition(ctx, request.GetTenantId(), request.GetEntity().GetType(), request.GetMetadata().GetSchemaVersion())
	if err != nil {
		return &base.PermissionCheckResponse{
			Can: base.CheckResult_CHECK_RESULT_DENIED,
			Metadata: &base.PermissionCheckResponseMetadata{
				CheckCount: 0,
			},
		}, err
	}

	isRelational := engines.IsRelational(en, request.GetPermission())

	recorder, ok := storage.DependencyRecorderFromContext(ctx)
	if !ok {
		recorder = storage.NewDependencyRecorder()
		ctx = storage.WithDependencyRecorder(ctx, recorder)
	}

	// Try to get the cached result for the given request.
	res, found := c.getCheckKey(request, isRelational)
	if found {
		c.recordHit(ctx)
		return &base.PermissionCheckResponse{
			Can:      res.GetCan(),
			Metadata: &base.PermissionCheckResponseMetadata{},
		}, nil
	}

	c.recordMiss(ctx)

	// Perform the actual permission check using the provided request.
	cres, err := c.checker.Check(ctx, request)
	if err != nil {
		return &base.PermissionCheckResponse{
			Can: base.CheckResult_CHECK_RESULT_DENIED,
			Metadata: &base.PermissionCheckResponseMetadata{
				CheckCount: 0,
			},
		}, err
	}

	c.setCheckKey(request, &base.PermissionCheckResponse{
		Can:      cres.GetCan(),
		Metadata: &base.PermissionCheckResponseMetadata{},
	}, isRelational, recorder.Dependencies()...)

	return cres, err
}

func (c *CheckEngineWithCache) recordHit(ctx context.Context) {
	hits := c.hits.Add(1)
	if c.cacheHitHistogram != nil {
		c.cacheHitHistogram.Record(ctx, 1)
	}
	if c.cacheHits != nil {
		c.cacheHits.Add(ctx, 1)
	}
	c.recordHitRatio(ctx, hits, c.misses.Load())
}

func (c *CheckEngineWithCache) recordMiss(ctx context.Context) {
	misses := c.misses.Add(1)
	if c.cacheMisses != nil {
		c.cacheMisses.Add(ctx, 1)
	}
	c.recordHitRatio(ctx, c.hits.Load(), misses)
}

func (c *CheckEngineWithCache) recordHitRatio(ctx context.Context, hits, misses int64) {
	if c.cacheHitRatio == nil || hits+misses == 0 {
		return
	}
	c.cacheHitRatio.Record(ctx, float64(hits)/float64(hits+misses))
}

// getCheckKey retrieves the value for the given key from the cache.
func (c *CheckEngineWithCache) getCheckKey(key *base.PermissionCheckRequest, isRelational bool) (*base.PermissionCheckResponse, bool) {
	if key == nil || c.cache == nil {
		return nil, false
	}

	resp, found := c.cache.Get(cacheKey(key, isRelational))
	if !found {
		return nil, false
	}

	switch value := resp.(type) {
	case checkCacheEntry:
		return &base.PermissionCheckResponse{
			Can: value.result,
			Metadata: &base.PermissionCheckResponseMetadata{
				CheckCount: 0,
			},
		}, true
	case *checkCacheEntry:
		if value == nil {
			return nil, false
		}
		return &base.PermissionCheckResponse{
			Can: value.result,
			Metadata: &base.PermissionCheckResponseMetadata{
				CheckCount: 0,
			},
		}, true
	case base.CheckResult:
		return &base.PermissionCheckResponse{
			Can: value,
			Metadata: &base.PermissionCheckResponseMetadata{
				CheckCount: 0,
			},
		}, true
	default:
		return nil, false
	}
}

// setCheckKey stores a check result together with the relationship scopes that
// contributed to it.
func (c *CheckEngineWithCache) setCheckKey(
	key *base.PermissionCheckRequest,
	value *base.PermissionCheckResponse,
	isRelational bool,
	dependencies ...storage.RelationshipDependency,
) bool {
	if key == nil || value == nil || c.cache == nil {
		return false
	}

	cacheKeyValue := cacheKey(key, isRelational)
	size := int64(len(cacheKeyValue))
	if !c.cache.Set(cacheKeyValue, checkCacheEntry{result: value.GetCan()}, size) {
		return false
	}
	if c.invalidator != nil {
		c.invalidator.Register(cacheKeyValue, dependencies)
	}
	return true
}

func cacheKey(key *base.PermissionCheckRequest, isRelational bool) string {
	value := engines.GenerateKey(key, isRelational)
	return strconv.FormatUint(xxhash.Sum64String(value), 16)
}
