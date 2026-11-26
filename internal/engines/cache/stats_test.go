package cache

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Permify/permify/pkg/cache/ristretto"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
)

var _ = Describe("Cache hit-rate statistics", func() {
	It("reports hits, misses and the hit ratio", func() {
		ristrettoCache, err := ristretto.New()
		Expect(err).ShouldNot(HaveOccurred())
		defer ristrettoCache.Close()

		checker := &countingChecker{}
		engine := NewCheckEngineWithCacheWithInvalidator(checker, &staticSchemaReader{}, ristrettoCache)
		request := &base.PermissionCheckRequest{
			TenantId: "tenant-1",
			Metadata: &base.PermissionCheckRequestMetadata{
				SchemaVersion: "v1",
				SnapToken:     "snap-1",
				Depth:         20,
			},
			Entity:     &base.Entity{Type: "document", Id: "doc-1"},
			Permission: "viewer",
			Subject:    &base.Subject{Type: "user", Id: "user-1"},
		}

		_, err = engine.Check(context.Background(), request)
		Expect(err).ShouldNot(HaveOccurred())
		ristrettoCache.Wait()

		_, err = engine.Check(context.Background(), request)
		Expect(err).ShouldNot(HaveOccurred())

		stats := engine.Stats()
		Expect(stats.Hits).Should(Equal(int64(1)))
		Expect(stats.Misses).Should(Equal(int64(1)))
		Expect(stats.HitRate).Should(Equal(0.5))
		Expect(checker.calls).Should(Equal(1))
	})
})

type countingChecker struct {
	calls int
}

func (c *countingChecker) Check(_ context.Context, _ *base.PermissionCheckRequest) (*base.PermissionCheckResponse, error) {
	c.calls++
	return &base.PermissionCheckResponse{
		Can:      base.CheckResult_CHECK_RESULT_ALLOWED,
		Metadata: &base.PermissionCheckResponseMetadata{},
	}, nil
}

type staticSchemaReader struct{}

func (s *staticSchemaReader) ReadSchema(_ context.Context, _, _ string) (*base.SchemaDefinition, error) {
	return &base.SchemaDefinition{}, nil
}

func (s *staticSchemaReader) ReadSchemaString(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

func (s *staticSchemaReader) ReadEntityDefinition(_ context.Context, _, _, _ string) (*base.EntityDefinition, string, error) {
	return &base.EntityDefinition{
		Name: "document",
		Relations: map[string]*base.RelationDefinition{
			"viewer": &base.RelationDefinition{Name: "viewer"},
		},
		References: map[string]base.EntityDefinition_Reference{
			"viewer": base.EntityDefinition_REFERENCE_RELATION,
		},
	}, "v1", nil
}

func (s *staticSchemaReader) ReadRuleDefinition(_ context.Context, _, _, _ string) (*base.RuleDefinition, string, error) {
	return &base.RuleDefinition{}, "", nil
}

func (s *staticSchemaReader) HeadVersion(_ context.Context, _ string) (string, error) {
	return "v1", nil
}

func (s *staticSchemaReader) ListSchemas(_ context.Context, _ string, _ database.Pagination) ([]*base.SchemaList, database.EncodedContinuousToken, error) {
	return nil, database.NewNoopContinuousToken().Encode(), nil
}
