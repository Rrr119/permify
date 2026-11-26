package cache

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/cache/ristretto"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
)

var _ = Describe("Relationship cache invalidation", func() {
	var ristrettoCache *ristretto.Ristretto
	var invalidator *Invalidator

	BeforeEach(func() {
		var err error
		ristrettoCache, err = ristretto.New()
		Expect(err).ShouldNot(HaveOccurred())
		invalidator = NewInvalidator(ristrettoCache)
	})

	AfterEach(func() {
		ristrettoCache.Close()
	})

	It("evicts only checks that depend on the changed relationship", func() {
		viewerKey := "viewer-check"
		editorKey := "editor-check"
		otherTenantKey := "other-tenant-check"

		Expect(ristrettoCache.Set(viewerKey, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_ALLOWED}, 1)).Should(BeTrue())
		Expect(ristrettoCache.Set(editorKey, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_DENIED}, 1)).Should(BeTrue())
		Expect(ristrettoCache.Set(otherTenantKey, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_ALLOWED}, 1)).Should(BeTrue())
		ristrettoCache.Wait()

		invalidator.Register(viewerKey, []storage.RelationshipDependency{
			{TenantID: "tenant-1", Filter: relationshipFilter("document", "doc-1", "viewer")},
		})
		invalidator.Register(editorKey, []storage.RelationshipDependency{
			{TenantID: "tenant-1", Filter: relationshipFilter("document", "doc-1", "editor")},
		})
		invalidator.Register(otherTenantKey, []storage.RelationshipDependency{
			{TenantID: "tenant-2", Filter: relationshipFilter("document", "doc-1", "viewer")},
		})

		changed := &base.Tuple{
			Entity:   &base.Entity{Type: "document", Id: "doc-1"},
			Relation: "viewer",
			Subject:  &base.Subject{Type: "user", Id: "user-1"},
		}
		Expect(invalidator.InvalidateTuples("tenant-1", changed)).Should(Equal(1))

		_, found := ristrettoCache.Get(viewerKey)
		Expect(found).Should(BeFalse())
		_, found = ristrettoCache.Get(editorKey)
		Expect(found).Should(BeTrue())
		_, found = ristrettoCache.Get(otherTenantKey)
		Expect(found).Should(BeTrue())
	})

	It("invalidates negative cache entries when a matching relationship appears", func() {
		key := "denied-check"
		Expect(ristrettoCache.Set(key, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_DENIED}, 1)).Should(BeTrue())
		ristrettoCache.Wait()

		invalidator.Register(key, []storage.RelationshipDependency{
			{TenantID: "tenant-1", Filter: relationshipFilter("document", "doc-1", "viewer")},
		})

		Expect(invalidator.InvalidateTuples("tenant-1", &base.Tuple{
			Entity:   &base.Entity{Type: "document", Id: "doc-1"},
			Relation: "viewer",
			Subject:  &base.Subject{Type: "user", Id: "user-1"},
		})).Should(Equal(1))

		_, found := ristrettoCache.Get(key)
		Expect(found).Should(BeFalse())
	})

	It("invalidates by a delete filter without flushing unrelated checks", func() {
		docKey := "doc-check"
		otherKey := "other-doc-check"
		Expect(ristrettoCache.Set(docKey, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_ALLOWED}, 1)).Should(BeTrue())
		Expect(ristrettoCache.Set(otherKey, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_ALLOWED}, 1)).Should(BeTrue())
		ristrettoCache.Wait()

		invalidator.Register(docKey, []storage.RelationshipDependency{
			{TenantID: "tenant-1", Filter: relationshipFilter("document", "doc-1", "viewer")},
		})
		invalidator.Register(otherKey, []storage.RelationshipDependency{
			{TenantID: "tenant-1", Filter: relationshipFilter("document", "doc-2", "viewer")},
		})

		Expect(invalidator.InvalidateFilter("tenant-1", &base.TupleFilter{
			Entity: &base.EntityFilter{Type: "document", Ids: []string{"doc-1"}},
		})).Should(Equal(1))

		_, found := ristrettoCache.Get(docKey)
		Expect(found).Should(BeFalse())
		_, found = ristrettoCache.Get(otherKey)
		Expect(found).Should(BeTrue())
	})
})

var _ = Describe("Invalidating data writer", func() {
	It("invalidates affected checks after a successful relationship write", func() {
		ristrettoCache, err := ristretto.New()
		Expect(err).ShouldNot(HaveOccurred())
		defer ristrettoCache.Close()

		invalidator := NewInvalidator(ristrettoCache)
		key := "denied-check"
		Expect(ristrettoCache.Set(key, checkCacheEntry{result: base.CheckResult_CHECK_RESULT_DENIED}, 1)).Should(BeTrue())
		ristrettoCache.Wait()
		invalidator.Register(key, []storage.RelationshipDependency{
			{TenantID: "tenant-1", Filter: relationshipFilter("document", "doc-1", "viewer")},
		})

		writer := NewInvalidatingDataWriter(&stubDataWriter{}, invalidator)
		tup := &base.Tuple{
			Entity:   &base.Entity{Type: "document", Id: "doc-1"},
			Relation: "viewer",
			Subject:  &base.Subject{Type: "user", Id: "user-1"},
		}
		_, err = writer.Write(context.Background(), "tenant-1", database.NewTupleCollection(tup), database.NewAttributeCollection())
		Expect(err).ShouldNot(HaveOccurred())

		_, found := ristrettoCache.Get(key)
		Expect(found).Should(BeFalse())
	})
})

func relationshipFilter(entityType, entityID, relation string) *base.TupleFilter {
	return &base.TupleFilter{
		Entity:   &base.EntityFilter{Type: entityType, Ids: []string{entityID}},
		Relation: relation,
	}
}

type stubDataWriter struct{}

func (s *stubDataWriter) Write(_ context.Context, _ string, _ *database.TupleCollection, _ *database.AttributeCollection) (token.EncodedSnapToken, error) {
	return token.NewNoopToken().Encode(), nil
}

func (s *stubDataWriter) Delete(_ context.Context, _ string, _ *base.TupleFilter, _ *base.AttributeFilter) (token.EncodedSnapToken, error) {
	return token.NewNoopToken().Encode(), nil
}

func (s *stubDataWriter) RunBundle(_ context.Context, _ string, _ map[string]string, _ *base.DataBundle) (token.EncodedSnapToken, error) {
	return token.NewNoopToken().Encode(), nil
}
