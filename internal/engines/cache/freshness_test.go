package cache

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Permify/permify/internal/config"
	"github.com/Permify/permify/internal/engines"
	"github.com/Permify/permify/internal/factories"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/pkg/cache/ristretto"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/tuple"
)

var _ = Describe("Check API cache freshness", func() {
	It("returns the new result immediately after a relationship change", func() {
		db, err := factories.DatabaseFactory(config.Database{Engine: "memory"})
		Expect(err).ShouldNot(HaveOccurred())

		definitions, err := newSchema(`
			entity user {}
			entity document {
				relation viewer @user
				permission view = viewer
			}
		`)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(factories.SchemaWriterFactory(db).WriteSchema(context.Background(), definitions)).Should(Succeed())

		schemaReader := factories.SchemaReaderFactory(db)
		dataReader := factories.DataReaderFactory(db)
		ristrettoCache, err := ristretto.New()
		Expect(err).ShouldNot(HaveOccurred())
		defer ristrettoCache.Close()

		invalidator := NewInvalidator(ristrettoCache)
		dataWriter := NewInvalidatingDataWriter(factories.DataWriterFactory(db), invalidator)
		checkEngine := engines.NewCheckEngine(schemaReader, dataReader)
		checkWithCache := NewCheckEngineWithCacheWithInvalidator(checkEngine, schemaReader, ristrettoCache, WithInvalidator(invalidator))
		invoker := invoke.NewDirectInvoker(schemaReader, dataReader, checkWithCache, nil, nil, nil)
		checkEngine.SetInvoker(invoker)

		request := func() *base.PermissionCheckRequest {
			return &base.PermissionCheckRequest{
				TenantId:   "t1",
				Entity:     &base.Entity{Type: "document", Id: "doc-1"},
				Permission: "view",
				Subject:    &base.Subject{Type: "user", Id: "user-1"},
				Metadata:   &base.PermissionCheckRequestMetadata{Depth: 20},
			}
		}

		before, err := invoker.Check(context.Background(), request())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(before.GetCan()).Should(Equal(base.CheckResult_CHECK_RESULT_DENIED))
		ristrettoCache.Wait()

		// Warm the denied result and verify the cache is actually used.
		_, err = invoker.Check(context.Background(), request())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(checkWithCache.Stats().Hits).Should(Equal(int64(1)))

		viewer, err := tuple.Tuple("document:doc-1#viewer@user:user-1")
		Expect(err).ShouldNot(HaveOccurred())
		start := time.Now()
		_, err = dataWriter.Write(context.Background(), "t1", database.NewTupleCollection(viewer), database.NewAttributeCollection())
		Expect(err).ShouldNot(HaveOccurred())

		after, err := invoker.Check(context.Background(), request())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(after.GetCan()).Should(Equal(base.CheckResult_CHECK_RESULT_ALLOWED))
		Expect(time.Since(start)).Should(BeNumerically("<", time.Second))
		Expect(invalidator.Invalidated()).Should(BeNumerically(">", 0))
	})
})
