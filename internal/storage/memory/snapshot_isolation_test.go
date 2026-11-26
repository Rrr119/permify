package memory

import (
	"context"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Permify/permify/internal/storage/memory/migrations"
	"github.com/Permify/permify/pkg/database"
	db "github.com/Permify/permify/pkg/database/memory"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/tuple"
)

var _ = Describe("Snapshot isolation", func() {
	var memoryDB *db.Memory
	var writer *DataWriter
	var reader *DataReader

	BeforeEach(func() {
		var err error
		memoryDB, err = db.New(migrations.Schema)
		Expect(err).ShouldNot(HaveOccurred())
		writer = NewDataWriter(memoryDB)
		reader = NewDataReader(memoryDB)
	})

	AfterEach(func() {
		Expect(memoryDB.Close()).ShouldNot(HaveOccurred())
	})

	It("reads relationships from the requested historical snapshot", func() {
		ctx := context.Background()

		first := mustTuple("document:doc-1#viewer@user:user-1")
		firstSnap, err := writer.Write(ctx, "tenant-1", database.NewTupleCollection(first), database.NewAttributeCollection())
		Expect(err).ShouldNot(HaveOccurred())

		second := mustTuple("document:doc-1#editor@user:user-2")
		secondSnap, err := writer.Write(ctx, "tenant-1", database.NewTupleCollection(second), database.NewAttributeCollection())
		Expect(err).ShouldNot(HaveOccurred())

		old, _, err := reader.ReadRelationships(ctx, "tenant-1", &base.TupleFilter{
			Entity: &base.EntityFilter{Type: "document", Ids: []string{"doc-1"}},
		}, firstSnap.String(), database.NewPagination())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(old.GetTuples()).Should(ConsistOf(first))

		current, _, err := reader.ReadRelationships(ctx, "tenant-1", &base.TupleFilter{
			Entity: &base.EntityFilter{Type: "document", Ids: []string{"doc-1"}},
		}, secondSnap.String(), database.NewPagination())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(current.GetTuples()).Should(ConsistOf(first, second))

		deleteSnap, err := writer.Delete(ctx, "tenant-1", &base.TupleFilter{
			Entity:   &base.EntityFilter{Type: "document", Ids: []string{"doc-1"}},
			Relation: "viewer",
		}, &base.AttributeFilter{})
		Expect(err).ShouldNot(HaveOccurred())

		historical, _, err := reader.ReadRelationships(ctx, "tenant-1", &base.TupleFilter{
			Entity: &base.EntityFilter{Type: "document", Ids: []string{"doc-1"}},
		}, secondSnap.String(), database.NewPagination())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(historical.GetTuples()).Should(ConsistOf(first, second))

		afterDelete, _, err := reader.ReadRelationships(ctx, "tenant-1", &base.TupleFilter{
			Entity: &base.EntityFilter{Type: "document", Ids: []string{"doc-1"}},
		}, deleteSnap.String(), database.NewPagination())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(afterDelete.GetTuples()).Should(ConsistOf(second))
	})

	It("never exposes a half-committed multi-relationship write", func() {
		ctx := context.Background()

		initial, err := writer.Write(ctx, "tenant-1", database.NewTupleCollection(), database.NewAttributeCollection())
		Expect(err).ShouldNot(HaveOccurred())

		for i := 0; i < 50; i++ {
			entityID := fmt.Sprintf("doc-%d", i)
			first := mustTuple(fmt.Sprintf("document:%s#viewer@user:user-1", entityID))
			second := mustTuple(fmt.Sprintf("document:%s#editor@user:user-1", entityID))

			var writeErr error
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, writeErr = writer.Write(ctx, "tenant-1", database.NewTupleCollection(first, second), database.NewAttributeCollection())
			}()

			for {
				snap, snapshotErr := reader.HeadSnapshot(ctx, "tenant-1")
				Expect(snapshotErr).ShouldNot(HaveOccurred())

				collection, _, readErr := reader.ReadRelationships(ctx, "tenant-1", &base.TupleFilter{
					Entity: &base.EntityFilter{Type: "document", Ids: []string{entityID}},
				}, snap.Encode().String(), database.NewPagination())
				Expect(readErr).ShouldNot(HaveOccurred())

				count := len(collection.GetTuples())
				Expect(count).Should(BeElementOf(0, 2))
				if count == 2 {
					break
				}
			}
			wg.Wait()
			Expect(writeErr).ShouldNot(HaveOccurred())
		}

		Expect(initial).ShouldNot(BeNil())
	})
})

func mustTuple(value string) *base.Tuple {
	tup, err := tuple.Tuple(value)
	Expect(err).ShouldNot(HaveOccurred())
	return tup
}
