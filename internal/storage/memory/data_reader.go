package memory

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strconv"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/internal/storage/memory/snapshot"
	"github.com/Permify/permify/internal/storage/memory/utils"
	"github.com/Permify/permify/pkg/database"
	db "github.com/Permify/permify/pkg/database/memory"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
)

// DataReader -
type DataReader struct {
	database *db.Memory
}

// NewDataReader - Creates a new DataReader
func NewDataReader(database *db.Memory) *DataReader {
	return &DataReader{
		database: database,
	}
}

// QueryRelationships queries a consistent snapshot for relationships matching
// the provided filter.
func (r *DataReader) QueryRelationships(_ context.Context, tenantID string, filter *base.TupleFilter, snap string, pagination database.CursorPagination) (it *database.TupleIterator, err error) {
	state, err := r.snapshotState(snap)
	if err != nil {
		return nil, err
	}

	var lowerBound string
	if pagination.Cursor() != "" {
		var t database.ContinuousToken
		t, err = utils.EncodedContinuousToken{Value: pagination.Cursor()}.Decode()
		if err != nil {
			return nil, err
		}
		lowerBound = t.(utils.ContinuousToken).Value
	}

	tuples := filterRelationshipState(state, tenantID, filter)

	sort.Slice(tuples, func(i, j int) bool {
		switch pagination.Sort() {
		case "entity_id":
			return tuples[i].EntityID < tuples[j].EntityID
		case "subject_id":
			return tuples[i].SubjectID < tuples[j].SubjectID
		default:
			for _, comparison := range []int{
				stringsCompare(tuples[i].EntityID, tuples[j].EntityID),
				stringsCompare(tuples[i].Relation, tuples[j].Relation),
				stringsCompare(tuples[i].SubjectType, tuples[j].SubjectType),
				stringsCompare(tuples[i].SubjectID, tuples[j].SubjectID),
				stringsCompare(tuples[i].SubjectRelation, tuples[j].SubjectRelation),
			} {
				if comparison != 0 {
					return comparison < 0
				}
			}
			return tuples[i].ID < tuples[j].ID
		}
	})

	var result []*base.Tuple
	count := uint32(0)
	limit := pagination.Limit()

	for _, t := range tuples {
		switch pagination.Sort() {
		case "entity_id":
			if t.EntityID < lowerBound {
				continue
			}
		case "subject_id":
			if t.SubjectID < lowerBound {
				continue
			}
		}

		result = append(result, t.ToTuple())
		count++
		if limit > 0 && count >= limit {
			break
		}
	}

	return database.NewTupleCollection(result...).CreateTupleIterator(), nil
}

// ReadRelationships reads relationships from one immutable snapshot taking
// into account the pagination.
func (r *DataReader) ReadRelationships(_ context.Context, tenantID string, filter *base.TupleFilter, snap string, pagination database.Pagination) (collection *database.TupleCollection, ct database.EncodedContinuousToken, err error) {
	state, err := r.snapshotState(snap)
	if err != nil {
		return nil, database.NewNoopContinuousToken().Encode(), err
	}

	var lowerBound uint64
	if pagination.Token() != "" {
		var t database.ContinuousToken
		t, err = utils.EncodedContinuousToken{Value: pagination.Token()}.Decode()
		if err != nil {
			return nil, database.NewNoopContinuousToken().Encode(), err
		}
		lowerBound, err = strconv.ParseUint(t.(utils.ContinuousToken).Value, 10, 64)
		if err != nil {
			return nil, database.NewNoopContinuousToken().Encode(), errors.New(base.ErrorCode_ERROR_CODE_INVALID_CONTINUOUS_TOKEN.String())
		}
	}

	tuples := filterRelationshipState(state, tenantID, filter)
	sort.Slice(tuples, func(i, j int) bool {
		return tuples[i].ID < tuples[j].ID
	})

	result := make([]*base.Tuple, 0, pagination.PageSize()+1)
	for _, t := range tuples {
		if t.ID >= lowerBound {
			result = append(result, t.ToTuple())
			if pagination.PageSize() != 0 && len(result) > int(pagination.PageSize()) {
				return database.NewTupleCollection(result[:pagination.PageSize()]...), utils.NewContinuousToken(strconv.FormatUint(t.ID, 10)).Encode(), nil
			}
		}
	}

	return database.NewTupleCollection(result...), database.NewNoopContinuousToken().Encode(), nil
}

// QuerySingleAttribute queries a consistent snapshot for a single attribute.
func (r *DataReader) QuerySingleAttribute(_ context.Context, tenantID string, filter *base.AttributeFilter, snap string) (attribute *base.Attribute, err error) {
	state, err := r.snapshotState(snap)
	if err != nil {
		return nil, err
	}

	attributes := filterAttributeState(state, tenantID, filter)
	sort.Slice(attributes, func(i, j int) bool {
		return attributes[i].ID < attributes[j].ID
	})
	if len(attributes) > 0 {
		return attributes[0].ToAttribute(), nil
	}
	return nil, nil
}

// QueryAttributes queries a consistent snapshot for attributes matching the
// provided filter.
func (r *DataReader) QueryAttributes(_ context.Context, tenantID string, filter *base.AttributeFilter, snap string, pagination database.CursorPagination) (iterator *database.AttributeIterator, err error) {
	state, err := r.snapshotState(snap)
	if err != nil {
		return nil, err
	}

	var lowerBound string
	if pagination.Cursor() != "" {
		var t database.ContinuousToken
		t, err = utils.EncodedContinuousToken{Value: pagination.Cursor()}.Decode()
		if err != nil {
			return nil, err
		}
		lowerBound = t.(utils.ContinuousToken).Value
	}

	attributes := filterAttributeState(state, tenantID, filter)
	sort.Slice(attributes, func(i, j int) bool {
		switch pagination.Sort() {
		case "entity_id":
			return attributes[i].EntityID < attributes[j].EntityID
		default:
			if attributes[i].EntityID != attributes[j].EntityID {
				return attributes[i].EntityID < attributes[j].EntityID
			}
			if attributes[i].Attribute != attributes[j].Attribute {
				return attributes[i].Attribute < attributes[j].Attribute
			}
			return attributes[i].ID < attributes[j].ID
		}
	})

	var result []*base.Attribute
	count := uint32(0)
	limit := pagination.Limit()

	for _, a := range attributes {
		if pagination.Sort() == "entity_id" && a.EntityID < lowerBound {
			continue
		}

		result = append(result, a.ToAttribute())
		count++
		if limit > 0 && count >= limit {
			break
		}
	}

	return database.NewAttributeCollection(result...).CreateAttributeIterator(), nil
}

// ReadAttributes reads attributes from one immutable snapshot taking into
// account the pagination.
func (r *DataReader) ReadAttributes(_ context.Context, tenantID string, filter *base.AttributeFilter, snap string, pagination database.Pagination) (collection *database.AttributeCollection, ct database.EncodedContinuousToken, err error) {
	state, err := r.snapshotState(snap)
	if err != nil {
		return nil, database.NewNoopContinuousToken().Encode(), err
	}

	var lowerBound uint64
	if pagination.Token() != "" {
		var t database.ContinuousToken
		t, err = utils.EncodedContinuousToken{Value: pagination.Token()}.Decode()
		if err != nil {
			return nil, database.NewNoopContinuousToken().Encode(), err
		}
		lowerBound, err = strconv.ParseUint(t.(utils.ContinuousToken).Value, 10, 64)
		if err != nil {
			return nil, database.NewNoopContinuousToken().Encode(), errors.New(base.ErrorCode_ERROR_CODE_INVALID_CONTINUOUS_TOKEN.String())
		}
	}

	attributes := filterAttributeState(state, tenantID, filter)
	sort.Slice(attributes, func(i, j int) bool {
		return attributes[i].ID < attributes[j].ID
	})

	result := make([]*base.Attribute, 0, pagination.PageSize()+1)
	for _, a := range attributes {
		if a.ID >= lowerBound {
			result = append(result, a.ToAttribute())
			if pagination.PageSize() != 0 && len(result) > int(pagination.PageSize()) {
				return database.NewAttributeCollection(result[:pagination.PageSize()]...), utils.NewContinuousToken(strconv.FormatUint(a.ID, 10)).Encode(), nil
			}
		}
	}

	return database.NewAttributeCollection(result...), database.NewNoopContinuousToken().Encode(), nil
}

// QueryUniqueSubjectReferences queries unique subject references from one
// immutable snapshot.
func (r *DataReader) QueryUniqueSubjectReferences(_ context.Context, tenantID string, subjectReference *base.RelationReference, excluded []string, snap string, pagination database.Pagination) (ids []string, _ database.EncodedContinuousToken, err error) {
	state, err := r.snapshotState(snap)
	if err != nil {
		return nil, database.NewNoopContinuousToken().Encode(), err
	}

	var lowerBound string
	if pagination.Token() != "" {
		var t database.ContinuousToken
		t, err := utils.EncodedContinuousToken{Value: pagination.Token()}.Decode()
		if err != nil {
			return nil, database.NewNoopContinuousToken().Encode(), err
		}
		lowerBound = t.(utils.ContinuousToken).Value
	}

	filter := &base.TupleFilter{
		Subject: &base.SubjectFilter{
			Type:     subjectReference.GetType(),
			Relation: subjectReference.GetRelation(),
		},
	}
	tuples := filterRelationshipState(state, tenantID, filter)

	var subjectIDs []string
	for _, t := range tuples {
		subjectIDs = append(subjectIDs, t.SubjectID)
	}
	sort.Strings(subjectIDs)

	seen := make(map[string]bool)
	var lastID string
	for _, subjectID := range subjectIDs {
		if slices.Contains(excluded, subjectID) {
			continue
		}
		if _, exists := seen[subjectID]; !exists && subjectID >= lowerBound {
			ids = append(ids, subjectID)
			seen[subjectID] = true

			if len(ids) == int(pagination.PageSize())+1 {
				lastID = subjectID
			}

			if pagination.PageSize() != 0 && len(ids) > int(pagination.PageSize()) {
				return ids[:pagination.PageSize()], utils.NewContinuousToken(lastID).Encode(), nil
			}
		}
	}

	return ids, database.NewNoopContinuousToken().Encode(), nil
}

// HeadSnapshot - Reads the latest committed snapshot from the repository.
func (r *DataReader) HeadSnapshot(_ context.Context, _ string) (token.SnapToken, error) {
	return snapshot.NewTokenFromValue(r.database.LatestSnapshot()), nil
}

func (r *DataReader) snapshotState(snap string) (*snapshot.State, error) {
	if snap != "" {
		decoded, err := snapshot.EncodedToken{Value: snap}.Decode()
		if err == nil {
			if value, ok := decoded.(snapshot.Token); ok {
				if state, found := r.database.Snapshot(value.Value); found {
					if snapshotState, ok := state.(*snapshot.State); ok {
						return snapshotState, nil
					}
				}
				if value.Value == 0 {
					return &snapshot.State{}, nil
				}
			}
		}
	}

	if state, found := r.database.Snapshot(r.database.LatestSnapshot()); found {
		if snapshotState, ok := state.(*snapshot.State); ok {
			return snapshotState, nil
		}
	}
	return &snapshot.State{}, nil
}

func filterRelationshipState(state *snapshot.State, tenantID string, filter *base.TupleFilter) []storage.RelationTuple {
	exclude := utils.FilterRelationTuplesQuery(tenantID, filter)
	result := make([]storage.RelationTuple, 0, len(state.Tuples))
	for _, tuple := range state.Tuples {
		if !exclude(tuple) {
			result = append(result, tuple)
		}
	}
	return result
}

func filterAttributeState(state *snapshot.State, tenantID string, filter *base.AttributeFilter) []storage.Attribute {
	exclude := utils.FilterAttributesQuery(tenantID, filter)
	result := make([]storage.Attribute, 0, len(state.Attributes))
	for _, attribute := range state.Attributes {
		if !exclude(attribute) {
			result = append(result, attribute)
		}
	}
	return result
}

func stringsCompare(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
