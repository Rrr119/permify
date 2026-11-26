package memory

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-memdb"
)

// Memory - Structure for in memory db
type Memory struct {
	sync.RWMutex
	rid uint64
	aid uint64

	snapshotMu     sync.RWMutex
	commitMu       sync.Mutex
	latestSnapshot uint64
	snapshotSeq    uint64
	snapshots      map[uint64]any

	DB *memdb.MemDB
}

// New - Creates new database schema in memory
func New(schema *memdb.DBSchema) (*Memory, error) {
	db, err := memdb.NewMemDB(schema)
	return &Memory{
		DB:        db,
		snapshots: make(map[uint64]any),
	}, err
}

func (m *Memory) RelationTupleID() (id uint64) {
	m.Lock()
	defer m.Unlock()
	if m.rid == 0 {
		m.rid++
	}
	id = m.rid
	m.rid++
	return id
}

func (m *Memory) AttributeID() (id uint64) {
	m.Lock()
	defer m.Unlock()
	if m.aid == 0 {
		m.aid++
	}
	id = m.aid
	m.aid++
	return id
}

// GetEngineType - Gets engine type, returns as string
func (m *Memory) GetEngineType() string {
	return "memory"
}

// NextSnapshot allocates a monotonically increasing snapshot token.
func (m *Memory) NextSnapshot() uint64 {
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()

	value := uint64(time.Now().UnixNano())
	if value <= m.snapshotSeq {
		value = m.snapshotSeq + 1
	}
	m.snapshotSeq = value
	return value
}

// LockSnapshotCommit serializes commit and snapshot capture so a snapshot
// cannot accidentally include a later concurrent write.
func (m *Memory) LockSnapshotCommit() {
	m.commitMu.Lock()
}

// UnlockSnapshotCommit releases the commit/snapshot serialization lock.
func (m *Memory) UnlockSnapshotCommit() {
	m.commitMu.Unlock()
}

// SaveSnapshot stores an immutable view of the data at the given snapshot.
func (m *Memory) SaveSnapshot(value uint64, state any) {
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()

	m.snapshots[value] = state
	if value > m.latestSnapshot {
		m.latestSnapshot = value
	}
}

// LatestSnapshot returns the most recently committed snapshot token.
func (m *Memory) LatestSnapshot() uint64 {
	m.snapshotMu.RLock()
	defer m.snapshotMu.RUnlock()
	return m.latestSnapshot
}

// Snapshot returns the immutable data view stored for a snapshot token.
func (m *Memory) Snapshot(value uint64) (any, bool) {
	m.snapshotMu.RLock()
	defer m.snapshotMu.RUnlock()
	state, ok := m.snapshots[value]
	return state, ok
}

// Close - Closing the in memory instance
func (m *Memory) Close() error {
	m.Lock()
	defer m.Unlock()
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()
	m.DB = nil
	m.snapshots = nil
	return nil
}

// IsReady - Check if database is ready
func (m *Memory) IsReady(_ context.Context) (bool, error) {
	return true, nil
}
