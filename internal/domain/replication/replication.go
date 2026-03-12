package replication

import (
	"errors"
	"time"
)

const (
	StatusPending    = "pending"
	StatusDispatched = "dispatched"
	StatusFailed     = "failed"
)

const (
	OpEnsureDir  = "ensure_dir"
	OpUpsertFile = "upsert_file"
	OpMovePath   = "move_path"
	OpCopyPath   = "copy_path"
	OpRemovePath = "remove_path"
)

var (
	ErrOutboxEventNotFound = errors.New("replication outbox event not found")
	ErrOffsetNotFound      = errors.New("replication offset not found")
)

// OutboxEvent is one durable file mutation event waiting to be replicated.
type OutboxEvent struct {
	ID            int64
	SourceNodeID  string
	TargetNodeID  string
	Op            string
	Path          *string
	FromPath      *string
	ToPath        *string
	IsDir         bool
	ContentSHA256 *string
	FileSize      *int64
	Status        string
	AttemptCount  int
	NextRetryAt   time.Time
	LastError     *string
	CreatedAt     time.Time
	DispatchedAt  *time.Time
}

// OutboxStatus summarizes current queue state for one source->target pair.
type OutboxStatus struct {
	PendingEvents          int64
	FailedEvents           int64
	LastOutboxID           *int64
	LastDispatchedOutboxID *int64
	OldestPendingCreatedAt *time.Time
	LastFailedOutboxID     *int64
	LastFailureAttempt     *int
	NextRetryAt            *time.Time
	LastError              *string
}

// Offset tracks how far a target node has applied events from a source node.
type Offset struct {
	SourceNodeID        string
	TargetNodeID        string
	LastAppliedOutboxID int64
	LastAppliedAt       time.Time
	UpdatedAt           time.Time
}
