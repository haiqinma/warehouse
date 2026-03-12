package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/replication"
)

const defaultReplicationPendingLimit = 100

// ReplicationOutboxRepository stores durable replication events.
type ReplicationOutboxRepository interface {
	Append(ctx context.Context, event *replication.OutboxEvent) error
	ListPending(ctx context.Context, sourceNodeID, targetNodeID string, limit int) ([]*replication.OutboxEvent, error)
	MarkDispatched(ctx context.Context, id int64, dispatchedAt time.Time) error
	MarkFailed(ctx context.Context, id int64, lastError string, nextRetryAt time.Time) error
	GetStatusSummary(ctx context.Context, sourceNodeID, targetNodeID string) (*replication.OutboxStatus, error)
}

// ReplicationOffsetRepository stores apply progress for a source->target pair.
type ReplicationOffsetRepository interface {
	Upsert(ctx context.Context, offset *replication.Offset) error
	Get(ctx context.Context, sourceNodeID, targetNodeID string) (*replication.Offset, error)
}

// PostgresReplicationOutboxRepository is the PostgreSQL implementation.
type PostgresReplicationOutboxRepository struct {
	db *sql.DB
}

// PostgresReplicationOffsetRepository is the PostgreSQL implementation.
type PostgresReplicationOffsetRepository struct {
	db *sql.DB
}

// NewPostgresReplicationOutboxRepository creates an outbox repository.
func NewPostgresReplicationOutboxRepository(db *sql.DB) *PostgresReplicationOutboxRepository {
	return &PostgresReplicationOutboxRepository{db: db}
}

// NewPostgresReplicationOffsetRepository creates an offset repository.
func NewPostgresReplicationOffsetRepository(db *sql.DB) *PostgresReplicationOffsetRepository {
	return &PostgresReplicationOffsetRepository{db: db}
}

// Append inserts a new durable replication event.
func (r *PostgresReplicationOutboxRepository) Append(ctx context.Context, event *replication.OutboxEvent) error {
	status := strings.TrimSpace(event.Status)
	if status == "" {
		status = replication.StatusPending
	}
	if event.NextRetryAt.IsZero() {
		event.NextRetryAt = time.Now()
	}

	query := `
		INSERT INTO replication_outbox (
			source_node_id, target_node_id, op, path, from_path, to_path,
			is_dir, content_sha256, file_size, status, next_retry_at, last_error
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id, status, attempt_count, next_retry_at, created_at, dispatched_at
	`

	var dispatchedAt sql.NullTime
	err := r.db.QueryRowContext(ctx, query,
		event.SourceNodeID,
		event.TargetNodeID,
		event.Op,
		event.Path,
		event.FromPath,
		event.ToPath,
		event.IsDir,
		event.ContentSHA256,
		event.FileSize,
		status,
		event.NextRetryAt,
		event.LastError,
	).Scan(
		&event.ID,
		&event.Status,
		&event.AttemptCount,
		&event.NextRetryAt,
		&event.CreatedAt,
		&dispatchedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to append replication outbox event: %w", err)
	}
	if dispatchedAt.Valid {
		event.DispatchedAt = &dispatchedAt.Time
	}

	return nil
}

// ListPending lists ready-to-dispatch events ordered by durable sequence id.
func (r *PostgresReplicationOutboxRepository) ListPending(ctx context.Context, sourceNodeID, targetNodeID string, limit int) ([]*replication.OutboxEvent, error) {
	if limit <= 0 {
		limit = defaultReplicationPendingLimit
	}

	query := `
		SELECT id, source_node_id, target_node_id, op, path, from_path, to_path,
		       is_dir, content_sha256, file_size, status, attempt_count,
		       next_retry_at, last_error, created_at, dispatched_at
		FROM replication_outbox
		WHERE source_node_id = $1
		  AND target_node_id = $2
		  AND status IN ($3, $4)
		  AND next_retry_at <= NOW()
		ORDER BY id ASC
		LIMIT $5
	`

	rows, err := r.db.QueryContext(ctx, query,
		sourceNodeID,
		targetNodeID,
		replication.StatusPending,
		replication.StatusFailed,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending replication events: %w", err)
	}
	defer rows.Close()

	var events []*replication.OutboxEvent
	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate pending replication events: %w", err)
	}

	return events, nil
}

// MarkDispatched records that an event was delivered successfully.
func (r *PostgresReplicationOutboxRepository) MarkDispatched(ctx context.Context, id int64, dispatchedAt time.Time) error {
	if dispatchedAt.IsZero() {
		dispatchedAt = time.Now()
	}

	query := `
		UPDATE replication_outbox
		SET status = $2,
		    dispatched_at = $3,
		    last_error = NULL
		WHERE id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id, replication.StatusDispatched, dispatchedAt)
	if err != nil {
		return fmt.Errorf("failed to mark replication event as dispatched: %w", err)
	}
	if err := ensureAffectedRows(result, replication.ErrOutboxEventNotFound); err != nil {
		return err
	}

	return nil
}

// MarkFailed records an attempt failure and schedules the next retry.
func (r *PostgresReplicationOutboxRepository) MarkFailed(ctx context.Context, id int64, lastError string, nextRetryAt time.Time) error {
	if nextRetryAt.IsZero() {
		nextRetryAt = time.Now()
	}

	query := `
		UPDATE replication_outbox
		SET status = $2,
		    attempt_count = attempt_count + 1,
		    next_retry_at = $3,
		    last_error = $4
		WHERE id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id, replication.StatusFailed, nextRetryAt, strings.TrimSpace(lastError))
	if err != nil {
		return fmt.Errorf("failed to mark replication event as failed: %w", err)
	}
	if err := ensureAffectedRows(result, replication.ErrOutboxEventNotFound); err != nil {
		return err
	}

	return nil
}

// GetStatusSummary returns queue depth and lag hints for one source->target pair.
func (r *PostgresReplicationOutboxRepository) GetStatusSummary(ctx context.Context, sourceNodeID, targetNodeID string) (*replication.OutboxStatus, error) {
	query := `
		WITH pair_events AS (
			SELECT id, status, attempt_count, next_retry_at, last_error, created_at
			FROM replication_outbox
			WHERE source_node_id = $1
			  AND target_node_id = $2
		),
		last_failed AS (
			SELECT id, attempt_count, next_retry_at, last_error
			FROM pair_events
			WHERE status = $3
			ORDER BY id DESC
			LIMIT 1
		)
		SELECT
			COUNT(*) FILTER (WHERE status IN ($4, $3)) AS pending_events,
			COUNT(*) FILTER (WHERE status = $3) AS failed_events,
			MAX(id) AS last_outbox_id,
			MAX(id) FILTER (WHERE status = $5) AS last_dispatched_outbox_id,
			MIN(created_at) FILTER (WHERE status IN ($4, $3)) AS oldest_pending_created_at,
			(SELECT id FROM last_failed),
			(SELECT attempt_count FROM last_failed),
			(SELECT next_retry_at FROM last_failed),
			(SELECT last_error FROM last_failed)
		FROM pair_events
	`

	status := &replication.OutboxStatus{}
	var lastOutboxID sql.NullInt64
	var lastDispatchedID sql.NullInt64
	var oldestPendingCreatedAt sql.NullTime
	var lastFailedID sql.NullInt64
	var lastFailureAttempt sql.NullInt64
	var nextRetryAt sql.NullTime
	var lastError sql.NullString
	err := r.db.QueryRowContext(ctx, query,
		sourceNodeID,
		targetNodeID,
		replication.StatusFailed,
		replication.StatusPending,
		replication.StatusDispatched,
	).Scan(
		&status.PendingEvents,
		&status.FailedEvents,
		&lastOutboxID,
		&lastDispatchedID,
		&oldestPendingCreatedAt,
		&lastFailedID,
		&lastFailureAttempt,
		&nextRetryAt,
		&lastError,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication outbox status summary: %w", err)
	}
	if lastOutboxID.Valid {
		status.LastOutboxID = &lastOutboxID.Int64
	}
	if lastDispatchedID.Valid {
		status.LastDispatchedOutboxID = &lastDispatchedID.Int64
	}
	if oldestPendingCreatedAt.Valid {
		status.OldestPendingCreatedAt = &oldestPendingCreatedAt.Time
	}
	if lastFailedID.Valid {
		status.LastFailedOutboxID = &lastFailedID.Int64
	}
	if lastFailureAttempt.Valid {
		status.LastFailureAttempt = nullableInt(lastFailureAttempt)
	}
	if nextRetryAt.Valid {
		status.NextRetryAt = &nextRetryAt.Time
	}
	status.LastError = nullableString(lastError)

	return status, nil
}

// Upsert stores the latest apply progress for one source->target pair.
func (r *PostgresReplicationOffsetRepository) Upsert(ctx context.Context, offset *replication.Offset) error {
	updatedAt := offset.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}

	query := `
		INSERT INTO replication_offsets (
			source_node_id, target_node_id, last_applied_outbox_id, last_applied_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (source_node_id, target_node_id)
		DO UPDATE SET
			last_applied_outbox_id = EXCLUDED.last_applied_outbox_id,
			last_applied_at = EXCLUDED.last_applied_at,
			updated_at = EXCLUDED.updated_at
	`

	_, err := r.db.ExecContext(ctx, query,
		offset.SourceNodeID,
		offset.TargetNodeID,
		offset.LastAppliedOutboxID,
		offset.LastAppliedAt,
		updatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert replication offset: %w", err)
	}
	if offset.UpdatedAt.IsZero() {
		offset.UpdatedAt = updatedAt
	}

	return nil
}

// Get fetches the last applied sequence for one source->target pair.
func (r *PostgresReplicationOffsetRepository) Get(ctx context.Context, sourceNodeID, targetNodeID string) (*replication.Offset, error) {
	query := `
		SELECT source_node_id, target_node_id, last_applied_outbox_id, last_applied_at, updated_at
		FROM replication_offsets
		WHERE source_node_id = $1 AND target_node_id = $2
	`

	offset := &replication.Offset{}
	err := r.db.QueryRowContext(ctx, query, sourceNodeID, targetNodeID).Scan(
		&offset.SourceNodeID,
		&offset.TargetNodeID,
		&offset.LastAppliedOutboxID,
		&offset.LastAppliedAt,
		&offset.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, replication.ErrOffsetNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get replication offset: %w", err)
	}

	return offset, nil
}

func scanOutboxEvent(scanner interface {
	Scan(dest ...interface{}) error
}) (*replication.OutboxEvent, error) {
	event := &replication.OutboxEvent{}
	var path sql.NullString
	var fromPath sql.NullString
	var toPath sql.NullString
	var contentSHA256 sql.NullString
	var fileSize sql.NullInt64
	var lastError sql.NullString
	var dispatchedAt sql.NullTime

	if err := scanner.Scan(
		&event.ID,
		&event.SourceNodeID,
		&event.TargetNodeID,
		&event.Op,
		&path,
		&fromPath,
		&toPath,
		&event.IsDir,
		&contentSHA256,
		&fileSize,
		&event.Status,
		&event.AttemptCount,
		&event.NextRetryAt,
		&lastError,
		&event.CreatedAt,
		&dispatchedAt,
	); err != nil {
		return nil, fmt.Errorf("failed to scan replication outbox event: %w", err)
	}

	event.Path = nullableString(path)
	event.FromPath = nullableString(fromPath)
	event.ToPath = nullableString(toPath)
	event.ContentSHA256 = nullableString(contentSHA256)
	event.FileSize = nullableInt64(fileSize)
	event.LastError = nullableString(lastError)
	if dispatchedAt.Valid {
		event.DispatchedAt = &dispatchedAt.Time
	}

	return event, nil
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	v := value.String
	return &v
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	v := value.Int64
	return &v
}

func nullableInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	v := int(value.Int64)
	return &v
}

func ensureAffectedRows(result sql.Result, notFound error) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	if rowsAffected == 0 {
		return notFound
	}
	return nil
}
