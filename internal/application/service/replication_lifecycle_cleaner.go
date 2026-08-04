package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/replication"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/infrastructure/repository"
	"go.uber.org/zap"
)

// ReplicationLifecycleCleaner periodically removes obsolete replication history.
type ReplicationLifecycleCleaner struct {
	config     *config.Config
	repository repository.ReplicationLifecycleRepository
	logger     *zap.Logger
	now        func() time.Time
}

// NewReplicationLifecycleCleaner creates an active-only cleanup worker.
func NewReplicationLifecycleCleaner(cfg *config.Config, repo repository.ReplicationLifecycleRepository, logger *zap.Logger) *ReplicationLifecycleCleaner {
	if cfg == nil || repo == nil {
		return nil
	}
	return &ReplicationLifecycleCleaner{config: cfg, repository: repo, logger: logger, now: time.Now}
}

// Enabled reports whether automatic lifecycle cleanup should run on this node.
func (c *ReplicationLifecycleCleaner) Enabled() bool {
	return c != nil && c.config != nil && c.config.Replication.Enabled &&
		c.config.Replication.LifecycleCleanupEnabled &&
		c.config.Replication.LifecycleCleanupInterval > 0 &&
		!strings.EqualFold(strings.TrimSpace(c.config.Node.Role), "standby")
}

// Run starts the periodic cleanup loop until ctx is canceled.
func (c *ReplicationLifecycleCleaner) Run(ctx context.Context) {
	if !c.Enabled() {
		return
	}
	ticker := time.NewTicker(c.config.Replication.LifecycleCleanupInterval)
	defer ticker.Stop()

	if c.logger != nil {
		c.logger.Info("replication lifecycle cleaner started", zap.Duration("interval", c.config.Replication.LifecycleCleanupInterval))
		defer c.logger.Info("replication lifecycle cleaner stopped")
	}
	c.cleanupAndLog(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanupAndLog(ctx)
		}
	}
}

// CleanupOnce executes one cleanup pass and is also suitable for manual operations.
func (c *ReplicationLifecycleCleaner) CleanupOnce(ctx context.Context) (*replication.LifecycleCleanupResult, error) {
	if c == nil || c.repository == nil || c.config == nil {
		return &replication.LifecycleCleanupResult{}, nil
	}
	now := c.now()
	return c.repository.CleanupHistory(
		ctx,
		now.Add(-c.config.Replication.ReconcileItemRetention),
		now.Add(-c.config.Replication.ReconcileJobRetention),
		now.Add(-c.config.Replication.OutboxRetention),
	)
}

// PreviewOnce returns the rows that would be removed by CleanupOnce.
func (c *ReplicationLifecycleCleaner) PreviewOnce(ctx context.Context) (*replication.LifecycleCleanupResult, error) {
	if c == nil || c.repository == nil || c.config == nil {
		return &replication.LifecycleCleanupResult{}, nil
	}
	now := c.now()
	return c.repository.PreviewHistoryCleanup(
		ctx,
		now.Add(-c.config.Replication.ReconcileItemRetention),
		now.Add(-c.config.Replication.ReconcileJobRetention),
		now.Add(-c.config.Replication.OutboxRetention),
	)
}

func (c *ReplicationLifecycleCleaner) cleanupAndLog(ctx context.Context) {
	result, err := c.CleanupOnce(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) && c.logger != nil {
			c.logger.Warn("replication lifecycle cleanup failed", zap.Error(err))
		}
		return
	}
	if c.logger != nil && result != nil {
		c.logger.Info("replication lifecycle cleanup completed",
			zap.Int64("deleted_reconcile_items", result.DeletedReconcileItems),
			zap.Int64("deleted_reconcile_jobs", result.DeletedReconcileJobs),
			zap.Int64("deleted_outbox_events", result.DeletedOutboxEvents))
	}
}
