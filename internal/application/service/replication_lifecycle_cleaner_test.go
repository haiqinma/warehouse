package service

import (
	"context"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/replication"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
)

type fakeReplicationLifecycleRepository struct {
	itemCutoff   time.Time
	jobCutoff    time.Time
	outboxCutoff time.Time
}

func (r *fakeReplicationLifecycleRepository) PreviewHistoryCleanup(_ context.Context, itemCutoff, jobCutoff, outboxCutoff time.Time) (*replication.LifecycleCleanupResult, error) {
	r.itemCutoff = itemCutoff
	r.jobCutoff = jobCutoff
	r.outboxCutoff = outboxCutoff
	return &replication.LifecycleCleanupResult{DeletedReconcileItems: 6, DeletedReconcileJobs: 4, DeletedOutboxEvents: 2}, nil
}

func (r *fakeReplicationLifecycleRepository) CleanupHistory(_ context.Context, itemCutoff, jobCutoff, outboxCutoff time.Time) (*replication.LifecycleCleanupResult, error) {
	r.itemCutoff = itemCutoff
	r.jobCutoff = jobCutoff
	r.outboxCutoff = outboxCutoff
	return &replication.LifecycleCleanupResult{DeletedReconcileItems: 3, DeletedReconcileJobs: 2, DeletedOutboxEvents: 1}, nil
}

func TestReplicationLifecycleCleanerCleanupOnceUsesConfiguredRetention(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Replication.Enabled = true
	cfg.Replication.ReconcileItemRetention = 48 * time.Hour
	cfg.Replication.ReconcileJobRetention = 10 * 24 * time.Hour
	cfg.Replication.OutboxRetention = 72 * time.Hour
	repo := &fakeReplicationLifecycleRepository{}
	cleaner := NewReplicationLifecycleCleaner(cfg, repo, nil)
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	cleaner.now = func() time.Time { return now }

	result, err := cleaner.CleanupOnce(context.Background())
	if err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}
	if result.DeletedReconcileItems != 3 || result.DeletedReconcileJobs != 2 || result.DeletedOutboxEvents != 1 {
		t.Fatalf("unexpected cleanup result: %#v", result)
	}
	if !repo.itemCutoff.Equal(now.Add(-48*time.Hour)) || !repo.jobCutoff.Equal(now.Add(-10*24*time.Hour)) || !repo.outboxCutoff.Equal(now.Add(-72*time.Hour)) {
		t.Fatalf("unexpected cutoffs: item=%v job=%v outbox=%v", repo.itemCutoff, repo.jobCutoff, repo.outboxCutoff)
	}
}

func TestReplicationLifecycleCleanerEnabledOnlyOnActive(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Replication.Enabled = true
	repo := &fakeReplicationLifecycleRepository{}
	cleaner := NewReplicationLifecycleCleaner(cfg, repo, nil)
	if !cleaner.Enabled() {
		t.Fatal("expected cleaner enabled on active")
	}
	cfg.Node.Role = "standby"
	if cleaner.Enabled() {
		t.Fatal("expected cleaner disabled on standby")
	}
	cfg.Node.Role = "active"
	cfg.Replication.LifecycleCleanupEnabled = false
	if cleaner.Enabled() {
		t.Fatal("expected cleaner disabled by config")
	}
}

func TestReplicationLifecycleCleanerPreviewOnceUsesConfiguredRetention(t *testing.T) {
	cfg := config.DefaultConfig()
	repo := &fakeReplicationLifecycleRepository{}
	cleaner := NewReplicationLifecycleCleaner(cfg, repo, nil)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	cleaner.now = func() time.Time { return now }

	result, err := cleaner.PreviewOnce(context.Background())
	if err != nil {
		t.Fatalf("PreviewOnce: %v", err)
	}
	if result.DeletedReconcileItems != 6 || result.DeletedReconcileJobs != 4 || result.DeletedOutboxEvents != 2 {
		t.Fatalf("unexpected preview result: %#v", result)
	}
	if !repo.itemCutoff.Equal(now.Add(-cfg.Replication.ReconcileItemRetention)) || !repo.jobCutoff.Equal(now.Add(-cfg.Replication.ReconcileJobRetention)) || !repo.outboxCutoff.Equal(now.Add(-cfg.Replication.OutboxRetention)) {
		t.Fatalf("unexpected preview cutoffs: item=%v job=%v outbox=%v", repo.itemCutoff, repo.jobCutoff, repo.outboxCutoff)
	}
}
