package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/replication"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

type fakeReplicationOutboxReader struct {
	expectedSource string
	expectedTarget string
	summary        *replication.OutboxStatus
}

func (r fakeReplicationOutboxReader) GetStatusSummary(_ context.Context, sourceNodeID, targetNodeID string) (*replication.OutboxStatus, error) {
	if sourceNodeID != r.expectedSource || targetNodeID != r.expectedTarget {
		return nil, fmt.Errorf("unexpected pair %s -> %s", sourceNodeID, targetNodeID)
	}
	return r.summary, nil
}

type fakeReplicationOffsetReader struct {
	expectedSource string
	expectedTarget string
	offset         *replication.Offset
	err            error
}

func (r fakeReplicationOffsetReader) Get(_ context.Context, sourceNodeID, targetNodeID string) (*replication.Offset, error) {
	if sourceNodeID != r.expectedSource || targetNodeID != r.expectedTarget {
		return nil, fmt.Errorf("unexpected pair %s -> %s", sourceNodeID, targetNodeID)
	}
	return r.offset, r.err
}

func (r fakeReplicationOffsetReader) Upsert(_ context.Context, _ *replication.Offset) error {
	return nil
}

func TestInternalReplicationHandleStatusActive(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Node.ID = "node-a"
	cfg.Node.Role = "active"
	cfg.Internal.Replication.Enabled = true
	cfg.Internal.Replication.PeerNodeID = "node-b"
	cfg.Internal.Replication.PeerBaseURL = "https://standby.internal"

	lastOutboxID := int64(12)
	lastDispatched := int64(11)
	lastApplied := int64(9)
	lastFailed := int64(10)
	lastFailureAttempt := 3
	oldestPending := time.Now().Add(-2 * time.Minute)
	nextRetryAt := time.Now().Add(30 * time.Second)
	lastError := "peer returned 500"
	handler := NewInternalReplicationHandler(
		cfg,
		zap.NewNop(),
		fakeReplicationOutboxReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			summary: &replication.OutboxStatus{
				PendingEvents:          3,
				FailedEvents:           1,
				LastOutboxID:           &lastOutboxID,
				LastDispatchedOutboxID: &lastDispatched,
				OldestPendingCreatedAt: &oldestPending,
				LastFailedOutboxID:     &lastFailed,
				LastFailureAttempt:     &lastFailureAttempt,
				NextRetryAt:            &nextRetryAt,
				LastError:              &lastError,
			},
		},
		fakeReplicationOffsetReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			offset: &replication.Offset{
				SourceNodeID:        "node-a",
				TargetNodeID:        "node-b",
				LastAppliedOutboxID: lastApplied,
			},
		},
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/replication/status", nil)
	recorder := httptest.NewRecorder()

	handler.HandleStatus(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	var resp internalReplicationStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Node.ID != "node-a" || resp.Node.Role != "active" {
		t.Fatalf("unexpected node payload: %#v", resp.Node)
	}
	if resp.Replication.State != "retrying" {
		t.Fatalf("unexpected replication state: %#v", resp.Replication)
	}
	if !resp.Replication.WorkerEnabled {
		t.Fatalf("expected worker to be enabled: %#v", resp.Replication)
	}
	if resp.Replication.PeerNodeID != "node-b" || resp.Replication.PeerBaseURL != "https://standby.internal" {
		t.Fatalf("unexpected peer payload: %#v", resp.Replication)
	}
	if resp.Replication.LastOutboxID == nil || *resp.Replication.LastOutboxID != lastOutboxID {
		t.Fatalf("unexpected last outbox id: %#v", resp.Replication.LastOutboxID)
	}
	if resp.Replication.PendingEvents == nil || *resp.Replication.PendingEvents != 3 {
		t.Fatalf("unexpected pending events: %#v", resp.Replication.PendingEvents)
	}
	if resp.Replication.FailedEvents == nil || *resp.Replication.FailedEvents != 1 {
		t.Fatalf("unexpected failed events: %#v", resp.Replication.FailedEvents)
	}
	if resp.Replication.LastDispatchedOutboxID == nil || *resp.Replication.LastDispatchedOutboxID != lastDispatched {
		t.Fatalf("unexpected last dispatched id: %#v", resp.Replication.LastDispatchedOutboxID)
	}
	if resp.Replication.LastAppliedOutboxID == nil || *resp.Replication.LastAppliedOutboxID != lastApplied {
		t.Fatalf("unexpected last applied id: %#v", resp.Replication.LastAppliedOutboxID)
	}
	if resp.Replication.LastFailedOutboxID == nil || *resp.Replication.LastFailedOutboxID != lastFailed {
		t.Fatalf("unexpected last failed id: %#v", resp.Replication.LastFailedOutboxID)
	}
	if resp.Replication.LastFailureAttempt == nil || *resp.Replication.LastFailureAttempt != lastFailureAttempt {
		t.Fatalf("unexpected last failure attempt: %#v", resp.Replication.LastFailureAttempt)
	}
	if resp.Replication.NextRetryAt == nil || !resp.Replication.NextRetryAt.Equal(nextRetryAt) {
		t.Fatalf("unexpected next retry at: %#v", resp.Replication.NextRetryAt)
	}
	if resp.Replication.LastError == nil || *resp.Replication.LastError != lastError {
		t.Fatalf("unexpected last error: %#v", resp.Replication.LastError)
	}
	if resp.Replication.LagSeconds == nil || *resp.Replication.LagSeconds < 60 {
		t.Fatalf("expected positive lag seconds, got %#v", resp.Replication.LagSeconds)
	}
}

func TestInternalReplicationHandleStatusStandbyUsesReversePair(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Node.ID = "node-b"
	cfg.Node.Role = "standby"
	cfg.Internal.Replication.Enabled = true
	cfg.Internal.Replication.PeerNodeID = "node-a"

	lastOutboxID := int64(42)
	lastDispatched := int64(42)
	lastApplied := int64(42)
	handler := NewInternalReplicationHandler(
		cfg,
		zap.NewNop(),
		fakeReplicationOutboxReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			summary: &replication.OutboxStatus{
				PendingEvents:          0,
				FailedEvents:           0,
				LastOutboxID:           &lastOutboxID,
				LastDispatchedOutboxID: &lastDispatched,
			},
		},
		fakeReplicationOffsetReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			offset: &replication.Offset{
				SourceNodeID:        "node-a",
				TargetNodeID:        "node-b",
				LastAppliedOutboxID: lastApplied,
			},
		},
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/replication/status", nil)
	recorder := httptest.NewRecorder()

	handler.HandleStatus(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	var resp internalReplicationStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Replication.State != "caught_up" {
		t.Fatalf("unexpected replication state: %#v", resp.Replication)
	}
	if resp.Replication.LastAppliedOutboxID == nil || *resp.Replication.LastAppliedOutboxID != lastApplied {
		t.Fatalf("unexpected last applied id: %#v", resp.Replication.LastAppliedOutboxID)
	}
	if resp.Replication.LastOutboxID == nil || *resp.Replication.LastOutboxID != lastOutboxID {
		t.Fatalf("unexpected last outbox id: %#v", resp.Replication.LastOutboxID)
	}
	if resp.Replication.WorkerEnabled {
		t.Fatalf("standby should not report worker enabled: %#v", resp.Replication)
	}
}

func TestInternalReplicationHandleStatusStandbyBootstrapRequired(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Node.ID = "node-b"
	cfg.Node.Role = "standby"
	cfg.Internal.Replication.Enabled = true
	cfg.Internal.Replication.PeerNodeID = "node-a"

	lastOutboxID := int64(5)
	handler := NewInternalReplicationHandler(
		cfg,
		zap.NewNop(),
		fakeReplicationOutboxReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			summary: &replication.OutboxStatus{
				PendingEvents: 1,
				FailedEvents:  0,
				LastOutboxID:  &lastOutboxID,
			},
		},
		fakeReplicationOffsetReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			err:            replication.ErrOffsetNotFound,
		},
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/replication/status", nil)
	recorder := httptest.NewRecorder()

	handler.HandleStatus(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	var resp internalReplicationStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Replication.State != "bootstrap_required" {
		t.Fatalf("unexpected replication state: %#v", resp.Replication)
	}
	if resp.Replication.LastAppliedOutboxID != nil {
		t.Fatalf("expected no applied offset yet: %#v", resp.Replication.LastAppliedOutboxID)
	}
}

func TestInternalReplicationHandleBootstrapMarkWithExplicitOutboxID(t *testing.T) {
	offsets := newMemoryReplicationOffsetStore()
	cfg := config.DefaultConfig()
	cfg.Node.ID = "node-b"
	cfg.Node.Role = "standby"
	cfg.Internal.Replication.Enabled = true
	cfg.Internal.Replication.PeerNodeID = "node-a"

	handler := NewInternalReplicationHandler(
		cfg,
		zap.NewNop(),
		fakeReplicationOutboxReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			summary:        &replication.OutboxStatus{},
		},
		offsets,
	)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/replication/bootstrap/mark", bytes.NewBufferString(`{"outboxId":7}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Warehouse-Node-Id", "node-a")
	recorder := httptest.NewRecorder()

	handler.HandleBootstrapMark(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var resp internalReplicationBootstrapMarkResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.Success || resp.LastAppliedOutboxID != 7 || resp.UsedCurrentOutboxID {
		t.Fatalf("unexpected bootstrap response: %#v", resp)
	}

	offset, err := offsets.Get(context.Background(), "node-a", "node-b")
	if err != nil {
		t.Fatalf("expected offset to exist: %v", err)
	}
	if offset.LastAppliedOutboxID != 7 {
		t.Fatalf("unexpected offset: %#v", offset)
	}
}

func TestInternalReplicationHandleBootstrapMarkUsesCurrentLastOutboxID(t *testing.T) {
	offsets := newMemoryReplicationOffsetStore()
	cfg := config.DefaultConfig()
	cfg.Node.ID = "node-b"
	cfg.Node.Role = "standby"
	cfg.Internal.Replication.Enabled = true
	cfg.Internal.Replication.PeerNodeID = "node-a"

	lastOutboxID := int64(11)
	handler := NewInternalReplicationHandler(
		cfg,
		zap.NewNop(),
		fakeReplicationOutboxReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			summary: &replication.OutboxStatus{
				LastOutboxID: &lastOutboxID,
			},
		},
		offsets,
	)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/replication/bootstrap/mark", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Warehouse-Node-Id", "node-a")
	recorder := httptest.NewRecorder()

	handler.HandleBootstrapMark(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var resp internalReplicationBootstrapMarkResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.UsedCurrentOutboxID || resp.LastAppliedOutboxID != 11 {
		t.Fatalf("unexpected bootstrap response: %#v", resp)
	}
}

func TestInternalReplicationHandleBootstrapMarkRejectsBackwardOffset(t *testing.T) {
	offsets := newMemoryReplicationOffsetStore()
	offsets.offsets["node-a->node-b"] = &replication.Offset{
		SourceNodeID:        "node-a",
		TargetNodeID:        "node-b",
		LastAppliedOutboxID: 9,
		LastAppliedAt:       time.Now(),
		UpdatedAt:           time.Now(),
	}
	cfg := config.DefaultConfig()
	cfg.Node.ID = "node-b"
	cfg.Node.Role = "standby"
	cfg.Internal.Replication.Enabled = true
	cfg.Internal.Replication.PeerNodeID = "node-a"

	handler := NewInternalReplicationHandler(
		cfg,
		zap.NewNop(),
		fakeReplicationOutboxReader{
			expectedSource: "node-a",
			expectedTarget: "node-b",
			summary:        &replication.OutboxStatus{},
		},
		offsets,
	)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/replication/bootstrap/mark", bytes.NewBufferString(`{"outboxId":8}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Warehouse-Node-Id", "node-a")
	recorder := httptest.NewRecorder()

	handler.HandleBootstrapMark(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d: %s", recorder.Code, recorder.Body.String())
	}

	offset, err := offsets.Get(context.Background(), "node-a", "node-b")
	if err != nil {
		t.Fatalf("expected offset to still exist: %v", err)
	}
	if offset.LastAppliedOutboxID != 9 {
		t.Fatalf("offset should not move backwards: %#v", offset)
	}
}
