package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/replication"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

type replicationOutboxStatusReader interface {
	GetStatusSummary(rctx context.Context, sourceNodeID, targetNodeID string) (*replication.OutboxStatus, error)
}

type replicationOffsetStore interface {
	Get(ctx context.Context, sourceNodeID, targetNodeID string) (*replication.Offset, error)
	Upsert(ctx context.Context, offset *replication.Offset) error
}

type replicationReconcileStore interface {
	CreateJob(ctx context.Context, job *replication.ReconcileJob) error
	ReplaceItems(ctx context.Context, jobID int64, items []*replication.ReconcileItem) error
	UpdateJobResult(ctx context.Context, jobID int64, status string, scannedItems, pendingItems int64, completedAt *time.Time, lastError *string) error
	GetLatestJob(ctx context.Context, sourceNodeID, targetNodeID string) (*replication.ReconcileJob, error)
	ListPendingItems(ctx context.Context, jobID int64, limit int) ([]*replication.ReconcileItem, error)
	UpdateItemsState(ctx context.Context, itemIDs []int64, state string) error
	CountPendingItems(ctx context.Context, jobID int64) (int64, error)
}

type replicationReconcileScanner interface {
	Scan(ctx context.Context) ([]*replication.ReconcileItem, error)
}

// InternalReplicationHandler exposes replication-related internal control endpoints.
type InternalReplicationHandler struct {
	logger           *zap.Logger
	config           *config.Config
	outbox           replicationOutboxStatusReader
	offsets          replicationOffsetStore
	reconcileStore   replicationReconcileStore
	reconcileScanner replicationReconcileScanner
}

// NewInternalReplicationHandler creates a new internal replication handler.
func NewInternalReplicationHandler(
	cfg *config.Config,
	logger *zap.Logger,
	outbox replicationOutboxStatusReader,
	offsets replicationOffsetStore,
	reconcileStore replicationReconcileStore,
	reconcileScanner replicationReconcileScanner,
) *InternalReplicationHandler {
	return &InternalReplicationHandler{
		logger:           logger,
		config:           cfg,
		outbox:           outbox,
		offsets:          offsets,
		reconcileStore:   reconcileStore,
		reconcileScanner: reconcileScanner,
	}
}

type internalReplicationStatusResponse struct {
	Node        internalNodeStatus          `json:"node"`
	Replication internalReplicationStatus   `json:"replication"`
	Reconcile   *internalReconcileJobStatus `json:"reconcile,omitempty"`
}

type internalNodeStatus struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}

type internalReplicationStatus struct {
	Enabled                bool       `json:"enabled"`
	Mode                   string     `json:"mode"`
	State                  string     `json:"state"`
	WorkerEnabled          bool       `json:"workerEnabled"`
	PeerNodeID             string     `json:"peerNodeId,omitempty"`
	PeerBaseURL            string     `json:"peerBaseUrl,omitempty"`
	LastOutboxID           *int64     `json:"lastOutboxId,omitempty"`
	LastAppliedOutboxID    *int64     `json:"lastAppliedOutboxId,omitempty"`
	LastDispatchedOutboxID *int64     `json:"lastDispatchedOutboxId,omitempty"`
	PendingEvents          *int64     `json:"pendingEvents,omitempty"`
	FailedEvents           *int64     `json:"failedEvents,omitempty"`
	LagSeconds             *int64     `json:"lagSeconds,omitempty"`
	LastFailedOutboxID     *int64     `json:"lastFailedOutboxId,omitempty"`
	LastFailureAttempt     *int       `json:"lastFailureAttempt,omitempty"`
	NextRetryAt            *time.Time `json:"nextRetryAt,omitempty"`
	LastError              *string    `json:"lastError,omitempty"`
	Notes                  []string   `json:"notes,omitempty"`
}

type internalReconcileJobStatus struct {
	Enabled           bool       `json:"enabled"`
	LatestJobID       int64      `json:"latestJobId"`
	Status            string     `json:"status"`
	WatermarkOutboxID int64      `json:"watermarkOutboxId"`
	ScannedItems      int64      `json:"scannedItems"`
	PendingItems      int64      `json:"pendingItems"`
	StartedAt         time.Time  `json:"startedAt"`
	CompletedAt       *time.Time `json:"completedAt,omitempty"`
	LastError         *string    `json:"lastError,omitempty"`
}

// HandleStatus returns the current internal replication configuration and persisted status summary.
func (h *InternalReplicationHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	replicationCfg := h.config.Internal.Replication
	response := internalReplicationStatusResponse{
		Node: internalNodeStatus{
			ID:   h.config.Node.ID,
			Role: h.config.Node.Role,
		},
		Replication: internalReplicationStatus{
			Enabled:       replicationCfg.Enabled,
			Mode:          "internal_async",
			State:         "disabled",
			WorkerEnabled: h.workerEnabled(),
			PeerNodeID:    replicationCfg.PeerNodeID,
			PeerBaseURL:   replicationCfg.PeerBaseURL,
		},
	}
	if !replicationCfg.Enabled {
		response.Replication.Notes = []string{"internal replication is disabled"}
		h.writeJSON(w, http.StatusOK, response)
		return
	}

	sourceNodeID, targetNodeID := h.replicationPair()
	if h.outbox != nil && sourceNodeID != "" && targetNodeID != "" {
		summary, err := h.outbox.GetStatusSummary(r.Context(), sourceNodeID, targetNodeID)
		if err != nil {
			h.logger.Error("failed to load replication outbox status",
				zap.String("source_node_id", sourceNodeID),
				zap.String("target_node_id", targetNodeID),
				zap.Error(err))
			h.writeError(w, http.StatusInternalServerError, "Failed to load replication outbox status")
			return
		}
		if summary != nil {
			response.Replication.PendingEvents = &summary.PendingEvents
			response.Replication.FailedEvents = &summary.FailedEvents
			response.Replication.LastOutboxID = summary.LastOutboxID
			response.Replication.LastDispatchedOutboxID = summary.LastDispatchedOutboxID
			response.Replication.LastFailedOutboxID = summary.LastFailedOutboxID
			response.Replication.LastFailureAttempt = summary.LastFailureAttempt
			response.Replication.NextRetryAt = summary.NextRetryAt
			response.Replication.LastError = summary.LastError
			if summary.OldestPendingCreatedAt != nil {
				lagSeconds := int64(time.Since(*summary.OldestPendingCreatedAt).Seconds())
				if lagSeconds < 0 {
					lagSeconds = 0
				}
				response.Replication.LagSeconds = &lagSeconds
			}
		}
	}

	if h.offsets != nil && sourceNodeID != "" && targetNodeID != "" {
		offset, err := h.offsets.Get(r.Context(), sourceNodeID, targetNodeID)
		if err != nil {
			if !errors.Is(err, replication.ErrOffsetNotFound) {
				h.logger.Error("failed to load replication offset",
					zap.String("source_node_id", sourceNodeID),
					zap.String("target_node_id", targetNodeID),
					zap.Error(err))
				h.writeError(w, http.StatusInternalServerError, "Failed to load replication offset")
				return
			}
		} else {
			response.Replication.LastAppliedOutboxID = &offset.LastAppliedOutboxID
		}
	}

	if h.reconcileStore != nil && sourceNodeID != "" && targetNodeID != "" {
		job, err := h.reconcileStore.GetLatestJob(r.Context(), sourceNodeID, targetNodeID)
		if err != nil {
			if !errors.Is(err, replication.ErrReconcileJobNotFound) {
				h.logger.Error("failed to load reconcile status",
					zap.String("source_node_id", sourceNodeID),
					zap.String("target_node_id", targetNodeID),
					zap.Error(err))
				h.writeError(w, http.StatusInternalServerError, "Failed to load reconcile status")
				return
			}
		} else {
			response.Reconcile = &internalReconcileJobStatus{
				Enabled:           true,
				LatestJobID:       job.ID,
				Status:            job.Status,
				WatermarkOutboxID: job.WatermarkOutboxID,
				ScannedItems:      job.ScannedItems,
				PendingItems:      job.PendingItems,
				StartedAt:         job.StartedAt,
				CompletedAt:       job.CompletedAt,
				LastError:         job.LastError,
			}
		}
	}

	response.Replication.State = h.determineState(response.Replication)
	response.Replication.Notes = h.buildNotes(response.Replication)
	h.writeJSON(w, http.StatusOK, response)
}

type internalReconcileStartRequest struct {
	TargetNodeID string `json:"targetNodeId,omitempty"`
}

type internalReconcileStartResponse struct {
	Success           bool   `json:"success"`
	JobID             int64  `json:"jobId"`
	SourceNodeID      string `json:"sourceNodeId"`
	TargetNodeID      string `json:"targetNodeId"`
	WatermarkOutboxID int64  `json:"watermarkOutboxId"`
	ScannedItems      int64  `json:"scannedItems"`
	PendingItems      int64  `json:"pendingItems"`
	Status            string `json:"status"`
}

// HandleReconcileStart scans active local data and persists a reconcile job with pending items.
func (h *InternalReplicationHandler) HandleReconcileStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !strings.EqualFold(strings.TrimSpace(h.config.Node.Role), "active") {
		h.writeError(w, http.StatusConflict, "reconcile start is only available on active nodes")
		return
	}
	if h.reconcileStore == nil || h.reconcileScanner == nil {
		h.writeError(w, http.StatusConflict, "reconcile components are not configured")
		return
	}

	var req internalReconcileStartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	targetNodeID := strings.TrimSpace(req.TargetNodeID)
	if targetNodeID == "" {
		targetNodeID = strings.TrimSpace(h.config.Internal.Replication.PeerNodeID)
	}
	resp, err := h.startReconcile(r.Context(), targetNodeID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

// RunStartupReconcile tries to trigger one full historical reconcile after startup.
func (h *InternalReplicationHandler) RunStartupReconcile(ctx context.Context) {
	if !strings.EqualFold(strings.TrimSpace(h.config.Node.Role), "active") {
		return
	}
	targetNodeID := strings.TrimSpace(h.config.Internal.Replication.PeerNodeID)
	if targetNodeID == "" || strings.TrimSpace(h.config.Internal.Replication.PeerBaseURL) == "" {
		return
	}
	if h.reconcileStore == nil || h.reconcileScanner == nil {
		return
	}

	const maxAttempts = 24
	const retryInterval = 5 * time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := h.startReconcile(ctx, targetNodeID)
		if err == nil {
			h.logger.Info("startup reconcile finished",
				zap.Int64("job_id", resp.JobID),
				zap.String("target_node_id", resp.TargetNodeID),
				zap.Int64("scanned_items", resp.ScannedItems),
				zap.Int64("pending_items", resp.PendingItems))
			return
		}

		h.logger.Warn("startup reconcile attempt failed, will retry",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAttempts),
			zap.String("target_node_id", targetNodeID),
			zap.Error(err))

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	h.logger.Warn("startup reconcile stopped after max retry attempts",
		zap.Int("max_attempts", maxAttempts),
		zap.String("target_node_id", targetNodeID))
}

func (h *InternalReplicationHandler) startReconcile(ctx context.Context, targetNodeID string) (*internalReconcileStartResponse, error) {
	targetNodeID = strings.TrimSpace(targetNodeID)
	if targetNodeID == "" {
		targetNodeID = strings.TrimSpace(h.config.Internal.Replication.PeerNodeID)
	}
	if targetNodeID == "" {
		return nil, fmt.Errorf("targetNodeId is required")
	}

	watermarkOutboxID := int64(0)
	if h.outbox != nil {
		summary, err := h.outbox.GetStatusSummary(ctx, h.config.Node.ID, targetNodeID)
		if err != nil {
			return nil, fmt.Errorf("load outbox status: %w", err)
		}
		if summary != nil && summary.LastOutboxID != nil {
			watermarkOutboxID = *summary.LastOutboxID
		}
	}

	job := &replication.ReconcileJob{
		SourceNodeID:      h.config.Node.ID,
		TargetNodeID:      targetNodeID,
		WatermarkOutboxID: watermarkOutboxID,
		Status:            replication.ReconcileJobStatusRunning,
		StartedAt:         time.Now(),
	}
	if err := h.reconcileStore.CreateJob(ctx, job); err != nil {
		return nil, fmt.Errorf("create reconcile job: %w", err)
	}

	items, err := h.reconcileScanner.Scan(ctx)
	if err != nil {
		lastErr := err.Error()
		_ = h.reconcileStore.UpdateJobResult(ctx, job.ID, replication.ReconcileJobStatusFailed, 0, 0, nil, &lastErr)
		return nil, fmt.Errorf("scan local data for reconcile: %w", err)
	}

	if err := h.reconcileStore.ReplaceItems(ctx, job.ID, items); err != nil {
		lastErr := err.Error()
		_ = h.reconcileStore.UpdateJobResult(ctx, job.ID, replication.ReconcileJobStatusFailed, int64(len(items)), int64(len(items)), nil, &lastErr)
		return nil, fmt.Errorf("persist reconcile items: %w", err)
	}

	scannedItems := int64(len(items))
	pendingItems := scannedItems
	if pendingItems > 0 && strings.TrimSpace(h.config.Internal.Replication.PeerBaseURL) != "" {
		remaining, dispatchErr := h.dispatchReconcilePendingItems(ctx, job.ID, targetNodeID)
		if dispatchErr != nil {
			lastErr := dispatchErr.Error()
			_ = h.reconcileStore.UpdateJobResult(
				ctx,
				job.ID,
				replication.ReconcileJobStatusFailed,
				scannedItems,
				remaining,
				nil,
				&lastErr,
			)
			return nil, fmt.Errorf("dispatch reconcile items: %w", dispatchErr)
		}
		pendingItems = remaining
	}

	completedAt := time.Now()
	if err := h.reconcileStore.UpdateJobResult(
		ctx,
		job.ID,
		replication.ReconcileJobStatusReady,
		scannedItems,
		pendingItems,
		&completedAt,
		nil,
	); err != nil {
		return nil, fmt.Errorf("finalize reconcile job: %w", err)
	}

	return &internalReconcileStartResponse{
		Success:           true,
		JobID:             job.ID,
		SourceNodeID:      job.SourceNodeID,
		TargetNodeID:      job.TargetNodeID,
		WatermarkOutboxID: watermarkOutboxID,
		ScannedItems:      scannedItems,
		PendingItems:      pendingItems,
		Status:            replication.ReconcileJobStatusReady,
	}, nil
}

func (h *InternalReplicationHandler) replicationPair() (string, string) {
	peerNodeID := h.config.Internal.Replication.PeerNodeID
	if strings.EqualFold(strings.TrimSpace(h.config.Node.Role), "standby") {
		return peerNodeID, h.config.Node.ID
	}
	return h.config.Node.ID, peerNodeID
}

func (h *InternalReplicationHandler) workerEnabled() bool {
	replCfg := h.config.Internal.Replication
	return replCfg.Enabled &&
		strings.EqualFold(strings.TrimSpace(h.config.Node.Role), "active") &&
		strings.TrimSpace(replCfg.PeerNodeID) != "" &&
		strings.TrimSpace(replCfg.PeerBaseURL) != ""
}

func (h *InternalReplicationHandler) determineState(status internalReplicationStatus) string {
	if !status.Enabled {
		return "disabled"
	}

	role := strings.ToLower(strings.TrimSpace(h.config.Node.Role))
	hasPending := status.PendingEvents != nil && *status.PendingEvents > 0
	hasFailed := status.FailedEvents != nil && *status.FailedEvents > 0
	hasOutboxHistory := status.LastOutboxID != nil && *status.LastOutboxID > 0
	hasApplyHistory := status.LastAppliedOutboxID != nil

	switch role {
	case "active":
		if hasFailed {
			return "retrying"
		}
		if hasPending {
			return "dispatching"
		}
		return "idle"
	case "standby":
		if !hasApplyHistory && hasOutboxHistory {
			return "bootstrap_required"
		}
		if hasFailed {
			return "retrying"
		}
		if hasPending {
			return "catching_up"
		}
		if hasApplyHistory || hasOutboxHistory {
			return "caught_up"
		}
		return "idle"
	default:
		if hasFailed {
			return "retrying"
		}
		if hasPending {
			return "active"
		}
		return "idle"
	}
}

func (h *InternalReplicationHandler) buildNotes(status internalReplicationStatus) []string {
	if !status.Enabled {
		return []string{"internal replication is disabled"}
	}

	role := strings.ToLower(strings.TrimSpace(h.config.Node.Role))
	notes := make([]string, 0, 3)
	switch role {
	case "active":
		notes = append(notes, "only active nodes dispatch internal replication events")
		if !status.WorkerEnabled {
			notes = append(notes, "replication worker is disabled until peer_node_id and peer_base_url are configured")
		}
	case "standby":
		notes = append(notes, "standby nodes only accept internal replication apply traffic")
		if status.LastAppliedOutboxID == nil && status.LastOutboxID != nil && *status.LastOutboxID > 0 {
			notes = append(notes, "standby offset is not initialized; finish baseline copy before promotion")
		}
	}
	if status.FailedEvents != nil && *status.FailedEvents > 0 {
		notes = append(notes, "pendingEvents includes events currently waiting for retry")
	}

	return notes
}

func (h *InternalReplicationHandler) writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("failed to encode response", zap.Error(err))
	}
}

func (h *InternalReplicationHandler) writeError(w http.ResponseWriter, code int, message string) {
	h.writeJSON(w, code, map[string]interface{}{
		"error":   message,
		"code":    code,
		"success": false,
	})
}
