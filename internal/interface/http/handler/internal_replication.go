package handler

import (
	"context"
	"encoding/json"
	"errors"
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

// InternalReplicationHandler exposes replication-related internal control endpoints.
type InternalReplicationHandler struct {
	logger  *zap.Logger
	config  *config.Config
	outbox  replicationOutboxStatusReader
	offsets replicationOffsetStore
}

// NewInternalReplicationHandler creates a new internal replication handler.
func NewInternalReplicationHandler(
	cfg *config.Config,
	logger *zap.Logger,
	outbox replicationOutboxStatusReader,
	offsets replicationOffsetStore,
) *InternalReplicationHandler {
	return &InternalReplicationHandler{
		logger:  logger,
		config:  cfg,
		outbox:  outbox,
		offsets: offsets,
	}
}

type internalReplicationStatusResponse struct {
	Node        internalNodeStatus        `json:"node"`
	Replication internalReplicationStatus `json:"replication"`
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

	response.Replication.State = h.determineState(response.Replication)
	response.Replication.Notes = h.buildNotes(response.Replication)
	h.writeJSON(w, http.StatusOK, response)
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
