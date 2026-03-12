package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/replication"
	"github.com/yeying-community/warehouse/internal/interface/http/middleware"
	"go.uber.org/zap"
)

type internalReplicationFSApplyRequest struct {
	OutboxID int64  `json:"outboxId"`
	Op       string `json:"op"`
	Path     string `json:"path,omitempty"`
	FromPath string `json:"fromPath,omitempty"`
	ToPath   string `json:"toPath,omitempty"`
	IsDir    bool   `json:"isDir"`
}

type internalReplicationApplyResponse struct {
	Success             bool   `json:"success"`
	OutboxID            int64  `json:"outboxId"`
	AlreadyApplied      bool   `json:"alreadyApplied,omitempty"`
	LastAppliedOutboxID int64  `json:"lastAppliedOutboxId"`
	Operation           string `json:"operation,omitempty"`
	Path                string `json:"path,omitempty"`
}

type replicationSequenceConflictError struct {
	ExpectedNextOutboxID int64
	LastAppliedOutboxID  int64
}

func (e *replicationSequenceConflictError) Error() string {
	return fmt.Sprintf("expected next outbox id %d, got a later event after %d", e.ExpectedNextOutboxID, e.LastAppliedOutboxID)
}

func (h *InternalReplicationHandler) HandleFSApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if err := h.requireStandbyRole(); err != nil {
		h.writeError(w, http.StatusConflict, err.Error())
		return
	}

	var req internalReplicationFSApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.OutboxID <= 0 {
		h.writeError(w, http.StatusBadRequest, "outboxId must be greater than zero")
		return
	}

	sourceNodeID := strings.TrimSpace(r.Header.Get(middleware.InternalNodeIDHeader))
	state, err := h.beginApply(r.Context(), sourceNodeID, req.OutboxID)
	if err != nil {
		h.writeApplyError(w, err)
		return
	}
	if state.alreadyApplied {
		h.writeJSON(w, http.StatusOK, internalReplicationApplyResponse{
			Success:             true,
			OutboxID:            req.OutboxID,
			AlreadyApplied:      true,
			LastAppliedOutboxID: state.lastAppliedOutboxID,
			Operation:           req.Op,
			Path:                req.Path,
		})
		return
	}

	if err := h.applyFSOperation(req); err != nil {
		h.logger.Error("failed to apply replication fs operation",
			zap.String("source_node_id", sourceNodeID),
			zap.Int64("outbox_id", req.OutboxID),
			zap.String("op", req.Op),
			zap.String("path", req.Path),
			zap.String("from_path", req.FromPath),
			zap.String("to_path", req.ToPath),
			zap.Error(err))
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.markApplied(r.Context(), sourceNodeID, req.OutboxID); err != nil {
		h.logger.Error("failed to update replication offset after fs apply",
			zap.String("source_node_id", sourceNodeID),
			zap.Int64("outbox_id", req.OutboxID),
			zap.Error(err))
		h.writeError(w, http.StatusInternalServerError, "Failed to update replication offset")
		return
	}

	h.writeJSON(w, http.StatusOK, internalReplicationApplyResponse{
		Success:             true,
		OutboxID:            req.OutboxID,
		LastAppliedOutboxID: req.OutboxID,
		Operation:           req.Op,
		Path:                firstNonEmpty(req.Path, req.ToPath),
	})
}

func (h *InternalReplicationHandler) HandleFileApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if err := h.requireStandbyRole(); err != nil {
		h.writeError(w, http.StatusConflict, err.Error())
		return
	}

	query := r.URL.Query()
	outboxID, err := strconv.ParseInt(strings.TrimSpace(query.Get("outboxId")), 10, 64)
	if err != nil || outboxID <= 0 {
		h.writeError(w, http.StatusBadRequest, "invalid outboxId")
		return
	}
	storagePath := strings.TrimSpace(query.Get("path"))
	if storagePath == "" {
		h.writeError(w, http.StatusBadRequest, "path is required")
		return
	}
	fileSize, err := strconv.ParseInt(strings.TrimSpace(query.Get("fileSize")), 10, 64)
	if err != nil || fileSize < 0 {
		h.writeError(w, http.StatusBadRequest, "invalid fileSize")
		return
	}
	expectedHash := strings.TrimSpace(r.Header.Get(middleware.InternalContentSHA256Header))
	if expectedHash == "" || strings.EqualFold(expectedHash, "UNSIGNED-PAYLOAD") {
		h.writeError(w, http.StatusBadRequest, "content sha256 header is required")
		return
	}
	if _, err := hex.DecodeString(expectedHash); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid content sha256 header")
		return
	}

	sourceNodeID := strings.TrimSpace(r.Header.Get(middleware.InternalNodeIDHeader))
	state, err := h.beginApply(r.Context(), sourceNodeID, outboxID)
	if err != nil {
		h.writeApplyError(w, err)
		return
	}
	if state.alreadyApplied {
		h.writeJSON(w, http.StatusOK, internalReplicationApplyResponse{
			Success:             true,
			OutboxID:            outboxID,
			AlreadyApplied:      true,
			LastAppliedOutboxID: state.lastAppliedOutboxID,
			Operation:           replication.OpUpsertFile,
			Path:                storagePath,
		})
		return
	}

	fullPath, err := h.resolveReplicaPath(storagePath)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	alreadyMatches, err := fileMatchesDigest(fullPath, fileSize, expectedHash)
	if err != nil {
		h.logger.Error("failed to compare existing file digest",
			zap.Int64("outbox_id", outboxID),
			zap.String("path", storagePath),
			zap.Error(err))
		h.writeError(w, http.StatusInternalServerError, "Failed to compare existing file digest")
		return
	}
	if !alreadyMatches {
		if err := h.applyFile(fullPath, r.Body, fileSize, expectedHash); err != nil {
			h.logger.Error("failed to apply replication file",
				zap.String("source_node_id", sourceNodeID),
				zap.Int64("outbox_id", outboxID),
				zap.String("path", storagePath),
				zap.Int64("file_size", fileSize),
				zap.Error(err))
			h.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if err := h.markApplied(r.Context(), sourceNodeID, outboxID); err != nil {
		h.logger.Error("failed to update replication offset after file apply",
			zap.String("source_node_id", sourceNodeID),
			zap.Int64("outbox_id", outboxID),
			zap.Error(err))
		h.writeError(w, http.StatusInternalServerError, "Failed to update replication offset")
		return
	}

	h.writeJSON(w, http.StatusOK, internalReplicationApplyResponse{
		Success:             true,
		OutboxID:            outboxID,
		LastAppliedOutboxID: outboxID,
		Operation:           replication.OpUpsertFile,
		Path:                storagePath,
	})
}

func (h *InternalReplicationHandler) requireStandbyRole() error {
	if strings.ToLower(strings.TrimSpace(h.config.Node.Role)) != "standby" {
		return fmt.Errorf("replication apply endpoints are only available on standby nodes")
	}
	if h.offsets == nil {
		return fmt.Errorf("replication offset store is not configured")
	}
	return nil
}

type applyState struct {
	lastAppliedOutboxID int64
	alreadyApplied      bool
}

func (h *InternalReplicationHandler) beginApply(ctx context.Context, sourceNodeID string, outboxID int64) (*applyState, error) {
	sourceNodeID = strings.TrimSpace(sourceNodeID)
	if sourceNodeID == "" {
		return nil, fmt.Errorf("missing %s", middleware.InternalNodeIDHeader)
	}
	offset, err := h.offsets.Get(ctx, sourceNodeID, h.config.Node.ID)
	if err != nil {
		if !errors.Is(err, replication.ErrOffsetNotFound) {
			return nil, err
		}
		offset = &replication.Offset{}
	}
	lastApplied := offset.LastAppliedOutboxID
	if outboxID <= lastApplied {
		return &applyState{lastAppliedOutboxID: lastApplied, alreadyApplied: true}, nil
	}
	expectedNext := lastApplied + 1
	if outboxID != expectedNext {
		return nil, &replicationSequenceConflictError{
			ExpectedNextOutboxID: expectedNext,
			LastAppliedOutboxID:  lastApplied,
		}
	}
	return &applyState{lastAppliedOutboxID: lastApplied}, nil
}

func (h *InternalReplicationHandler) markApplied(ctx context.Context, sourceNodeID string, outboxID int64) error {
	return h.offsets.Upsert(ctx, &replication.Offset{
		SourceNodeID:        sourceNodeID,
		TargetNodeID:        h.config.Node.ID,
		LastAppliedOutboxID: outboxID,
		LastAppliedAt:       time.Now(),
		UpdatedAt:           time.Now(),
	})
}

func (h *InternalReplicationHandler) applyFSOperation(req internalReplicationFSApplyRequest) error {
	switch req.Op {
	case replication.OpEnsureDir:
		fullPath, err := h.resolveReplicaPath(req.Path)
		if err != nil {
			return err
		}
		if fullPath == h.webdavRoot() {
			return nil
		}
		return os.MkdirAll(fullPath, 0755)
	case replication.OpRemovePath:
		fullPath, err := h.resolveReplicaPath(req.Path)
		if err != nil {
			return err
		}
		if fullPath == h.webdavRoot() {
			return fmt.Errorf("refusing to remove webdav root")
		}
		if err := os.RemoveAll(fullPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	case replication.OpMovePath:
		return h.applyMove(req.FromPath, req.ToPath)
	case replication.OpCopyPath:
		return h.applyCopy(req.FromPath, req.ToPath)
	default:
		return fmt.Errorf("unsupported fs apply operation %q", req.Op)
	}
}

func (h *InternalReplicationHandler) applyMove(fromPath, toPath string) error {
	sourcePath, err := h.resolveReplicaPath(fromPath)
	if err != nil {
		return err
	}
	targetPath, err := h.resolveReplicaPath(toPath)
	if err != nil {
		return err
	}
	if sourcePath == h.webdavRoot() || targetPath == h.webdavRoot() {
		return fmt.Errorf("refusing to move webdav root")
	}
	if _, err := os.Stat(sourcePath); err != nil {
		if os.IsNotExist(err) {
			if _, targetErr := os.Stat(targetPath); targetErr == nil {
				return nil
			}
			return fmt.Errorf("source path does not exist: %s", fromPath)
		}
		return err
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		if err := os.RemoveAll(targetPath); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(sourcePath, targetPath)
}

func (h *InternalReplicationHandler) applyCopy(fromPath, toPath string) error {
	sourcePath, err := h.resolveReplicaPath(fromPath)
	if err != nil {
		return err
	}
	targetPath, err := h.resolveReplicaPath(toPath)
	if err != nil {
		return err
	}
	if sourcePath == h.webdavRoot() || targetPath == h.webdavRoot() {
		return fmt.Errorf("refusing to copy webdav root")
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, targetErr := os.Stat(targetPath); targetErr == nil {
				return nil
			}
		}
		return err
	}
	if err := os.RemoveAll(targetPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if info.IsDir() {
		return copyDirectory(sourcePath, targetPath)
	}
	return copyFile(sourcePath, targetPath, info.Mode())
}

func (h *InternalReplicationHandler) applyFile(fullPath string, body io.Reader, fileSize int64, expectedHash string) error {
	if fullPath == h.webdavRoot() {
		return fmt.Errorf("refusing to overwrite webdav root")
	}
	parentDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return fmt.Errorf("create target parent directory: %w", err)
	}
	tmpFile, err := os.CreateTemp(parentDir, ".warehouse-repl-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmpFile, hasher), body)
	if closeErr := tmpFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if written != fileSize {
		return fmt.Errorf("file size mismatch: expected %d bytes, wrote %d", fileSize, written)
	}
	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(actualHash, expectedHash) {
		return fmt.Errorf("file sha256 mismatch")
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

func fileMatchesDigest(fullPath string, expectedSize int64, expectedHash string) (bool, error) {
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if info.IsDir() || info.Size() != expectedSize {
		return false, nil
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return false, err
	}
	return strings.EqualFold(hex.EncodeToString(hasher.Sum(nil)), expectedHash), nil
}

func (h *InternalReplicationHandler) resolveReplicaPath(raw string) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(raw))
	if cleaned == "/" {
		return h.webdavRoot(), nil
	}
	if strings.HasPrefix(cleaned, "/..") {
		return "", fmt.Errorf("invalid path %q", raw)
	}
	return filepath.Join(h.webdavRoot(), filepath.FromSlash(strings.TrimPrefix(cleaned, "/"))), nil
}

func (h *InternalReplicationHandler) webdavRoot() string {
	return filepath.Clean(h.config.WebDAV.Directory)
}

func (h *InternalReplicationHandler) writeApplyError(w http.ResponseWriter, err error) {
	var conflict *replicationSequenceConflictError
	if errors.As(err, &conflict) {
		h.writeJSON(w, http.StatusConflict, map[string]any{
			"error":                err.Error(),
			"code":                 http.StatusConflict,
			"success":              false,
			"expectedNextOutboxId": conflict.ExpectedNextOutboxID,
			"lastAppliedOutboxId":  conflict.LastAppliedOutboxID,
		})
		return
	}
	h.writeError(w, http.StatusBadRequest, err.Error())
}

func copyDirectory(sourcePath, targetPath string) error {
	info, err := os.Stat(sourcePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(targetPath, info.Mode()); err != nil {
		return err
	}
	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		from := filepath.Join(sourcePath, entry.Name())
		to := filepath.Join(targetPath, entry.Name())
		entryInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if entryInfo.IsDir() {
			if err := copyDirectory(from, to); err != nil {
				return err
			}
			continue
		}
		if err := copyFile(from, to, entryInfo.Mode()); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(sourcePath, targetPath string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	tmpFile, err := os.CreateTemp(filepath.Dir(targetPath), ".warehouse-copy-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := io.Copy(tmpFile, source); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Chmod(mode); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, targetPath)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
