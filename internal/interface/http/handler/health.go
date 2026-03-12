package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	apphealth "github.com/yeying-community/warehouse/internal/health"
	"go.uber.org/zap"
)

type readinessChecker interface {
	Check(ctx context.Context) apphealth.Result
}

// HealthHandler 健康检查处理器
type HealthHandler struct {
	logger           *zap.Logger
	startTime        time.Time
	readiness        readinessChecker
	readinessTimeout time.Duration
}

// NewHealthHandler 创建健康检查处理器
func NewHealthHandler(logger *zap.Logger, readiness readinessChecker) *HealthHandler {
	return &HealthHandler{
		logger:           logger,
		startTime:        time.Now(),
		readiness:        readiness,
		readinessTimeout: 5 * time.Second,
	}
}

// HealthResponse 健康检查响应
type HealthResponse struct {
	Status  string        `json:"status"`
	Uptime  time.Duration `json:"uptime"`
	Version string        `json:"version"`
}

// Handle 处理健康检查请求
func (h *HealthHandler) Handle(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:  "healthy",
		Uptime:  time.Since(h.startTime),
		Version: "2.0.0",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("failed to encode health response", zap.Error(err))
	}
}

// HandleReadiness returns dependency-aware readiness information.
func (h *HealthHandler) HandleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readinessTimeout)
	defer cancel()

	result := apphealth.Result{
		Status: apphealth.StatusNotReady,
		Checks: []apphealth.CheckResult{{
			Name:   "server",
			Status: apphealth.StatusNotReady,
			Error:  "readiness checker is not configured",
		}},
	}

	if h.readiness != nil {
		result = h.readiness.Check(ctx)
	}

	statusCode := http.StatusOK
	if !result.Ready() {
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.logger.Error("failed to encode readiness response", zap.Error(err))
	}
}
