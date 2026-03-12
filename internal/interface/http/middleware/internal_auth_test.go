package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

func TestInternalAuthMiddlewareAllowsValidRequest(t *testing.T) {
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	cfg := config.InternalReplicationConfig{
		Enabled:          true,
		SharedSecret:     "shared-secret",
		AllowedClockSkew: time.Minute,
	}
	middleware := NewInternalAuthMiddleware(cfg, zap.NewNop())
	middleware.now = func() time.Time { return now }
	if middleware.config.SharedSecret != cfg.SharedSecret {
		t.Fatalf("unexpected shared secret in middleware: got=%q want=%q", middleware.config.SharedSecret, cfg.SharedSecret)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/replication/status", nil)
	timestamp := now.Format(time.RFC3339)
	req.Header.Set(InternalNodeIDHeader, "node-a")
	req.Header.Set(InternalTimestampHeader, timestamp)
	req.Header.Set(InternalSignatureHeader, SignInternalRequest(req.Method, req.URL.Path, "node-a", timestamp, "", cfg.SharedSecret))
	expected := SignInternalRequest(
		req.Method,
		req.URL.Path,
		req.Header.Get(InternalNodeIDHeader),
		req.Header.Get(InternalTimestampHeader),
		req.Header.Get(InternalContentSHA256Header),
		cfg.SharedSecret,
	)
	if got := req.Header.Get(InternalSignatureHeader); got != expected {
		t.Fatalf("unexpected signature setup: got=%q expected=%q path=%q method=%q", got, expected, req.URL.Path, req.Method)
	}
	if err := middleware.validateRequest(req); err != nil {
		t.Fatalf("expected request to validate, got error: %v", err)
	}

	recorder := httptest.NewRecorder()
	nextCalled := false
	handler := middleware.Handle(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(recorder, req)

	if !nextCalled {
		t.Fatalf("expected next handler to be called")
	}
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", recorder.Code)
	}
}

func TestInternalAuthMiddlewareRejectsStaleTimestamp(t *testing.T) {
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	cfg := config.InternalReplicationConfig{
		Enabled:          true,
		SharedSecret:     "shared-secret",
		AllowedClockSkew: time.Minute,
	}
	middleware := NewInternalAuthMiddleware(cfg, zap.NewNop())
	middleware.now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/replication/status", nil)
	timestamp := now.Add(-2 * time.Minute).Format(time.RFC3339)
	req.Header.Set(InternalNodeIDHeader, "node-a")
	req.Header.Set(InternalTimestampHeader, timestamp)
	req.Header.Set(InternalSignatureHeader, SignInternalRequest(req.Method, req.URL.Path, "node-a", timestamp, "", cfg.SharedSecret))

	recorder := httptest.NewRecorder()
	handler := middleware.Handle(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", recorder.Code)
	}
}

func TestInternalAuthMiddlewareRejectsBadSignature(t *testing.T) {
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	cfg := config.InternalReplicationConfig{
		Enabled:          true,
		SharedSecret:     "shared-secret",
		AllowedClockSkew: time.Minute,
	}
	middleware := NewInternalAuthMiddleware(cfg, zap.NewNop())
	middleware.now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/replication/status", nil)
	req.Header.Set(InternalNodeIDHeader, "node-a")
	req.Header.Set(InternalTimestampHeader, now.Format(time.RFC3339))
	req.Header.Set(InternalSignatureHeader, "bad-signature")

	recorder := httptest.NewRecorder()
	handler := middleware.Handle(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", recorder.Code)
	}
}
