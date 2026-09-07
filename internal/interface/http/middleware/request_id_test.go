package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDMiddlewarePreservesIncomingID(t *testing.T) {
	var seen string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "client-request-1")
	rec := httptest.NewRecorder()
	RequestIDMiddleware(next).ServeHTTP(rec, req)
	if seen != "client-request-1" || rec.Header().Get("X-Request-ID") != seen {
		t.Fatalf("request id not preserved: seen=%q response=%q", seen, rec.Header().Get("X-Request-ID"))
	}
}

func TestRequestIDMiddlewareGeneratesID(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) == "" {
			t.Fatal("request id missing from context")
		}
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	RequestIDMiddleware(next).ServeHTTP(rec, req)
	id := rec.Header().Get("X-Request-ID")
	if len(id) != 32 {
		t.Fatalf("expected generated 128-bit id, got %q", id)
	}
}
