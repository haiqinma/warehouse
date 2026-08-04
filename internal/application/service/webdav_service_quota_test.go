package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yeying-community/warehouse/internal/domain/permission"
	"github.com/yeying-community/warehouse/internal/domain/quota"
	"github.com/yeying-community/warehouse/internal/domain/user"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/interface/http/middleware"
	"go.uber.org/zap"
)

func TestCheckQuotaRejectsNewFileWhenQuotaExceeded(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 100, 95)

	req := httptest.NewRequest(http.MethodPut, "/dav/personal/new.txt", strings.NewReader("1234567890"))
	req.Header.Set("Content-Length", "10")

	err := svc.checkQuota(context.Background(), u, req)
	if err == nil {
		t.Fatal("expected quota error, got nil")
	}
	if err != user.ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}

func TestCheckQuotaAllowsOverwriteWhenOnlyDeltaFits(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 100, 95)

	userDir := svc.getUserDirectory(u)
	targetPath := filepath.Join(userDir, "personal", "exists.txt")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		t.Fatalf("mkdir target dir: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatalf("seed target file: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/dav/personal/exists.txt", strings.NewReader("123456789012"))
	req.Header.Set("Content-Length", "12")

	if err := svc.checkQuota(context.Background(), u, req); err != nil {
		t.Fatalf("expected overwrite quota check to pass, got %v", err)
	}
}

func TestCheckQuotaRejectsCopyWhenQuotaExceeded(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 100, 95)

	userDir := svc.getUserDirectory(u)
	sourcePath := filepath.Join(userDir, "personal", "source.txt")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("mkdir source dir: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("1234567890"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}

	req := httptest.NewRequest("COPY", "/dav/personal/source.txt", nil)
	req.Header.Set("Destination", "/dav/personal/copied.txt")

	err := svc.checkQuota(context.Background(), u, req)
	if err == nil {
		t.Fatal("expected quota error for copy, got nil")
	}
	if err != user.ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}

func TestCheckQuotaAllowsCopyOverwriteWhenOnlyDeltaFits(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 100, 95)

	userDir := svc.getUserDirectory(u)
	sourcePath := filepath.Join(userDir, "personal", "source.txt")
	targetPath := filepath.Join(userDir, "personal", "target.txt")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("mkdir personal dir: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("123456789012"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatalf("seed target file: %v", err)
	}

	req := httptest.NewRequest("COPY", "/dav/personal/source.txt", nil)
	req.Header.Set("Destination", "/dav/personal/target.txt")

	if err := svc.checkQuota(context.Background(), u, req); err != nil {
		t.Fatalf("expected copy overwrite quota check to pass, got %v", err)
	}
}

func TestCheckQuotaRejectsDirectoryCopyWhenTargetHasUnrelatedLargeFile(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 100, 95)

	userDir := svc.getUserDirectory(u)
	sourceFile := filepath.Join(userDir, "personal", "source", "new.txt")
	targetExisting := filepath.Join(userDir, "personal", "target", "unrelated.bin")
	if err := os.MkdirAll(filepath.Dir(sourceFile), 0o755); err != nil {
		t.Fatalf("mkdir source dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(targetExisting), 0o755); err != nil {
		t.Fatalf("mkdir target dir: %v", err)
	}
	if err := os.WriteFile(sourceFile, []byte("1234567890"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := os.WriteFile(targetExisting, []byte("12345678901234567890"), 0o644); err != nil {
		t.Fatalf("seed target existing file: %v", err)
	}

	req := httptest.NewRequest("COPY", "/dav/personal/source", nil)
	req.Header.Set("Destination", "/dav/personal/target")

	err := svc.checkQuota(context.Background(), u, req)
	if err == nil {
		t.Fatal("expected quota error for directory copy delta, got nil")
	}
	if err != user.ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}

func TestWebDAVServeHTTPDirectoryCopyUpdatesUsedSpaceByRelativeDelta(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 200, 50)

	userDir := svc.getUserDirectory(u)
	sourceFile := filepath.Join(userDir, "personal", "source", "new.txt")
	targetExisting := filepath.Join(userDir, "personal", "target", "unrelated.bin")
	if err := os.MkdirAll(filepath.Dir(sourceFile), 0o755); err != nil {
		t.Fatalf("mkdir source dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(targetExisting), 0o755); err != nil {
		t.Fatalf("mkdir target dir: %v", err)
	}
	if err := os.WriteFile(sourceFile, []byte("1234567890"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := os.WriteFile(targetExisting, []byte("12345678901234567890"), 0o644); err != nil {
		t.Fatalf("seed target existing file: %v", err)
	}

	req := httptest.NewRequest("COPY", "/dav/personal/source", nil)
	req.Header.Set("Destination", "/dav/personal/target")
	req = req.WithContext(context.WithValue(req.Context(), middleware.UserContextKey, u))
	resp := httptest.NewRecorder()

	svc.ServeHTTP(resp, req)

	if resp.Code < 200 || resp.Code >= 300 {
		t.Fatalf("expected directory COPY to succeed, got status=%d body=%q", resp.Code, resp.Body.String())
	}
	if u.UsedSpace != 60 {
		t.Fatalf("expected used space to become 60, got %d", u.UsedSpace)
	}
	if _, err := os.Stat(filepath.Join(userDir, "personal", "target", "new.txt")); err != nil {
		t.Fatalf("expected copied file to exist: %v", err)
	}
}

func TestWebDAVServeHTTPOverwriteUpdatesUsedSpaceByDelta(t *testing.T) {
	t.Parallel()

	svc, u := newQuotaTestService(t, 100, 95)

	userDir := svc.getUserDirectory(u)
	targetPath := filepath.Join(userDir, "personal", "exists.txt")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		t.Fatalf("mkdir target dir: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatalf("seed target file: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/dav/personal/exists.txt", strings.NewReader("123456789012"))
	req.Header.Set("Content-Length", "12")
	req = req.WithContext(context.WithValue(req.Context(), middleware.UserContextKey, u))
	resp := httptest.NewRecorder()

	svc.ServeHTTP(resp, req)

	if resp.Code < 200 || resp.Code >= 300 {
		t.Fatalf("expected overwrite PUT to succeed, got status=%d body=%q", resp.Code, resp.Body.String())
	}
	if u.UsedSpace != 97 {
		t.Fatalf("expected used space to become 97, got %d", u.UsedSpace)
	}
}

func newQuotaTestService(t *testing.T, quotaBytes, usedBytes int64) (*WebDAVService, *user.User) {
	t.Helper()

	rootDir := t.TempDir()
	cfg := &config.Config{
		WebDAV: config.WebDAVConfig{
			Prefix:              "/dav",
			Directory:           rootDir,
			AutoCreateDirectory: true,
			NoSniff:             true,
		},
	}

	userRepo := newTestUserRepo()
	u := user.NewUser("alice", "alice")
	u.Permissions = user.FullPermissions()
	u.Quota = quotaBytes
	u.UsedSpace = usedBytes
	if err := userRepo.Save(context.Background(), u); err != nil {
		t.Fatalf("save user: %v", err)
	}

	svc := NewWebDAVService(
		cfg,
		allowPermissionChecker{},
		quota.NewService(userRepo),
		userRepo,
		&testRecycleRepo{},
		nil,
		nil,
		zap.NewNop(),
	)
	return svc, u
}

var _ permission.Checker = allowPermissionChecker{}
