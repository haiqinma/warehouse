package service

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/yeying-community/warehouse/internal/domain/permission"
	"github.com/yeying-community/warehouse/internal/domain/quota"
	"github.com/yeying-community/warehouse/internal/domain/share"
	"github.com/yeying-community/warehouse/internal/domain/shareuser"
	"github.com/yeying-community/warehouse/internal/domain/user"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/infrastructure/repository"
	"github.com/yeying-community/warehouse/internal/interface/http/middleware"
	"go.uber.org/zap"
)

func TestSyncUserSharePathsForOwnerMoveUpdatesStoragePaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.WebDAV.Directory = root

	owner := user.NewUser("alice", "alice")
	repo := &captureUserShareRepo{}

	fromPath := filepath.Join(root, owner.Username, "personal", "test")
	toPath := filepath.Join(root, owner.Username, "personal", "test_upload")
	if err := SyncUserSharePathsForOwnerMove(context.Background(), repo, cfg, owner, fromPath, toPath); err != nil {
		t.Fatalf("SyncUserSharePathsForOwnerMove: %v", err)
	}

	if repo.ownerID != owner.ID || repo.fromPath != "/personal/test" || repo.toPath != "/personal/test_upload" {
		t.Fatalf("unexpected sync args: owner=%q from=%q to=%q", repo.ownerID, repo.fromPath, repo.toPath)
	}
}

func TestSyncAllSharePathsForOwnerMoveUpdatesDirectedAndPublicShares(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.WebDAV.Directory = root
	owner := user.NewUser("alice", "alice")
	directedRepo := &captureUserShareRepo{}
	publicRepo := &capturePublicShareRepo{}
	fromPath := filepath.Join(root, owner.Username, "personal", "folder_100%")
	toPath := filepath.Join(root, owner.Username, "personal", "renamed")

	if err := SyncAllSharePathsForOwnerMove(context.Background(), directedRepo, publicRepo, cfg, owner, fromPath, toPath); err != nil {
		t.Fatalf("SyncAllSharePathsForOwnerMove: %v", err)
	}
	if directedRepo.fromPath != "/personal/folder_100%" || directedRepo.toPath != "/personal/renamed" {
		t.Fatalf("unexpected directed share paths: from=%q to=%q", directedRepo.fromPath, directedRepo.toPath)
	}
	if publicRepo.fromPath != "/personal/folder_100%" || publicRepo.toPath != "/personal/renamed" {
		t.Fatalf("unexpected public share paths: from=%q to=%q", publicRepo.fromPath, publicRepo.toPath)
	}
}

func TestRemoveAllShareReferencesForOwnerPathRemovesDirectedAndPublicShares(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.WebDAV.Directory = root
	owner := user.NewUser("alice", "alice")
	directedRepo := &captureUserShareRepo{}
	publicRepo := &capturePublicShareRepo{}
	target := filepath.Join(root, owner.Username, "personal", "shared")

	if err := RemoveAllShareReferencesForOwnerPath(context.Background(), directedRepo, publicRepo, cfg, owner, target); err != nil {
		t.Fatalf("RemoveAllShareReferencesForOwnerPath: %v", err)
	}
	if directedRepo.removedPath != "/personal/shared" {
		t.Fatalf("unexpected directed removed path: %q", directedRepo.removedPath)
	}
	if publicRepo.removedPath != "/personal/shared" {
		t.Fatalf("unexpected public removed path: %q", publicRepo.removedPath)
	}
}

func TestWebDAVServeHTTPMoveSyncsSharePaths(t *testing.T) {
	t.Parallel()

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
	if err := userRepo.Save(context.Background(), u); err != nil {
		t.Fatalf("save user: %v", err)
	}

	shareRepo := &captureUserShareRepo{}
	svc := NewWebDAVService(
		cfg,
		allowPermissionChecker{},
		quota.NewService(userRepo),
		userRepo,
		&testRecycleRepo{},
		shareRepo,
		nil,
		zap.NewNop(),
	)

	userDir := svc.getUserDirectory(u)
	if err := os.MkdirAll(filepath.Join(userDir, "personal", "test"), 0o755); err != nil {
		t.Fatalf("mkdir source dir: %v", err)
	}

	req := httptest.NewRequest("MOVE", "/dav/personal/test", nil)
	req.Header.Set("Destination", "/dav/personal/test_upload")
	req = req.WithContext(context.WithValue(req.Context(), middleware.UserContextKey, u))
	resp := httptest.NewRecorder()

	svc.ServeHTTP(resp, req)

	if resp.Code < 200 || resp.Code >= 300 {
		t.Fatalf("expected MOVE to succeed, got status=%d body=%q", resp.Code, resp.Body.String())
	}
	if shareRepo.fromPath != "/personal/test" || shareRepo.toPath != "/personal/test_upload" {
		t.Fatalf("unexpected share sync args: from=%q to=%q", shareRepo.fromPath, shareRepo.toPath)
	}
}

func TestWebDAVServeHTTPDeleteRemovesShareReferences(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	cfg := &config.Config{WebDAV: config.WebDAVConfig{Prefix: "/dav", Directory: rootDir, AutoCreateDirectory: true}}
	userRepo := newTestUserRepo()
	u := user.NewUser("alice", "alice")
	u.Permissions = user.FullPermissions()
	if err := userRepo.Save(context.Background(), u); err != nil {
		t.Fatalf("save user: %v", err)
	}
	shareRepo := &captureUserShareRepo{}
	svc := NewWebDAVService(cfg, allowPermissionChecker{}, quota.NewService(userRepo), userRepo, &testRecycleRepo{}, shareRepo, nil, zap.NewNop())
	userDir := svc.getUserDirectory(u)
	target := filepath.Join(userDir, "personal", "shared")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	req := httptest.NewRequest("DELETE", "/dav/personal/shared", nil)
	req = req.WithContext(context.WithValue(req.Context(), middleware.UserContextKey, u))
	resp := httptest.NewRecorder()
	svc.ServeHTTP(resp, req)
	if resp.Code < 200 || resp.Code >= 300 {
		t.Fatalf("expected DELETE success, got %d: %s", resp.Code, resp.Body.String())
	}
	if shareRepo.removedPath != "/personal/shared" {
		t.Fatalf("unexpected removed share path: %q", shareRepo.removedPath)
	}
}

type captureUserShareRepo struct {
	ownerID     string
	fromPath    string
	toPath      string
	removedPath string
}

func (r *captureUserShareRepo) DeletePathsForOwner(_ context.Context, ownerID, rootPath string) error {
	r.ownerID = ownerID
	r.removedPath = rootPath
	return nil
}

func (*captureUserShareRepo) CreateWithAudiences(context.Context, *shareuser.ShareUserItem, []repository.UserShareAudience) error {
	return nil
}

func (*captureUserShareRepo) GetByID(context.Context, string) (*shareuser.ShareUserItem, error) {
	return nil, shareuser.ErrShareNotFound
}

func (*captureUserShareRepo) GetByOwnerID(context.Context, string) ([]*shareuser.ShareUserItem, error) {
	return nil, nil
}

func (*captureUserShareRepo) GetByTargetID(context.Context, string) ([]*shareuser.ShareUserItem, error) {
	return nil, nil
}

func (r *captureUserShareRepo) UpdatePathsForOwnerMove(_ context.Context, ownerID, fromPath, toPath string) error {
	r.ownerID = ownerID
	r.fromPath = fromPath
	r.toPath = toPath
	return nil
}

func (*captureUserShareRepo) DeleteByID(context.Context, string) error {
	return nil
}

func (*captureUserShareRepo) ListAudiencesByShareID(context.Context, string) ([]repository.UserShareAudience, error) {
	return nil, nil
}

var _ permission.Checker = allowPermissionChecker{}
var _ repository.UserShareRepository = (*captureUserShareRepo)(nil)

type capturePublicShareRepo struct {
	ownerID     string
	fromPath    string
	toPath      string
	removedPath string
}

func (*capturePublicShareRepo) Create(context.Context, *share.ShareItem) error { return nil }
func (*capturePublicShareRepo) GetByToken(context.Context, string) (*share.ShareItem, error) {
	return nil, share.ErrShareNotFound
}
func (*capturePublicShareRepo) GetByUserID(context.Context, string) ([]*share.ShareItem, error) {
	return nil, nil
}
func (*capturePublicShareRepo) DeleteByToken(context.Context, string) error     { return nil }
func (*capturePublicShareRepo) IncrementView(context.Context, string) error     { return nil }
func (*capturePublicShareRepo) IncrementDownload(context.Context, string) error { return nil }
func (r *capturePublicShareRepo) UpdatePathsForOwnerMove(_ context.Context, ownerID, fromPath, toPath string) error {
	r.ownerID = ownerID
	r.fromPath = fromPath
	r.toPath = toPath
	return nil
}
func (r *capturePublicShareRepo) DeletePathsForOwner(_ context.Context, ownerID, rootPath string) error {
	r.ownerID = ownerID
	r.removedPath = rootPath
	return nil
}

var _ repository.ShareRepository = (*capturePublicShareRepo)(nil)
var _ repository.ShareReferenceRepository = (*capturePublicShareRepo)(nil)
