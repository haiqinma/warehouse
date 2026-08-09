package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/group"
	"github.com/yeying-community/warehouse/internal/domain/sharegrant"
	"github.com/yeying-community/warehouse/internal/domain/shareuser"
	"github.com/yeying-community/warehouse/internal/domain/user"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/infrastructure/repository"
	"go.uber.org/zap"
)

func TestUploadSessionServiceCompleteWritesFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	owner := user.NewUser("alice", "alice")
	owner.ID = "user-alice"
	if err := os.MkdirAll(filepath.Join(root, "alice", "personal"), 0o755); err != nil {
		t.Fatal(err)
	}
	recorder := &uploadSessionTestRecorder{}
	svc := NewUploadSessionService(uploadSessionTestConfig(root), nil, nil, nil, nil, recorder, zap.NewNop())

	session, err := svc.Create(context.Background(), owner, UploadSessionCreateInput{Path: "/personal/file.txt", Size: 6, ChunkSize: 4, FileName: "file.txt"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), owner, session.ID, 1, uploadSessionTestChecksum("abcd"), strings.NewReader("abcd")); err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), owner, session.ID, 2, uploadSessionTestChecksum("ef"), strings.NewReader("ef")); err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}
	if _, err := svc.Complete(context.Background(), owner, session.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(root, "alice", "personal", "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("expected abcdef, got %q", string(data))
	}
	if recorder.upsertPath != filepath.Join(root, "alice", "personal", "file.txt") {
		t.Fatalf("expected upsert path recorded, got %q", recorder.upsertPath)
	}
}

func TestUploadSessionServiceCompleteRequiresAllParts(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	owner := user.NewUser("alice", "alice")
	owner.ID = "user-alice"
	if err := os.MkdirAll(filepath.Join(root, "alice", "personal"), 0o755); err != nil {
		t.Fatal(err)
	}
	svc := NewUploadSessionService(uploadSessionTestConfig(root), nil, nil, nil, nil, noopMutationRecorder{}, zap.NewNop())

	session, err := svc.Create(context.Background(), owner, UploadSessionCreateInput{Path: "/personal/file.txt", Size: 6, ChunkSize: 4, FileName: "file.txt"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), owner, session.ID, 1, uploadSessionTestChecksum("abcd"), strings.NewReader("abcd")); err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	if _, err := svc.Complete(context.Background(), owner, session.ID); !errors.Is(err, ErrUploadSessionInvalid) {
		t.Fatalf("expected ErrUploadSessionInvalid, got %v", err)
	}
}

func TestUploadSessionServiceShareCreatePermissionDoesNotOverwriteExistingFile(t *testing.T) {
	t.Parallel()

	svc, target, shareID, sharedDir := newUploadSessionShareFixture(t, "CR")
	existing := filepath.Join(sharedDir, "file.txt")
	if err := os.WriteFile(existing, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := svc.Create(context.Background(), target, UploadSessionCreateInput{
		ShareID:   shareID,
		Path:      "file.txt",
		Size:      6,
		ChunkSize: 3,
		FileName:  "file.txt",
	})
	if !errors.Is(err, ErrUploadSessionForbidden) {
		t.Fatalf("expected ErrUploadSessionForbidden, got %v", err)
	}
	data, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old" {
		t.Fatalf("existing file was overwritten: %q", data)
	}
}

func TestUploadSessionServiceShareUpdatePermissionOverwritesExistingFile(t *testing.T) {
	t.Parallel()

	svc, target, shareID, sharedDir := newUploadSessionShareFixture(t, "CRU")
	existing := filepath.Join(sharedDir, "file.txt")
	if err := os.WriteFile(existing, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	session, err := svc.Create(context.Background(), target, UploadSessionCreateInput{
		ShareID:   shareID,
		Path:      "file.txt",
		Size:      6,
		ChunkSize: 3,
		FileName:  "file.txt",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), target, session.ID, 1, uploadSessionTestChecksum("abc"), strings.NewReader("abc")); err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), target, session.ID, 2, uploadSessionTestChecksum("def"), strings.NewReader("def")); err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}
	if _, err := svc.Complete(context.Background(), target, session.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	data, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("expected overwritten file content, got %q", data)
	}
}

func TestUploadSessionServiceDynamicGroupShareRevalidatesCurrentMembership(t *testing.T) {
	t.Parallel()

	svc, target, shareID, sharedDir, groupRepo, memberID := newUploadSessionGroupShareFixture(t, "CRU")
	session, err := svc.Create(context.Background(), target, UploadSessionCreateInput{
		ShareID:   shareID,
		Path:      "file.txt",
		Size:      6,
		ChunkSize: 3,
		FileName:  "file.txt",
	})
	if err != nil {
		t.Fatalf("Create while active group member: %v", err)
	}

	groupRepo.members[memberID].Status = group.MemberStatusPending
	if _, _, err := svc.UploadPart(context.Background(), target, session.ID, 1, uploadSessionTestChecksum("abc"), strings.NewReader("abc")); !errors.Is(err, ErrUploadSessionForbidden) {
		t.Fatalf("UploadPart after membership became pending error = %v, want %v", err, ErrUploadSessionForbidden)
	}

	groupRepo.members[memberID].Status = group.MemberStatusActive
	if _, _, err := svc.UploadPart(context.Background(), target, session.ID, 1, uploadSessionTestChecksum("abc"), strings.NewReader("abc")); err != nil {
		t.Fatalf("UploadPart 1 after membership restored: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), target, session.ID, 2, uploadSessionTestChecksum("def"), strings.NewReader("def")); err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}
	if _, err := svc.Complete(context.Background(), target, session.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(sharedDir, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("expected completed content, got %q", data)
	}
}

func TestUploadSessionServiceCleanupExpiredRemovesSessionDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	owner := user.NewUser("alice", "alice")
	owner.ID = "user-alice"
	if err := os.MkdirAll(filepath.Join(root, "alice", "personal"), 0o755); err != nil {
		t.Fatal(err)
	}
	svc := NewUploadSessionService(uploadSessionTestConfig(root), nil, nil, nil, nil, noopMutationRecorder{}, zap.NewNop())

	session, err := svc.Create(context.Background(), owner, UploadSessionCreateInput{Path: "/personal/file.txt", Size: 4, ChunkSize: 4, FileName: "file.txt"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), owner, session.ID, 1, uploadSessionTestChecksum("abcd"), strings.NewReader("abcd")); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	session.ExpiresAt = time.Now().Add(-time.Minute)
	if err := svc.saveSession(session); err != nil {
		t.Fatalf("saveSession: %v", err)
	}

	cleaned, err := svc.CleanupExpired(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned = %d, want 1", cleaned)
	}
	if _, err := os.Stat(svc.sessionDir(session.ID)); !os.IsNotExist(err) {
		t.Fatalf("expected session dir removed, stat err = %v", err)
	}
}

func TestUploadSessionServiceRejectsPartChecksumMismatch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	owner := user.NewUser("alice", "alice")
	owner.ID = "user-alice"
	if err := os.MkdirAll(filepath.Join(root, "alice", "personal"), 0o755); err != nil {
		t.Fatal(err)
	}
	svc := NewUploadSessionService(uploadSessionTestConfig(root), nil, nil, nil, nil, noopMutationRecorder{}, zap.NewNop())

	session, err := svc.Create(context.Background(), owner, UploadSessionCreateInput{Path: "/personal/file.txt", Size: 4, ChunkSize: 4, FileName: "file.txt"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, _, err := svc.UploadPart(context.Background(), owner, session.ID, 1, uploadSessionTestChecksum("different"), strings.NewReader("abcd")); !errors.Is(err, ErrUploadSessionChecksum) {
		t.Fatalf("expected ErrUploadSessionChecksum, got %v", err)
	}
}

func uploadSessionTestConfig(root string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.WebDAV.Directory = root
	return cfg
}

func uploadSessionTestChecksum(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func newUploadSessionShareFixture(t *testing.T, permissions string) (*UploadSessionService, *user.User, string, string) {
	t.Helper()

	root := t.TempDir()
	cfg := uploadSessionTestConfig(root)
	owner := user.NewUser("owner", "owner")
	owner.ID = "owner-id"
	target := user.NewUser("target", "target")
	target.ID = "target-id"

	userRepo := newTestUserRepo()
	mustSaveUser(t, userRepo, owner)
	mustSaveUser(t, userRepo, target)

	sharedDir := filepath.Join(root, owner.Directory, "personal", "shared")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}

	shareRepo := newMemoryShareRepo()
	item := shareuser.NewInternalShareItem(owner.ID, owner.Username, "/personal/shared", "shared", true, permissions, nil)
	shareRepo.items[item.ID] = cloneShareUserItem(item)
	shareRepo.audiences[item.ID] = []repository.UserShareAudience{
		{
			AudienceType: shareuser.AudienceTypeUser,
			TargetUserID: target.ID,
		},
	}
	shareService := NewShareUserService(shareRepo, userRepo, nil, nil, cfg, zap.NewNop())
	svc := NewUploadSessionService(cfg, nil, nil, userRepo, shareService, noopMutationRecorder{}, zap.NewNop())
	svc.SetSharedResourceAccess(NewSharedResourceAccessService(fakeSharedResourceGrantRepository{resource: &sharegrant.Resource{ID: "resource-" + item.ID}, grants: []sharegrant.Grant{{ID: item.ID, Permissions: permissions, Status: sharegrant.StatusActive}}}))
	return svc, target, item.ID, sharedDir
}

func newUploadSessionGroupShareFixture(t *testing.T, permissions string) (*UploadSessionService, *user.User, string, string, *fakeGroupRepository, string) {
	t.Helper()

	root := t.TempDir()
	cfg := uploadSessionTestConfig(root)
	owner := newShareTestUser(t, "owner", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	target := newShareTestUser(t, "target", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	userRepo := newTestUserRepo()
	mustSaveUser(t, userRepo, owner)
	mustSaveUser(t, userRepo, target)

	sharedDir := filepath.Join(root, owner.Directory, "personal", "shared")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}

	groupRepo := newFakeGroupRepository()
	groupSvc := NewGroupService(groupRepo, userRepo)
	grp, err := groupSvc.CreateGroup(context.Background(), owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	member, err := groupSvc.CreateMember(context.Background(), owner, CreateMemberInput{Target: target.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember: %v", err)
	}
	if err := groupSvc.ApproveMember(context.Background(), target, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember: %v", err)
	}

	shareRepo := newMemoryShareRepo()
	shareService := NewShareUserService(shareRepo, userRepo, groupSvc, nil, cfg, zap.NewNop())
	item, err := shareService.CreateByGroups(context.Background(), owner, []string{grp.ID}, "/personal/shared", permissions, ShareExpiryInput{})
	if err != nil {
		t.Fatalf("CreateByGroups: %v", err)
	}

	svc := NewUploadSessionService(cfg, nil, nil, userRepo, shareService, noopMutationRecorder{}, zap.NewNop())
	svc.SetSharedResourceAccess(NewSharedResourceAccessService(fakeSharedResourceGrantRepository{resource: &sharegrant.Resource{ID: "resource-" + item.ID}, grants: []sharegrant.Grant{{ID: item.ID, Permissions: permissions, Status: sharegrant.StatusActive}}}))
	return svc, target, item.ID, sharedDir, groupRepo, member.ID
}

type uploadSessionTestRecorder struct {
	upsertPath string
}

func (r *uploadSessionTestRecorder) EnsureDir(context.Context, string) error { return nil }
func (r *uploadSessionTestRecorder) UpsertFile(_ context.Context, fullPath string) error {
	r.upsertPath = fullPath
	return nil
}
func (r *uploadSessionTestRecorder) MovePath(context.Context, string, string, bool) error { return nil }
func (r *uploadSessionTestRecorder) CopyPath(context.Context, string, string, bool) error { return nil }
func (r *uploadSessionTestRecorder) RemovePath(context.Context, string, bool) error       { return nil }
