package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yeying-community/warehouse/internal/application/service"
	"github.com/yeying-community/warehouse/internal/domain/user"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/interface/http/middleware"
	"go.uber.org/zap"
)

func TestAssetObjectHandlerPutMetadataDownloadAndList(t *testing.T) {
	root := t.TempDir()
	objects := service.NewObjectService(root)
	handler := NewAssetObjectHandler(&config.Config{}, objects, zap.NewNop())
	owner := &user.User{ID: "u1", Username: "alice", Directory: "alice", Quota: 0}
	payload := "hello knowledge"
	sum := sha256.Sum256([]byte(payload))
	checksum := hex.EncodeToString(sum[:])

	putReq := newAssetObjectRequest(t, http.MethodPut, "/api/v1/public/assets/object/content?path=/services/knowledge/artifacts/report.md", strings.NewReader(payload), owner)
	putReq.Header.Set("X-Warehouse-Checksum-SHA256", checksum)
	putReq.Header.Set("Content-Type", "text/markdown; charset=utf-8")
	putRec := httptest.NewRecorder()
	handler.HandleObjectContent(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status=%d body=%s", putRec.Code, putRec.Body.String())
	}
	var putResp assetObjectResponse
	if err := json.NewDecoder(putRec.Body).Decode(&putResp); err != nil {
		t.Fatalf("decode put response: %v", err)
	}
	if putResp.Path != "/services/knowledge/artifacts/report.md" || putResp.ChecksumSHA256 != checksum {
		t.Fatalf("unexpected put response: %+v", putResp)
	}

	metaReq := newAssetObjectRequest(t, http.MethodGet, "/api/v1/public/assets/object?path=/services/knowledge/artifacts/report.md", nil, owner)
	metaRec := httptest.NewRecorder()
	handler.HandleObject(metaRec, metaReq)
	if metaRec.Code != http.StatusOK {
		t.Fatalf("metadata status=%d body=%s", metaRec.Code, metaRec.Body.String())
	}
	var metaResp assetObjectResponse
	if err := json.NewDecoder(metaRec.Body).Decode(&metaResp); err != nil {
		t.Fatalf("decode metadata response: %v", err)
	}
	if metaResp.Size != int64(len(payload)) || metaResp.ChecksumSHA256 != checksum {
		t.Fatalf("unexpected metadata response: %+v", metaResp)
	}

	getReq := newAssetObjectRequest(t, http.MethodGet, "/api/v1/public/assets/object/content?path=/services/knowledge/artifacts/report.md", nil, owner)
	getRec := httptest.NewRecorder()
	handler.HandleObjectContent(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("download status=%d body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != payload {
		t.Fatalf("unexpected download body: %q", got)
	}
	if got := getRec.Header().Get("X-Warehouse-Checksum-SHA256"); got != checksum {
		t.Fatalf("unexpected checksum header: %q", got)
	}

	listReq := newAssetObjectRequest(t, http.MethodGet, "/api/v1/public/assets/objects?prefix=/services/knowledge/&delimiter=/", nil, owner)
	listRec := httptest.NewRecorder()
	handler.HandleObjects(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", listRec.Code, listRec.Body.String())
	}
	var listResp assetObjectListResponse
	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Prefixes) != 1 || listResp.Prefixes[0] != "/services/knowledge/artifacts/" {
		t.Fatalf("unexpected list prefixes: %+v", listResp)
	}
}

func TestAssetObjectHandlerRejectsChecksumMismatch(t *testing.T) {
	root := t.TempDir()
	objects := service.NewObjectService(root)
	handler := NewAssetObjectHandler(&config.Config{}, objects, zap.NewNop())
	owner := &user.User{ID: "u1", Username: "alice", Directory: "alice", Quota: 0}

	req := newAssetObjectRequest(t, http.MethodPut, "/api/v1/public/assets/object/content?path=/services/knowledge/artifacts/report.md", strings.NewReader("hello"), owner)
	req.Header.Set("X-Warehouse-Checksum-SHA256", strings.Repeat("0", 64))
	rec := httptest.NewRecorder()
	handler.HandleObjectContent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAssetObjectHandlerRejectsInvalidPath(t *testing.T) {
	handler := NewAssetObjectHandler(&config.Config{}, service.NewObjectService(t.TempDir()), zap.NewNop())
	owner := &user.User{ID: "u1", Username: "alice", Directory: "alice", Quota: 0}

	req := newAssetObjectRequest(t, http.MethodGet, "/api/v1/public/assets/object?path=/private/data.txt", nil, owner)
	rec := httptest.NewRecorder()
	handler.HandleObject(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAssetObjectHandlerEnforcesUcanAppScope(t *testing.T) {
	root := t.TempDir()
	objects := service.NewObjectService(root)
	handler := NewAssetObjectHandler(&config.Config{}, objects, zap.NewNop())
	owner := &user.User{ID: "u1", Username: "alice", Directory: "alice", Quota: 0}
	if _, err := objects.PutForUser(context.Background(), owner, "services", "knowledge/private.txt", strings.NewReader("secret")); err != nil {
		t.Fatalf("seed object: %v", err)
	}

	req := newAssetObjectRequest(t, http.MethodGet, "/api/v1/public/assets/object?path=/services/knowledge/private.txt", nil, owner)
	req = req.WithContext(middleware.WithUcanContext(req.Context(), &middleware.UcanContext{
		HasAppCaps: true,
		AppCaps:    map[string][]string{"demo.app": {"read"}},
	}))
	rec := httptest.NewRecorder()
	handler.HandleObject(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func newAssetObjectRequest(t *testing.T, method, target string, body io.Reader, u *user.User) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	ctx := context.WithValue(req.Context(), middleware.UserContextKey, u)
	return req.WithContext(ctx)
}
