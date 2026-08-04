package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestReconcileScannerSkipsInternalArtifacts(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	paths := map[string]string{
		"alice/personal/doc.txt":                                                    "asset",
		"alice/apps/localhost-3020/backup.json":                                     "asset",
		".recycle/hash_doc.txt":                                                     "recycle",
		".warehouse-uploads/session-1/session.json":                                 "session",
		".s3-multipart/upload-1/part-00001":                                         "multipart",
		"alice/personal/._upload-123":                                               "atomic",
		"alice/personal/final.txt.tmp":                                              "tmp",
		"alice/personal/final.txt.tmp.reconcile":                                    "reconcile tmp",
		"alice/personal/.warehouse-repl-123":                                        "replication tmp",
		"alice/personal/.warehouse-copy-123":                                        "copy tmp",
		"alice/apps/localhost-3020/backup.__sync_mutex_v1.__sync_lock_v1/lock.json": "sync lock",
		"alice/apps/localhost-3020/backup.__sync_txn_head_v1.json":                  "sync head",
		"alice/apps/localhost-3020/backup.__sync_txn_data_v1.123-abc.json":          "sync txn",
		"alice/personal/backup.__sync_txn_data_v1.123-abc.json":                     "ordinary personal file",
	}
	for rel, body := range paths {
		fullPath := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(fullPath, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	scanner, err := NewReconcileScanner(root)
	if err != nil {
		t.Fatalf("new scanner: %v", err)
	}
	items, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	got := make(map[string]bool, len(items))
	for _, item := range items {
		got[item.Path] = true
	}
	for _, want := range []string{
		"/alice",
		"/alice/personal",
		"/alice/personal/doc.txt",
		"/alice/personal/backup.__sync_txn_data_v1.123-abc.json",
		"/alice/apps",
		"/alice/apps/localhost-3020",
		"/alice/apps/localhost-3020/backup.json",
	} {
		if !got[want] {
			t.Fatalf("expected scanned item %s, got %#v", want, got)
		}
	}
	for _, skipped := range []string{
		"/.recycle",
		"/.recycle/hash_doc.txt",
		"/.warehouse-uploads",
		"/.warehouse-uploads/session-1/session.json",
		"/.s3-multipart",
		"/.s3-multipart/upload-1/part-00001",
		"/alice/personal/._upload-123",
		"/alice/personal/final.txt.tmp",
		"/alice/personal/final.txt.tmp.reconcile",
		"/alice/personal/.warehouse-repl-123",
		"/alice/personal/.warehouse-copy-123",
		"/alice/apps/localhost-3020/backup.__sync_mutex_v1.__sync_lock_v1",
		"/alice/apps/localhost-3020/backup.__sync_mutex_v1.__sync_lock_v1/lock.json",
		"/alice/apps/localhost-3020/backup.__sync_txn_head_v1.json",
		"/alice/apps/localhost-3020/backup.__sync_txn_data_v1.123-abc.json",
	} {
		if got[skipped] {
			t.Fatalf("expected %s to be skipped, got %#v", skipped, got)
		}
	}
}
