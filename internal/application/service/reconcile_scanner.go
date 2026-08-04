package service

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/yeying-community/warehouse/internal/domain/replication"
)

// ReconcileScanner walks local webdav data and produces reconcile items.
type ReconcileScanner struct {
	root string
}

// NewReconcileScanner creates a scanner for one webdav root.
func NewReconcileScanner(root string) (*ReconcileScanner, error) {
	abs, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return nil, fmt.Errorf("resolve reconcile root: %w", err)
	}
	return &ReconcileScanner{root: filepath.Clean(abs)}, nil
}

// Scan returns all paths under root as pending reconcile items.
func (s *ReconcileScanner) Scan(ctx context.Context) ([]*replication.ReconcileItem, error) {
	items := make([]*replication.ReconcileItem, 0, 256)
	err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return fmt.Errorf("compute relative path for %q: %w", path, err)
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if shouldSkipReconcilePath(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		item := &replication.ReconcileItem{
			Path:  "/" + strings.TrimPrefix(rel, "/"),
			IsDir: d.IsDir(),
			State: replication.ReconcileItemStatePending,
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("read file info for %q: %w", path, err)
		}
		modifiedAt := info.ModTime().UTC()
		item.ModifiedAt = &modifiedAt
		if !d.IsDir() {
			size := info.Size()
			item.FileSize = &size
		}
		items = append(items, item)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan reconcile root %q: %w", s.root, err)
	}

	return items, nil
}

func shouldSkipReconcilePath(rel string, isDir bool) bool {
	cleaned := filepath.ToSlash(filepath.Clean(strings.TrimSpace(rel)))
	if cleaned == "." || cleaned == "" {
		return false
	}
	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	if len(parts) == 0 {
		return false
	}
	base := parts[len(parts)-1]

	switch parts[0] {
	case ".recycle", ".warehouse-uploads", ".s3-multipart":
		return true
	}
	if strings.HasPrefix(base, "._upload-") ||
		strings.HasPrefix(base, ".warehouse-repl-") ||
		strings.HasPrefix(base, ".warehouse-copy-") ||
		strings.HasSuffix(base, ".tmp") ||
		strings.HasSuffix(base, ".tmp.reconcile") {
		return true
	}

	if isDir && base == ".s3-multipart" {
		return true
	}
	return isEphemeralSyncArtifactPath("/" + strings.TrimPrefix(cleaned, "/"))
}
