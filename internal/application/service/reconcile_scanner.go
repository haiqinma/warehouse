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
