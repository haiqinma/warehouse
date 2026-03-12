package health

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type fakePinger struct {
	err error
}

func (p fakePinger) PingContext(context.Context) error {
	return p.err
}

func TestReadinessCheckerReady(t *testing.T) {
	dir := t.TempDir()
	checker := NewReadinessChecker(fakePinger{}, dir)

	result := checker.Check(context.Background())
	if !result.Ready() {
		t.Fatalf("expected ready result, got %#v", result)
	}
}

func TestReadinessCheckerDatabaseFailure(t *testing.T) {
	dir := t.TempDir()
	checker := NewReadinessChecker(fakePinger{err: errors.New("db down")}, dir)

	result := checker.Check(context.Background())
	if result.Ready() {
		t.Fatalf("expected not ready result, got %#v", result)
	}
	if result.Checks[0].Name != "database" || result.Checks[0].Status != StatusNotReady {
		t.Fatalf("expected database failure, got %#v", result.Checks[0])
	}
}

func TestReadinessCheckerDirectoryFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	checker := NewReadinessChecker(fakePinger{}, dir)

	result := checker.Check(context.Background())
	if result.Ready() {
		t.Fatalf("expected not ready result, got %#v", result)
	}
	if result.Checks[1].Name != "webdav_directory" || result.Checks[1].Status != StatusNotReady {
		t.Fatalf("expected directory failure, got %#v", result.Checks[1])
	}
}

func TestReadinessCheckerDirectoryPathIsFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	checker := NewReadinessChecker(fakePinger{}, path)
	result := checker.Check(context.Background())
	if result.Ready() {
		t.Fatalf("expected not ready result, got %#v", result)
	}
	if result.Checks[1].Name != "webdav_directory" || result.Checks[1].Status != StatusNotReady {
		t.Fatalf("expected directory write failure, got %#v", result.Checks[1])
	}
}
