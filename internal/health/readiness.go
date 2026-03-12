package health

import (
	"context"
	"fmt"
	"os"
	"time"
)

const defaultDatabaseTimeout = 3 * time.Second

const (
	StatusReady    = "ready"
	StatusNotReady = "not_ready"
)

// Pinger describes the database capability needed by the readiness check.
type Pinger interface {
	PingContext(ctx context.Context) error
}

// CheckResult captures the readiness state of one dependency.
type CheckResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Result is the aggregated readiness result.
type Result struct {
	Status string        `json:"status"`
	Checks []CheckResult `json:"checks"`
}

// Ready reports whether all readiness checks passed.
func (r Result) Ready() bool {
	return r.Status == StatusReady
}

// ReadinessChecker verifies dependencies required for active traffic.
type ReadinessChecker struct {
	db              Pinger
	webdavDirectory string
	dbTimeout       time.Duration
}

// NewReadinessChecker creates a checker for database connectivity and WebDAV storage.
func NewReadinessChecker(db Pinger, webdavDirectory string) *ReadinessChecker {
	return &ReadinessChecker{
		db:              db,
		webdavDirectory: webdavDirectory,
		dbTimeout:       defaultDatabaseTimeout,
	}
}

// Check executes the readiness checks.
func (c *ReadinessChecker) Check(ctx context.Context) Result {
	results := []CheckResult{
		c.checkDatabase(ctx),
		c.checkWebDAVDirectory(),
	}

	overall := StatusReady
	for _, result := range results {
		if result.Status != StatusReady {
			overall = StatusNotReady
			break
		}
	}

	return Result{
		Status: overall,
		Checks: results,
	}
}

func (c *ReadinessChecker) checkDatabase(ctx context.Context) CheckResult {
	result := CheckResult{
		Name:   "database",
		Status: StatusReady,
	}

	if c.db == nil {
		result.Status = StatusNotReady
		result.Error = "database pinger is not configured"
		return result
	}

	pingCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		pingCtx, cancel = context.WithTimeout(ctx, c.dbTimeout)
	}
	defer cancel()

	if err := c.db.PingContext(pingCtx); err != nil {
		result.Status = StatusNotReady
		result.Error = err.Error()
	}

	return result
}

func (c *ReadinessChecker) checkWebDAVDirectory() CheckResult {
	result := CheckResult{
		Name:   "webdav_directory",
		Status: StatusReady,
	}

	if c.webdavDirectory == "" {
		result.Status = StatusNotReady
		result.Error = "webdav directory is empty"
		return result
	}

	info, err := os.Stat(c.webdavDirectory)
	if err != nil {
		result.Status = StatusNotReady
		result.Error = fmt.Sprintf("stat directory: %v", err)
		return result
	}
	if !info.IsDir() {
		result.Status = StatusNotReady
		result.Error = "configured path is not a directory"
		return result
	}

	file, err := os.CreateTemp(c.webdavDirectory, ".warehouse-readiness-*")
	if err != nil {
		result.Status = StatusNotReady
		result.Error = fmt.Sprintf("create temp file: %v", err)
		return result
	}

	name := file.Name()
	defer func() {
		_ = os.Remove(name)
	}()

	if _, err := file.WriteString("ok"); err != nil {
		result.Status = StatusNotReady
		result.Error = fmt.Sprintf("write temp file: %v", err)
		_ = file.Close()
		return result
	}

	if err := file.Close(); err != nil {
		result.Status = StatusNotReady
		result.Error = fmt.Sprintf("close temp file: %v", err)
		return result
	}

	return result
}
