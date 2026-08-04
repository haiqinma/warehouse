package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateWebDAVAutoCreateDirectory(t *testing.T) {
	loader := NewLoader()
	root := t.TempDir()
	target := filepath.Join(root, "webdav")

	cfg := DefaultConfig()
	cfg.WebDAV.Directory = target
	cfg.WebDAV.AutoCreateDirectory = true

	if err := loader.validateWebDAV(cfg); err != nil {
		t.Fatalf("expected directory to be auto-created, got error: %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("expected directory to exist, got error: %v", err)
	}
}

func TestValidateWebDAVRequireExistingDirectory(t *testing.T) {
	loader := NewLoader()
	cfg := DefaultConfig()
	cfg.WebDAV.Directory = filepath.Join(t.TempDir(), "missing")
	cfg.WebDAV.AutoCreateDirectory = false

	if err := loader.validateWebDAV(cfg); err == nil {
		t.Fatalf("expected error when directory is missing and auto creation is disabled")
	}
}

func TestValidateReplicationRequiresNodeIDAndSecret(t *testing.T) {
	loader := NewLoader()
	cfg := DefaultConfig()
	cfg.Replication.Enabled = true

	if err := loader.validateNode(cfg); err != nil {
		t.Fatalf("validateNode failed: %v", err)
	}
	if err := loader.validateReplication(cfg); err == nil {
		t.Fatalf("expected error when replication is enabled without node id and shared secret")
	}
}

func TestValidateReplicationAcceptsStandbyRole(t *testing.T) {
	loader := NewLoader()
	cfg := DefaultConfig()
	cfg.Node.ID = "node-b"
	cfg.Node.Role = "standby"
	cfg.Replication.Enabled = true
	cfg.Replication.SharedSecret = "secret"
	cfg.Replication.AllowedClockSkew = time.Minute

	if err := loader.validateNode(cfg); err != nil {
		t.Fatalf("expected standby role to be accepted, got: %v", err)
	}
	if err := loader.validateReplication(cfg); err != nil {
		t.Fatalf("expected replication config to be valid, got: %v", err)
	}
}

func TestValidateReplicationActiveAllowsDynamicPeerDiscovery(t *testing.T) {
	loader := NewLoader()
	cfg := DefaultConfig()
	cfg.Node.ID = "node-a"
	cfg.Node.Role = "active"
	cfg.Replication.Enabled = true
	cfg.Replication.SharedSecret = "secret"
	cfg.Replication.AllowedClockSkew = time.Minute

	if err := loader.validateNode(cfg); err != nil {
		t.Fatalf("validateNode failed: %v", err)
	}
	if err := loader.validateReplication(cfg); err != nil {
		t.Fatalf("expected dynamic peer discovery config to be valid, got: %v", err)
	}
}

func TestValidateReplicationRejectsInvalidWorkerSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "batch size",
			mutate: func(cfg *Config) {
				cfg.Replication.BatchSize = 0
			},
		},
		{
			name: "reconcile max concurrency",
			mutate: func(cfg *Config) {
				cfg.Replication.ReconcileMaxConcurrency = 0
			},
		},
		{
			name: "reconcile bandwidth limit",
			mutate: func(cfg *Config) {
				cfg.Replication.ReconcileBandwidthLimitBPS = -1
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader := NewLoader()
			cfg := DefaultConfig()
			cfg.Node.ID = "node-a"
			cfg.Node.Role = "active"
			cfg.Replication.Enabled = true
			cfg.Replication.SharedSecret = "secret"
			cfg.Replication.AllowedClockSkew = time.Minute
			tc.mutate(cfg)

			if err := loader.validateNode(cfg); err != nil {
				t.Fatalf("validateNode failed: %v", err)
			}
			if err := loader.validateReplication(cfg); err == nil {
				t.Fatalf("expected invalid worker setting to be rejected")
			}
		})
	}
}

func TestValidateQuotaRejectsInvalidAutoReconcileSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "interval",
			mutate: func(cfg *Config) {
				cfg.Quota.AutoReconcileInterval = 0
			},
		},
		{
			name: "batch size",
			mutate: func(cfg *Config) {
				cfg.Quota.AutoReconcileBatchSize = 0
			},
		},
		{
			name: "batch pause",
			mutate: func(cfg *Config) {
				cfg.Quota.AutoReconcileBatchPause = -time.Second
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader := NewLoader()
			cfg := DefaultConfig()
			cfg.Quota.AutoReconcileEnabled = true
			tc.mutate(cfg)

			if err := loader.validateQuota(cfg); err == nil {
				t.Fatalf("expected invalid quota setting to be rejected")
			}
		})
	}
}
