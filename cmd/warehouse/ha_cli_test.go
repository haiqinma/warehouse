package main

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
)

func TestResolveHABaseURLDefaultsToLocalhost(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.Address = "0.0.0.0"
	cfg.Server.Port = 6065

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("base-url", "", "")
	flags.Bool("peer", false, "")

	baseURL, err := resolveHABaseURL(cfg, flags)
	if err != nil {
		t.Fatalf("resolveHABaseURL returned error: %v", err)
	}
	if baseURL != "http://127.0.0.1:6065" {
		t.Fatalf("unexpected baseURL: %q", baseURL)
	}
}

func TestResolveHABaseURLUsesPeerWhenRequested(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Internal.Replication.PeerBaseURL = "http://127.0.0.1:6066"

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("base-url", "", "")
	flags.Bool("peer", false, "")
	if err := flags.Set("peer", "true"); err != nil {
		t.Fatalf("set peer flag: %v", err)
	}

	baseURL, err := resolveHABaseURL(cfg, flags)
	if err != nil {
		t.Fatalf("resolveHABaseURL returned error: %v", err)
	}
	if baseURL != "http://127.0.0.1:6066" {
		t.Fatalf("unexpected baseURL: %q", baseURL)
	}
}

func TestResolveHABaseURLRejectsInvalidOverride(t *testing.T) {
	cfg := config.DefaultConfig()

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("base-url", "", "")
	flags.Bool("peer", false, "")
	if err := flags.Set("base-url", "127.0.0.1:6065"); err != nil {
		t.Fatalf("set base-url flag: %v", err)
	}

	if _, err := resolveHABaseURL(cfg, flags); err == nil {
		t.Fatalf("expected invalid base-url error")
	}
}
