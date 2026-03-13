package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/interface/http/middleware"
)

func runHACommand(args []string) error {
	if len(args) == 0 {
		printHAHelp()
		return nil
	}

	switch args[0] {
	case "status":
		return runHAStatus(args[1:])
	case "reconcile":
		return runHAReconcile(args[1:])
	case "bootstrap":
		return runHABootstrap(args[1:])
	case "-h", "--help", "help":
		printHAHelp()
		return nil
	default:
		return fmt.Errorf("unsupported ha subcommand %q", args[0])
	}
}

func runHAStatus(args []string) error {
	flags := newHAFlags("status")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		fmt.Println("Usage:")
		fmt.Println("  warehouse ha status -c config.yaml [--peer] [--base-url URL]")
		return nil
	}

	client, baseURL, err := buildHAClientFromFlags(flags)
	if err != nil {
		return err
	}
	body, err := client.doJSON(context.Background(), http.MethodGet, baseURL, "/api/v1/internal/replication/status", nil)
	if err != nil {
		return err
	}
	printPrettyJSON(body)
	return nil
}

func runHAReconcile(args []string) error {
	if len(args) == 0 {
		printHAReconcileHelp()
		return nil
	}

	switch args[0] {
	case "start":
		return runHAReconcileStart(args[1:])
	case "status":
		return runHAReconcileStatus(args[1:])
	case "-h", "--help", "help":
		printHAReconcileHelp()
		return nil
	default:
		return fmt.Errorf("unsupported ha reconcile subcommand %q", args[0])
	}
}

func runHAReconcileStart(args []string) error {
	flags := newHAFlags("reconcile-start")
	targetNodeID := flags.String("target-node-id", "", "Target standby node id override")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		fmt.Println("Usage:")
		fmt.Println("  warehouse ha reconcile start -c config.yaml [--peer] [--base-url URL] [--target-node-id ID]")
		return nil
	}
	client, baseURL, err := buildHAClientFromFlags(flags)
	if err != nil {
		return err
	}

	payload := map[string]string{}
	if strings.TrimSpace(*targetNodeID) != "" {
		payload["targetNodeId"] = strings.TrimSpace(*targetNodeID)
	}
	body, err := client.doJSON(context.Background(), http.MethodPost, baseURL, "/api/v1/internal/replication/reconcile/start", payload)
	if err != nil {
		return err
	}
	printPrettyJSON(body)
	return nil
}

func runHAReconcileStatus(args []string) error {
	flags := newHAFlags("reconcile-status")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		fmt.Println("Usage:")
		fmt.Println("  warehouse ha reconcile status -c config.yaml [--peer] [--base-url URL]")
		return nil
	}

	client, baseURL, err := buildHAClientFromFlags(flags)
	if err != nil {
		return err
	}
	body, err := client.doJSON(context.Background(), http.MethodGet, baseURL, "/api/v1/internal/replication/status", nil)
	if err != nil {
		return err
	}

	var statusResp map[string]any
	if err := json.Unmarshal(body, &statusResp); err != nil {
		return fmt.Errorf("parse status response: %w", err)
	}
	reconcile, ok := statusResp["reconcile"]
	if !ok {
		fmt.Println("{}")
		return nil
	}
	reconcileBody, err := json.Marshal(reconcile)
	if err != nil {
		return fmt.Errorf("marshal reconcile status: %w", err)
	}
	printPrettyJSON(reconcileBody)
	return nil
}

func runHABootstrap(args []string) error {
	if len(args) == 0 {
		printHABootstrapHelp()
		return nil
	}

	switch args[0] {
	case "mark":
		return runHABootstrapMark(args[1:])
	case "-h", "--help", "help":
		printHABootstrapHelp()
		return nil
	default:
		return fmt.Errorf("unsupported ha bootstrap subcommand %q", args[0])
	}
}

func runHABootstrapMark(args []string) error {
	flags := newHAFlags("bootstrap-mark")
	outboxID := flags.Int64("outbox-id", -1, "Explicit bootstrap outbox id")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		fmt.Println("Usage:")
		fmt.Println("  warehouse ha bootstrap mark -c config.yaml [--peer] [--base-url URL] [--outbox-id N]")
		return nil
	}
	client, baseURL, err := buildHAClientFromFlags(flags)
	if err != nil {
		return err
	}

	payload := map[string]int64{}
	if *outboxID >= 0 {
		payload["outboxId"] = *outboxID
	}
	var body []byte
	if len(payload) == 0 {
		body, err = client.doJSON(context.Background(), http.MethodPost, baseURL, "/api/v1/internal/replication/bootstrap/mark", map[string]any{})
	} else {
		body, err = client.doJSON(context.Background(), http.MethodPost, baseURL, "/api/v1/internal/replication/bootstrap/mark", payload)
	}
	if err != nil {
		return err
	}
	printPrettyJSON(body)
	return nil
}

type haClient struct {
	cfg        *config.Config
	httpClient *http.Client
}

func newHAFlags(name string) *pflag.FlagSet {
	flags := pflag.NewFlagSet(name, pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.String("base-url", "", "Internal base URL override, such as http://127.0.0.1:6065")
	flags.Bool("peer", false, "Use internal.replication.peer_base_url from config")
	flags.Duration("timeout", 0, "HTTP timeout override")
	flags.BoolP("help", "h", false, "Show help")
	return flags
}

func buildHAClientFromFlags(flags *pflag.FlagSet) (*haClient, string, error) {
	configFile, _ := flags.GetString("config")
	if strings.TrimSpace(configFile) == "" {
		return nil, "", fmt.Errorf("config file is required, use -c config.yaml")
	}

	cfg, err := loadConfig(configFile, nil)
	if err != nil {
		return nil, "", err
	}

	baseURL, err := resolveHABaseURL(cfg, flags)
	if err != nil {
		return nil, "", err
	}

	timeout, _ := flags.GetDuration("timeout")
	if timeout <= 0 {
		timeout = cfg.Internal.Replication.RequestTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
	}

	return &haClient{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}, baseURL, nil
}

func resolveHABaseURL(cfg *config.Config, flags *pflag.FlagSet) (string, error) {
	baseURL, _ := flags.GetString("base-url")
	if strings.TrimSpace(baseURL) != "" {
		return normalizeURL(baseURL)
	}

	usePeer, _ := flags.GetBool("peer")
	if usePeer {
		peerBaseURL := strings.TrimSpace(cfg.Internal.Replication.PeerBaseURL)
		if peerBaseURL == "" {
			return "", fmt.Errorf("internal.replication.peer_base_url is empty")
		}
		return normalizeURL(peerBaseURL)
	}

	scheme := "http"
	if cfg.Server.TLS {
		scheme = "https"
	}
	host := strings.TrimSpace(cfg.Server.Address)
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return normalizeURL(fmt.Sprintf("%s://%s:%d", scheme, host, cfg.Server.Port))
}

func (c *haClient) doJSON(ctx context.Context, method, baseURL, path string, payload any) ([]byte, error) {
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal request payload: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.signRequest(req, body)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(respBody))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("request failed: %s", message)
	}
	return respBody, nil
}

func (c *haClient) signRequest(req *http.Request, body []byte) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	payloadHash := "UNSIGNED-PAYLOAD"
	if len(body) > 0 {
		payloadHash = payloadSHA256(body)
	}
	req.Header.Set(middleware.InternalNodeIDHeader, strings.TrimSpace(c.cfg.Node.ID))
	req.Header.Set(middleware.InternalTimestampHeader, timestamp)
	req.Header.Set(middleware.InternalContentSHA256Header, payloadHash)
	req.Header.Set(middleware.InternalSignatureHeader, middleware.SignInternalRequest(
		req.Method,
		req.URL.Path,
		strings.TrimSpace(c.cfg.Node.ID),
		timestamp,
		payloadHash,
		strings.TrimSpace(c.cfg.Internal.Replication.SharedSecret),
	))
}

func payloadSHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func printPrettyJSON(body []byte) {
	var out bytes.Buffer
	if err := json.Indent(&out, body, "", "  "); err != nil {
		fmt.Println(string(body))
		return
	}
	fmt.Println(out.String())
}

func printHAHelp() {
	fmt.Println("Usage:")
	fmt.Println("  warehouse ha status -c config.yaml [--peer] [--base-url URL]")
	fmt.Println("  warehouse ha reconcile start -c config.yaml [--peer] [--base-url URL] [--target-node-id ID]")
	fmt.Println("  warehouse ha reconcile status -c config.yaml [--peer] [--base-url URL]")
	fmt.Println("  warehouse ha bootstrap mark -c config.yaml [--peer] [--base-url URL] [--outbox-id N]")
}

func printHAReconcileHelp() {
	fmt.Println("Usage:")
	fmt.Println("  warehouse ha reconcile start -c config.yaml [--peer] [--base-url URL] [--target-node-id ID]")
	fmt.Println("  warehouse ha reconcile status -c config.yaml [--peer] [--base-url URL]")
}

func printHABootstrapHelp() {
	fmt.Println("Usage:")
	fmt.Println("  warehouse ha bootstrap mark -c config.yaml [--peer] [--base-url URL] [--outbox-id N]")
}

func normalizeURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("base URL must include scheme and host")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}
