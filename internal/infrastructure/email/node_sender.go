package email

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yeying-community/warehouse/internal/application/service"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

// Sender delegates email delivery to YeYing Node. Warehouse no longer sends
// email through SMTP directly.
type Sender struct {
	cfg    config.EmailConfig
	logger *zap.Logger
	client *http.Client
}

// NewSender creates a Node-backed email sender.
func NewSender(cfg config.EmailConfig, logger *zap.Logger) *Sender {
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Sender{
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: timeout},
	}
}

// SendCode is kept for the legacy email-login handler. Node currently exposes
// email verification through Wallet Identity; Warehouse must not fall back to
// local SMTP sending.
func (s *Sender) SendCode(_ string, _ string, _ time.Duration) error {
	if s == nil || !s.cfg.Enabled {
		return errors.New("email login is disabled")
	}
	return errors.New("warehouse email code delivery is delegated to node identity verification")
}

// PublishNotification publishes a signed pusher event to Node. Node persists
// the notification and prepares email deliveries from its Identity email data.
func (s *Sender) PublishNotification(ctx context.Context, event service.NotificationEmailEvent) error {
	if s == nil || !s.cfg.NodeEmailEnabled {
		return nil
	}
	if strings.TrimSpace(s.cfg.NodeURL) == "" || strings.TrimSpace(s.cfg.PusherAppID) == "" || strings.TrimSpace(s.cfg.PusherKey) == "" || strings.TrimSpace(s.cfg.PusherSecret) == "" {
		return errors.New("node email pusher configuration is incomplete")
	}
	if len(event.Recipients) == 0 {
		return nil
	}

	body := map[string]any{
		"eventId":    strings.TrimSpace(event.EventID),
		"type":       strings.TrimSpace(event.Type),
		"source":     firstNonEmpty(event.Source, s.cfg.PusherAppID),
		"channels":   cleanStrings(event.Channels),
		"recipients": cleanStrings(event.Recipients),
		"persist":    event.Persist,
		"data":       map[string]any{},
		"notification": map[string]any{
			"title":       strings.TrimSpace(event.Title),
			"body":        strings.TrimSpace(event.Body),
			"level":       strings.TrimSpace(event.Level),
			"subjectType": strings.TrimSpace(event.SubjectType),
			"subjectId":   strings.TrimSpace(event.SubjectID),
		},
	}
	if body["eventId"] == "" {
		body["eventId"] = "warehouse-" + uuid.NewString()
	}
	if body["type"] == "" {
		body["type"] = "warehouse.notification"
	}
	if len(body["channels"].([]string)) == 0 {
		body["channels"] = []string{"public-warehouse"}
	}
	if strings.TrimSpace(event.Actor) != "" {
		body["actor"] = strings.TrimSpace(event.Actor)
	}
	if len(event.Payload) > 0 {
		body["data"] = event.Payload
		body["notification"].(map[string]any)["payload"] = event.Payload
	}
	if event.EmailRequired {
		payload, _ := body["data"].(map[string]any)
		payload["email"] = true
		body["data"] = payload
	}

	requestBody, err := json.Marshal(body)
	if err != nil {
		return err
	}
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	signature, err := buildPusherSignature(timestamp, body, s.cfg.PusherSecret)
	if err != nil {
		return err
	}
	url := strings.TrimRight(s.cfg.NodeURL, "/") + "/api/v1/public/pusher/apps/" + strings.TrimSpace(s.cfg.PusherAppID) + "/events"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Pusher-Key", strings.TrimSpace(s.cfg.PusherKey))
	req.Header.Set("X-Pusher-Timestamp", timestamp)
	req.Header.Set("X-Pusher-Signature", signature)

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("node email pusher returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

func buildPusherSignature(timestamp string, body map[string]any, secret string) (string, error) {
	canonical, err := canonicalJSON(body)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(strings.TrimSpace(timestamp) + "." + canonical))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil)), nil
}

func canonicalJSON(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "null", nil
	case string:
		data, err := json.Marshal(v)
		return string(data), err
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		data, err := json.Marshal(v)
		return string(data), err
	case []string:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			encoded, err := canonicalJSON(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, encoded)
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			encoded, err := canonicalJSON(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, encoded)
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			encodedKey, err := canonicalJSON(key)
			if err != nil {
				return "", err
			}
			encodedValue, err := canonicalJSON(v[key])
			if err != nil {
				return "", err
			}
			parts = append(parts, encodedKey+":"+encodedValue)
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		var normalized any
		if err := json.Unmarshal(data, &normalized); err != nil {
			return "", err
		}
		return canonicalJSON(normalized)
	}
}

func cleanStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		result = append(result, trimmed)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
