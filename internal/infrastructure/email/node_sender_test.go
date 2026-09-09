package email

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/application/service"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

func TestSenderPublishNotificationSendsSignedPusherEvent(t *testing.T) {
	var gotBody map[string]any
	var gotTimestamp string
	var gotSignature string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/public/pusher/apps/warehouse/events" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("X-Pusher-Key") != "pk_test" {
			t.Fatalf("unexpected pusher key: %s", r.Header.Get("X-Pusher-Key"))
		}
		gotTimestamp = r.Header.Get("X-Pusher-Timestamp")
		gotSignature = r.Header.Get("X-Pusher-Signature")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":200,"data":{"accepted":true}}`))
	}))
	defer server.Close()

	sender := NewSender(config.EmailConfig{
		NodeEmailEnabled: true,
		NodeURL:          server.URL,
		PusherAppID:      "warehouse",
		PusherKey:        "pk_test",
		PusherSecret:     "ps_secret",
		RequestTimeout:   time.Second,
	}, zap.NewNop())
	err := sender.PublishNotification(context.Background(), service.NotificationEmailEvent{
		EventID:       "evt-1",
		Type:          "warehouse.storage.quota.warning",
		Source:        "warehouse",
		Channels:      []string{"public-warehouse"},
		Recipients:    []string{"did:yeying:wid_1234567890123456789012"},
		Title:         "存储额度接近上限",
		Body:          "当前已使用 82.00%，请及时处理。",
		Level:         "warning",
		SubjectType:   "warehouse_user",
		SubjectID:     "user-1",
		Persist:       true,
		EmailRequired: true,
		Payload: map[string]any{
			"emailTemplateId":  "warehouse-storage-quota-warning",
			"appName":          "Warehouse",
			"unit":             "GiB",
			"usedStorage":      "8.20",
			"storageQuota":     "10.00",
			"remainingStorage": "1.80",
			"overageStorage":   "0.00",
			"usagePercent":     "82.00",
			"accountId":        "did:yeying:wid_1234567890123456789012",
		},
	})
	if err != nil {
		t.Fatalf("PublishNotification() error = %v", err)
	}
	if gotBody["eventId"] != "evt-1" || gotBody["type"] != "warehouse.storage.quota.warning" {
		t.Fatalf("unexpected event body: %#v", gotBody)
	}
	data := gotBody["data"].(map[string]any)
	if data["emailTemplateId"] != "warehouse-storage-quota-warning" || data["email"] != true {
		t.Fatalf("unexpected data payload: %#v", data)
	}
	expected, err := buildPusherSignature(gotTimestamp, gotBody, "ps_secret")
	if err != nil {
		t.Fatalf("build signature: %v", err)
	}
	if gotSignature != expected || !strings.HasPrefix(gotSignature, "sha256=") {
		t.Fatalf("signature = %q, want %q", gotSignature, expected)
	}
}
