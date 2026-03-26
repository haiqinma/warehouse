package auth

import (
	"encoding/json"
	"testing"
)

func TestExtractAppCapsFromCapsRejectsWildcardAndInvalidAppID(t *testing.T) {
	caps := []UcanCapability{
		{Resource: "app:*", Action: "write"},
		{Resource: "app:good.example.com", Action: "read"},
		{Resource: "app:bad*", Action: "read"},
		{Resource: "app:bad/path", Action: "read"},
		{Resource: "profile", Action: "read"},
	}

	extracted := extractAppCapsFromCaps(caps, "app:")

	if !extracted.HasAppCaps {
		t.Fatalf("expected HasAppCaps=true")
	}

	actions, ok := extracted.AppCaps["good.example.com"]
	if !ok {
		t.Fatalf("expected app cap for good.example.com")
	}
	if len(actions) != 1 || actions[0] != "read" {
		t.Fatalf("unexpected actions: %#v", actions)
	}

	if !containsString(extracted.InvalidAppCaps, "app:*#write") {
		t.Fatalf("expected invalid cap app:*#write, got %#v", extracted.InvalidAppCaps)
	}
	if !containsString(extracted.InvalidAppCaps, "app:bad*#read") {
		t.Fatalf("expected invalid cap app:bad*#read, got %#v", extracted.InvalidAppCaps)
	}
	if !containsString(extracted.InvalidAppCaps, "app:bad/path#read") {
		t.Fatalf("expected invalid cap app:bad/path#read, got %#v", extracted.InvalidAppCaps)
	}
}

func TestExtractAppCapsFromCapsAcceptsScopedAllAndLegacyResource(t *testing.T) {
	caps := []UcanCapability{
		{With: "app:all:dapp-a", Can: "write"},
		{Resource: "app:dapp-b", Action: "read"},
		{Resource: "app:profile:dapp-c", Action: "read"},
	}

	extracted := extractAppCapsFromCaps(caps, "app:")
	if !extracted.HasAppCaps {
		t.Fatalf("expected HasAppCaps=true")
	}

	if got := extracted.AppCaps["dapp-a"]; len(got) != 1 || got[0] != "write" {
		t.Fatalf("unexpected dapp-a actions: %#v", got)
	}
	if got := extracted.AppCaps["dapp-b"]; len(got) != 1 || got[0] != "read" {
		t.Fatalf("unexpected dapp-b actions: %#v", got)
	}
	if !containsString(extracted.InvalidAppCaps, "app:profile:dapp-c#read") {
		t.Fatalf("expected invalid scope app cap, got %#v", extracted.InvalidAppCaps)
	}
}

func TestExtractCapabilitiesSupportsCapWithCanAndAtt(t *testing.T) {
	rawCaps := []json.RawMessage{
		json.RawMessage(`{"resource":"profile","action":"read"}`),
		json.RawMessage(`{"with":"app:all:dapp-a","can":"write","nb":{"path":"/apps/dapp-a"}}`),
	}
	att := map[string]map[string]json.RawMessage{
		"app:all:dapp-b": {
			"read": json.RawMessage(`[{"path":"/apps/dapp-b"}]`),
		},
	}

	caps, err := extractCapabilities(rawCaps, att)
	if err != nil {
		t.Fatalf("extractCapabilities failed: %v", err)
	}
	if !hasCapability(caps, "profile", "read") {
		t.Fatalf("missing legacy resource/action capability: %#v", caps)
	}
	if !hasCapability(caps, "app:all:dapp-a", "write") {
		t.Fatalf("missing with/can capability: %#v", caps)
	}
	if !hasCapability(caps, "app:all:dapp-b", "read") {
		t.Fatalf("missing att capability: %#v", caps)
	}
}

func TestBuildRequiredUcanCapsSupportsAdditionalCapabilities(t *testing.T) {
	additional := []UcanCapability{
		{With: "profile", Can: "read"},
		{Resource: "app:*", Action: "read,write"},
		{With: "profile", Can: "read"}, // duplicate should be deduped
	}

	caps := BuildRequiredUcanCaps("app:*", "read,write", additional)
	if len(caps) != 2 {
		t.Fatalf("expected 2 deduped caps, got %#v", caps)
	}
	if !hasCapability(caps, "app:*", "read,write") {
		t.Fatalf("missing app required cap: %#v", caps)
	}
	if !hasCapability(caps, "profile", "read") {
		t.Fatalf("missing profile required cap: %#v", caps)
	}
}

func TestExtractAppCapsFromCapsWithoutAppResource(t *testing.T) {
	caps := []UcanCapability{
		{Resource: "profile", Action: "read"},
	}

	extracted := extractAppCapsFromCaps(caps, "app:")
	if extracted.HasAppCaps {
		t.Fatalf("expected HasAppCaps=false")
	}
	if len(extracted.AppCaps) != 0 {
		t.Fatalf("expected empty AppCaps, got %#v", extracted.AppCaps)
	}
	if len(extracted.InvalidAppCaps) != 0 {
		t.Fatalf("expected empty InvalidAppCaps, got %#v", extracted.InvalidAppCaps)
	}
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func hasCapability(caps []UcanCapability, resource, action string) bool {
	for _, cap := range caps {
		if cap.normalizedResource() == resource && cap.normalizedAction() == action {
			return true
		}
	}
	return false
}
