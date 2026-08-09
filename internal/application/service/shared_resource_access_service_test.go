package service

import (
	"context"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/sharegrant"
)

type fakeSharedResourceGrantRepository struct {
	resource *sharegrant.Resource
	grants   []sharegrant.Grant
	err      error
}

func (r fakeSharedResourceGrantRepository) GetAccessibleGrants(context.Context, string, string) (*sharegrant.Resource, []sharegrant.Grant, error) {
	return r.resource, r.grants, r.err
}

func (r fakeSharedResourceGrantRepository) GetResourceIDByLegacyShareID(context.Context, string) (string, error) {
	if r.resource == nil {
		return "", r.err
	}
	return r.resource.ID, r.err
}

func TestSharedResourceAccessServiceAuthorizesOnlyAnEffectiveAction(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Second)
	service := NewSharedResourceAccessService(fakeSharedResourceGrantRepository{
		resource: &sharegrant.Resource{ID: "resource-1"},
		grants:   []sharegrant.Grant{{Permissions: "R", Status: sharegrant.StatusActive}, {Permissions: "U", Status: sharegrant.StatusActive, ExpiresAt: &expired}},
	})
	if _, err := service.Authorize(context.Background(), "resource-1", "user-1", "read", now); err != nil {
		t.Fatalf("read authorization: %v", err)
	}
	if _, err := service.Authorize(context.Background(), "resource-1", "user-1", "update", now); err == nil {
		t.Fatal("expired update grant must be denied")
	}
}

func TestSharedResourceAccessServiceReturnsDisplayUnionSeparately(t *testing.T) {
	now := time.Now()
	service := NewSharedResourceAccessService(fakeSharedResourceGrantRepository{resource: &sharegrant.Resource{ID: "resource-1"}, grants: []sharegrant.Grant{{Permissions: "CR", Status: sharegrant.StatusActive}}})
	_, permissions, err := service.EffectivePermissions(context.Background(), "resource-1", "user-1", now)
	if err != nil || permissions != "CR" {
		t.Fatalf("EffectivePermissions() = %q, %v", permissions, err)
	}
}
