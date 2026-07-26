package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/notification"
	"github.com/yeying-community/warehouse/internal/domain/user"
)

func TestNotificationServiceEnsuresPendingGroupInviteForCurrentUser(t *testing.T) {
	ctx := context.Background()
	groupRepo := newFakeGroupRepository()
	notificationRepo := newFakeNotificationRepository()
	userRepo := newTestUserRepo()
	svc := NewNotificationService(notificationRepo, userRepo, nil)
	svc.SetGroupRepository(groupRepo)
	groupSvc := NewGroupService(groupRepo, userRepo)
	groupSvc.SetNotificationService(svc)

	owner := &user.User{ID: "owner-user", Username: "owner", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", Username: "invited", WalletAddress: "0x9AdD99615252CaF379030d8966965BD9e5D80157"}
	if err := userRepo.Save(ctx, owner); err != nil {
		t.Fatalf("Save(owner) error = %v", err)
	}
	if err := userRepo.Save(ctx, invited); err != nil {
		t.Fatalf("Save(invited) error = %v", err)
	}

	grp, err := groupSvc.CreateGroup(ctx, owner, "test_01")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := groupSvc.CreateMember(ctx, owner, CreateMemberInput{
		Target:  strings.ToLower(invited.WalletAddress),
		GroupID: grp.ID,
	})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}

	count, err := svc.UnreadCountForCurrentUser(ctx, invited, false)
	if err != nil {
		t.Fatalf("UnreadCountForCurrentUser() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("UnreadCountForCurrentUser() = %d, want 1", count)
	}

	items, err := svc.ListForCurrentUser(ctx, invited, false, 20)
	if err != nil {
		t.Fatalf("ListForCurrentUser() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("ListForCurrentUser() returned %d items, want 1", len(items))
	}
	item := items[0]
	if item.Type != notification.TypeGroupInvite {
		t.Fatalf("notification type = %q, want %q", item.Type, notification.TypeGroupInvite)
	}
	if item.ActionURL != "#group-invite:"+member.ID {
		t.Fatalf("notification action URL = %q, want invite member action", item.ActionURL)
	}
	if !strings.Contains(item.Content, "test_01") {
		t.Fatalf("notification content = %q, want group name", item.Content)
	}
	if !strings.Contains(item.Content, owner.Username) {
		t.Fatalf("notification content = %q, want inviter username", item.Content)
	}
}

func TestNotificationServiceDismissesInviteAfterApproveAndReject(t *testing.T) {
	ctx := context.Background()
	for _, accepted := range []bool{true, false} {
		t.Run(map[bool]string{true: "approve", false: "reject"}[accepted], func(t *testing.T) {
			groupRepo := newFakeGroupRepository()
			notificationRepo := newFakeNotificationRepository()
			userRepo := newTestUserRepo()
			svc := NewNotificationService(notificationRepo, userRepo, nil)
			svc.SetGroupRepository(groupRepo)
			groupSvc := NewGroupService(groupRepo, userRepo)
			groupSvc.SetNotificationService(svc)

			owner := &user.User{ID: "owner-user", Username: "owner", WalletAddress: "0x1111111111111111111111111111111111111111"}
			invited := &user.User{ID: "invited-user", Username: "invited", WalletAddress: "0x2222222222222222222222222222222222222222"}
			_ = userRepo.Save(ctx, owner)
			_ = userRepo.Save(ctx, invited)
			grp, _ := groupSvc.CreateGroup(ctx, owner, "team")
			member, err := groupSvc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
			if err != nil {
				t.Fatalf("CreateMember() error = %v", err)
			}
			if _, err := svc.UnreadCountForUser(ctx, invited); err != nil {
				t.Fatalf("UnreadCountForUser() error = %v", err)
			}
			if accepted {
				err = groupSvc.ApproveMember(ctx, invited, member.ID, invited.Username)
			} else {
				err = groupSvc.RejectMember(ctx, invited, member.ID)
			}
			if err != nil {
				t.Fatalf("respond invite error = %v", err)
			}
			items, err := svc.ListForUser(ctx, invited, 20)
			if err != nil {
				t.Fatalf("ListForUser() error = %v", err)
			}
			if len(items) != 0 {
				t.Fatalf("ListForUser() returned %d active notifications, want 0", len(items))
			}
		})
	}
}

type fakeNotificationRepository struct {
	items []*notification.Notification
}

func newFakeNotificationRepository() *fakeNotificationRepository {
	return &fakeNotificationRepository{items: make([]*notification.Notification, 0)}
}

func (r *fakeNotificationRepository) Create(_ context.Context, item *notification.Notification) error {
	r.items = append(r.items, cloneNotification(item))
	return nil
}

func (r *fakeNotificationRepository) UpsertByDedupeKey(_ context.Context, item *notification.Notification) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.DedupeKey) != "" {
		for i, current := range r.items {
			if current.DedupeKey == item.DedupeKey {
				createdAt := current.CreatedAt
				r.items[i] = cloneNotification(item)
				r.items[i].ID = current.ID
				r.items[i].CreatedAt = createdAt
				return nil
			}
		}
	}
	return r.Create(context.Background(), item)
}

func (r *fakeNotificationRepository) ListForUser(_ context.Context, userID string, limit int) ([]*notification.Notification, error) {
	result := make([]*notification.Notification, 0, len(r.items))
	for _, item := range r.items {
		if item == nil || item.ExpiresAt != nil {
			continue
		}
		if item.RecipientUserID == userID || item.RecipientRole == notification.RecipientRoleAll {
			result = append(result, cloneNotification(item))
		}
	}
	if limit > 0 && len(result) > limit {
		return result[:limit], nil
	}
	return result, nil
}

func (r *fakeNotificationRepository) ListForRole(_ context.Context, role string, limit int) ([]*notification.Notification, error) {
	result := make([]*notification.Notification, 0, len(r.items))
	for _, item := range r.items {
		if item == nil || item.ExpiresAt != nil || item.RecipientRole != role {
			continue
		}
		result = append(result, cloneNotification(item))
	}
	if limit > 0 && len(result) > limit {
		return result[:limit], nil
	}
	return result, nil
}

func (r *fakeNotificationRepository) UnreadCountForUser(_ context.Context, userID string) (int, error) {
	count := 0
	for _, item := range r.items {
		if item == nil || item.ReadAt != nil || item.ExpiresAt != nil {
			continue
		}
		if item.RecipientUserID == userID || item.RecipientRole == notification.RecipientRoleAll {
			count++
		}
	}
	return count, nil
}

func (r *fakeNotificationRepository) UnreadCountForRole(_ context.Context, role string) (int, error) {
	count := 0
	for _, item := range r.items {
		if item != nil && item.ReadAt == nil && item.ExpiresAt == nil && item.RecipientRole == role {
			count++
		}
	}
	return count, nil
}

func (r *fakeNotificationRepository) MarkReadForUser(_ context.Context, userID string, ids []string) error {
	idSet := notificationIDSet(ids)
	now := time.Now()
	for _, item := range r.items {
		if item != nil && idSet[item.ID] && item.RecipientUserID == userID {
			item.ReadAt = &now
		}
	}
	return nil
}

func (r *fakeNotificationRepository) MarkAllReadForUser(_ context.Context, userID string) error {
	now := time.Now()
	for _, item := range r.items {
		if item != nil && item.RecipientUserID == userID {
			item.ReadAt = &now
		}
	}
	return nil
}

func (r *fakeNotificationRepository) DismissByActionURLForUser(_ context.Context, userID, actionURL string) error {
	now := time.Now()
	for _, item := range r.items {
		if item != nil && item.RecipientUserID == userID && item.ActionURL == actionURL {
			item.ReadAt = &now
			item.ExpiresAt = &now
		}
	}
	return nil
}

func (r *fakeNotificationRepository) MarkReadForRole(_ context.Context, role string, ids []string) error {
	idSet := notificationIDSet(ids)
	now := time.Now()
	for _, item := range r.items {
		if item != nil && idSet[item.ID] && item.RecipientRole == role {
			item.ReadAt = &now
		}
	}
	return nil
}

func (r *fakeNotificationRepository) MarkAllReadForRole(_ context.Context, role string) error {
	now := time.Now()
	for _, item := range r.items {
		if item != nil && item.RecipientRole == role {
			item.ReadAt = &now
		}
	}
	return nil
}

func (r *fakeNotificationRepository) GetPreferences(context.Context, string) ([]notification.Preference, error) {
	return nil, nil
}

func (r *fakeNotificationRepository) SetPreference(context.Context, string, string, bool) error {
	return nil
}

func cloneNotification(item *notification.Notification) *notification.Notification {
	if item == nil {
		return nil
	}
	copied := *item
	return &copied
}

func notificationIDSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}
