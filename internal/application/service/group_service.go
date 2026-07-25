package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yeying-community/warehouse/internal/domain/group"
	"github.com/yeying-community/warehouse/internal/domain/user"
	"github.com/yeying-community/warehouse/internal/infrastructure/repository"
)

type GroupService struct {
	repo         repository.GroupRepository
	userRepo     user.Repository
	notification *NotificationService
}

type CreateMemberInput struct {
	Target  string
	Alias   string
	GroupID string
}

func NewGroupService(repo repository.GroupRepository, userRepo user.Repository) *GroupService {
	return &GroupService{repo: repo, userRepo: userRepo}
}

func (s *GroupService) SetNotificationService(notification *NotificationService) {
	if s == nil {
		return
	}
	s.notification = notification
}

func (s *GroupService) ListGroups(ctx context.Context, u *user.User) ([]*group.Group, error) {
	return s.repo.ListVisibleGroups(ctx, u.ID, u.WalletAddress)
}

func (s *GroupService) CreateGroup(ctx context.Context, u *user.User, name string) (*group.Group, error) {
	grp, err := group.NewGroup(u.ID, name)
	if err != nil {
		return nil, err
	}
	ownerMemberName := defaultMemberName(u)
	var ownerMember *group.Member
	if strings.TrimSpace(u.WalletAddress) != "" {
		ownerMember, err = group.NewMember(u.ID, grp.ID, ownerMemberName, u.WalletAddress)
		if err != nil {
			return nil, err
		}
		ownerMember.Status = group.MemberStatusActive
	}
	if err := s.repo.CreateGroup(ctx, grp, ownerMember); err != nil {
		return nil, err
	}
	return grp, nil
}

func (s *GroupService) RenameGroup(ctx context.Context, u *user.User, groupID, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("group name is required")
	}
	return s.repo.UpdateGroupName(ctx, u.ID, groupID, name)
}

func (s *GroupService) DeleteGroup(ctx context.Context, u *user.User, groupID string) error {
	return s.repo.DeleteGroup(ctx, u.ID, groupID)
}

func (s *GroupService) ListMembers(ctx context.Context, u *user.User) ([]*group.Member, error) {
	return s.repo.ListVisibleMembers(ctx, u.ID, u.WalletAddress)
}

func (s *GroupService) CreateMember(ctx context.Context, u *user.User, input CreateMemberInput) (*group.Member, error) {
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return nil, fmt.Errorf("group id is required")
	}
	targetGroup, err := s.repo.GetVisibleGroupByID(ctx, u.ID, u.WalletAddress, groupID)
	if err != nil {
		return nil, err
	}
	if !targetGroup.CanInvite {
		return nil, group.ErrGroupPermissionDenied
	}

	wallet, err := s.resolveMemberTarget(ctx, input.Target)
	if err != nil {
		return nil, err
	}
	memberName := ""
	if strings.EqualFold(strings.TrimSpace(u.WalletAddress), wallet) {
		memberName = defaultMemberName(u)
	}
	member, err := group.NewMember(targetGroup.UserID, groupID, memberName, wallet)
	if err != nil {
		return nil, err
	}
	member.Status = group.MemberStatusPending
	if err := s.repo.CreateMember(ctx, member); err != nil {
		return nil, err
	}
	alias := strings.TrimSpace(input.Alias)
	if alias != "" && !strings.EqualFold(strings.TrimSpace(u.WalletAddress), wallet) {
		if err := s.repo.SetMemberAlias(ctx, u.ID, member.ID, alias); err != nil {
			return nil, err
		}
		member.Alias = alias
	}
	if s.notification != nil {
		s.notification.NotifyGroupInvite(ctx, u, member, targetGroup.Name)
	}
	return member, nil
}

func (s *GroupService) resolveMemberTarget(ctx context.Context, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("username or wallet address is required")
	}
	if isWalletAddress(target) {
		return strings.ToLower(target), nil
	}
	if s.userRepo == nil {
		return "", fmt.Errorf("user repository is required")
	}
	targetUser, err := s.userRepo.FindByUsername(ctx, target)
	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return "", user.ErrUserNotFound
		}
		return "", err
	}
	wallet := strings.TrimSpace(targetUser.WalletAddress)
	if wallet == "" {
		return "", fmt.Errorf("user has no wallet address")
	}
	return strings.ToLower(wallet), nil
}

func isWalletAddress(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 42 || !strings.HasPrefix(value, "0x") {
		return false
	}
	for _, ch := range value[2:] {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') {
			continue
		}
		return false
	}
	return true
}

func (s *GroupService) UpdateMember(ctx context.Context, u *user.User, id, name, wallet, groupID string) (*group.Member, error) {
	member, err := s.repo.GetMemberByID(ctx, u.ID, id)
	if err != nil {
		return nil, err
	}
	originalWallet := member.WalletAddress
	originalGroupID := member.GroupID
	if strings.TrimSpace(name) != "" {
		member.Name = strings.TrimSpace(name)
	}
	if strings.TrimSpace(wallet) != "" {
		member.WalletAddress = strings.ToLower(strings.TrimSpace(wallet))
	}
	if strings.TrimSpace(groupID) != "" {
		if _, err := s.repo.GetGroupByID(ctx, u.ID, groupID); err != nil {
			return nil, err
		}
		member.GroupID = groupID
	}
	if !strings.EqualFold(originalWallet, member.WalletAddress) || originalGroupID != member.GroupID {
		member.Status = group.MemberStatusPending
	}
	if err := s.repo.UpdateMember(ctx, member); err != nil {
		return nil, err
	}
	return member, nil
}

func (s *GroupService) UpdateMemberName(ctx context.Context, u *user.User, id, name string) (*group.Member, error) {
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)
	if id == "" {
		return nil, group.ErrMemberNotFound
	}
	if name == "" {
		return nil, fmt.Errorf("member name is required")
	}
	member, err := s.findVisibleMember(ctx, u, id)
	if err != nil {
		return nil, err
	}
	userWallet := strings.TrimSpace(u.WalletAddress)
	memberWallet := strings.TrimSpace(member.WalletAddress)
	if userWallet == "" || memberWallet == "" || !strings.EqualFold(userWallet, memberWallet) {
		return nil, group.ErrGroupPermissionDenied
	}
	if err := s.repo.UpdateMemberNameByWallet(ctx, userWallet, id, name); err != nil {
		return nil, err
	}
	member.Name = name
	return member, nil
}

func (s *GroupService) UpdateMemberAlias(ctx context.Context, u *user.User, id, alias string) (*group.Member, error) {
	id = strings.TrimSpace(id)
	alias = strings.TrimSpace(alias)
	if id == "" {
		return nil, group.ErrMemberNotFound
	}
	member, err := s.findVisibleMember(ctx, u, id)
	if err != nil {
		return nil, err
	}
	userWallet := strings.TrimSpace(u.WalletAddress)
	memberWallet := strings.TrimSpace(member.WalletAddress)
	if userWallet == "" || memberWallet == "" || strings.EqualFold(userWallet, memberWallet) {
		return nil, group.ErrGroupPermissionDenied
	}
	if err := s.repo.SetMemberAlias(ctx, u.ID, id, alias); err != nil {
		return nil, err
	}
	member.Alias = alias
	return member, nil
}

func (s *GroupService) findVisibleMember(ctx context.Context, u *user.User, id string) (*group.Member, error) {
	members, err := s.repo.ListVisibleMembers(ctx, u.ID, u.WalletAddress)
	if err != nil {
		return nil, err
	}
	for _, member := range members {
		if member.ID == id {
			return member, nil
		}
	}
	return nil, group.ErrMemberNotFound
}

func (s *GroupService) DeleteMember(ctx context.Context, u *user.User, id string) error {
	return s.repo.DeleteMember(ctx, u.ID, id)
}

func (s *GroupService) ApproveMember(ctx context.Context, u *user.User, id, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultMemberName(u)
	}
	if name == "" {
		return fmt.Errorf("member name is required")
	}
	if err := s.repo.UpdateMemberStatusByWallet(ctx, u.WalletAddress, id, group.MemberStatusActive, name); err != nil {
		return err
	}
	if s.notification != nil {
		s.notification.DismissGroupInvite(ctx, u, id)
	}
	return nil
}

func defaultMemberName(u *user.User) string {
	if u == nil {
		return ""
	}
	return strings.TrimSpace(u.Username)
}

func (s *GroupService) RejectMember(ctx context.Context, u *user.User, id string) error {
	if err := s.repo.DeleteMemberByWallet(ctx, u.WalletAddress, id); err != nil {
		return err
	}
	if s.notification != nil {
		s.notification.DismissGroupInvite(ctx, u, id)
	}
	return nil
}
