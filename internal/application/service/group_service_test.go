package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yeying-community/warehouse/internal/domain/group"
	"github.com/yeying-community/warehouse/internal/domain/user"
)

func TestGroupServiceCreateGroupAddsOwnerAsActiveMember(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", Username: "Owner", WalletAddress: "0x1111111111111111111111111111111111111111"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	var ownerMember *group.Member
	for _, member := range repo.members {
		if member.GroupID == grp.ID && strings.EqualFold(member.WalletAddress, owner.WalletAddress) {
			ownerMember = member
			break
		}
	}
	if ownerMember == nil {
		t.Fatal("owner member was not created")
	}
	if ownerMember.Status != group.MemberStatusActive {
		t.Fatalf("owner member status = %q, want %q", ownerMember.Status, group.MemberStatusActive)
	}
}

func TestGroupServiceListGroupsRequiresApprovalBeforeTargetCanSeeGroup(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", Username: "Night Member", WalletAddress: "0x2222222222222222222222222222222222222222"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	groups, err := svc.ListGroups(ctx, invited)
	if err != nil {
		t.Fatalf("ListGroups() before invite error = %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("ListGroups() before invite returned %d groups, want 0", len(groups))
	}

	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if member.Name != "" {
		t.Fatalf("pending member name = %q, want empty", member.Name)
	}
	groups, err = svc.ListGroups(ctx, invited)
	if err != nil {
		t.Fatalf("ListGroups() after invite error = %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("ListGroups() after pending invite returned %d groups, want 0", len(groups))
	}

	if err := svc.ApproveMember(ctx, invited, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember() error = %v", err)
	}
	if got := repo.members[member.ID].Name; got != "Member Name" {
		t.Fatalf("approved member name = %q, want Member Name", got)
	}
	groups, err = svc.ListGroups(ctx, invited)
	if err != nil {
		t.Fatalf("ListGroups() after approval error = %v", err)
	}
	if len(groups) != 1 || groups[0].ID != grp.ID {
		t.Fatalf("ListGroups() after approval = %#v, want group %s", groups, grp.ID)
	}
}

func TestGroupServiceApproveMemberRequiresDisplayName(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}

	if err := svc.ApproveMember(ctx, invited, member.ID, ""); err == nil || err.Error() != "member name is required" {
		t.Fatalf("ApproveMember() error = %v, want member name is required", err)
	}
	if got := repo.members[member.ID].Status; got != group.MemberStatusPending {
		t.Fatalf("member status = %q, want pending", got)
	}
}

func TestGroupServiceRejectInviteHidesGroupFromTarget(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}

	if err := svc.RejectMember(ctx, invited, member.ID); err != nil {
		t.Fatalf("RejectMember() error = %v", err)
	}
	groups, err := svc.ListGroups(ctx, invited)
	if err != nil {
		t.Fatalf("ListGroups() after reject error = %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("ListGroups() after reject returned %d groups, want 0", len(groups))
	}
}

func TestGroupServiceCreateMemberResolvesUsernameAndStoresAlias(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	userRepo := newTestUserRepo()
	svc := NewGroupService(repo, userRepo)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	target := &user.User{
		ID:            "target-user",
		Username:      "target",
		WalletAddress: "0x5555555555555555555555555555555555555555",
	}
	if err := userRepo.Save(ctx, target); err != nil {
		t.Fatalf("Save(target) error = %v", err)
	}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{
		Target:  target.Username,
		Alias:   "Partner",
		GroupID: grp.ID,
	})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if member.WalletAddress != target.WalletAddress {
		t.Fatalf("member wallet = %q, want %q", member.WalletAddress, target.WalletAddress)
	}
	if member.Name != "" {
		t.Fatalf("member name = %q, want empty", member.Name)
	}
	if member.Alias != "Partner" {
		t.Fatalf("member alias = %q, want Partner", member.Alias)
	}

	ownerMembers, err := svc.ListMembers(ctx, owner)
	if err != nil {
		t.Fatalf("ListMembers(owner) error = %v", err)
	}
	targetMembers, err := svc.ListMembers(ctx, target)
	if err != nil {
		t.Fatalf("ListMembers(target) error = %v", err)
	}
	if got := findTestMemberAlias(ownerMembers, member.ID); got != "Partner" {
		t.Fatalf("owner alias = %q, want Partner", got)
	}
	if got := findTestMemberAlias(targetMembers, member.ID); got != "" {
		t.Fatalf("target alias = %q, want empty", got)
	}
}

func TestGroupServiceCreateMemberRejectsUnknownUsername(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, newTestUserRepo())
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	_, err = svc.CreateMember(ctx, owner, CreateMemberInput{Target: "missing-user", GroupID: grp.ID})
	if !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("CreateMember() error = %v, want %v", err, user.ErrUserNotFound)
	}
}

func TestGroupServiceCreateMemberAcceptsMixedCaseWalletAddress(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	target := "0x9AdD99615252CaF379030d8966965BD9e5D80157"

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: target, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if got, want := member.WalletAddress, strings.ToLower(target); got != want {
		t.Fatalf("member wallet = %q, want %q", got, want)
	}
}

func TestGroupServiceMemberInviteRequiresTargetConfirmation(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}
	other := &user.User{ID: "other-user", WalletAddress: "0x3333333333333333333333333333333333333333"}

	grp, err := group.NewGroup(owner.ID, "team")
	if err != nil {
		t.Fatalf("NewGroup() error = %v", err)
	}
	repo.groups[grp.ID] = grp

	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if member.Status != group.MemberStatusPending {
		t.Fatalf("CreateMember() status = %q, want %q", member.Status, group.MemberStatusPending)
	}

	if err := svc.ApproveMember(ctx, other, member.ID, "Member Name"); err != group.ErrMemberNotFound {
		t.Fatalf("ApproveMember() by unrelated wallet error = %v, want %v", err, group.ErrMemberNotFound)
	}
	if got := repo.members[member.ID].Status; got != group.MemberStatusPending {
		t.Fatalf("status after unrelated approve = %q, want pending", got)
	}

	if err := svc.ApproveMember(ctx, invited, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember() by invited wallet error = %v", err)
	}
	if got := repo.members[member.ID].Status; got != group.MemberStatusActive {
		t.Fatalf("status after invited approve = %q, want active", got)
	}
}

func TestGroupServiceActiveMemberInviteRequiresTargetConfirmation(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	memberUser := &user.User{ID: "member-user", WalletAddress: "0x4444444444444444444444444444444444444444"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: memberUser.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember(owner invite) error = %v", err)
	}
	if err := svc.ApproveMember(ctx, memberUser, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember(member) error = %v", err)
	}

	invite, err := svc.CreateMember(ctx, memberUser, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember(member invite) error = %v", err)
	}
	if invite.Status != group.MemberStatusPending {
		t.Fatalf("member invite status = %q, want %q", invite.Status, group.MemberStatusPending)
	}
	if err := svc.ApproveMember(ctx, owner, invite.ID, "Member Name"); err != group.ErrMemberNotFound {
		t.Fatalf("ApproveMember(owner) error = %v, want %v", err, group.ErrMemberNotFound)
	}
	if err := svc.ApproveMember(ctx, invited, invite.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember(invited) error = %v", err)
	}
	if got := repo.members[invite.ID].Status; got != group.MemberStatusActive {
		t.Fatalf("status after invited approve = %q, want %q", got, group.MemberStatusActive)
	}
}

func TestGroupServiceRejectActiveMemberInviteRequiresTargetWallet(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	memberUser := &user.User{ID: "member-user", WalletAddress: "0x4444444444444444444444444444444444444444"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: memberUser.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember(owner invite) error = %v", err)
	}
	if err := svc.ApproveMember(ctx, memberUser, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember(member) error = %v", err)
	}
	invite, err := svc.CreateMember(ctx, memberUser, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember(member invite) error = %v", err)
	}

	if err := svc.RejectMember(ctx, owner, invite.ID); err != group.ErrMemberNotFound {
		t.Fatalf("RejectMember(owner) error = %v, want %v", err, group.ErrMemberNotFound)
	}
	if _, ok := repo.members[invite.ID]; !ok {
		t.Fatal("invite deleted by owner wallet")
	}
	if err := svc.RejectMember(ctx, invited, invite.ID); err != nil {
		t.Fatalf("RejectMember(invited) error = %v", err)
	}
	if _, ok := repo.members[invite.ID]; ok {
		t.Fatal("invite still exists after invited reject")
	}
}

func TestGroupServiceRejectMemberInviteRequiresTargetWallet(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}
	other := &user.User{ID: "other-user", WalletAddress: "0x3333333333333333333333333333333333333333"}

	grp, err := group.NewGroup(owner.ID, "team")
	if err != nil {
		t.Fatalf("NewGroup() error = %v", err)
	}
	repo.groups[grp.ID] = grp

	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if err := svc.RejectMember(ctx, other, member.ID); err != group.ErrMemberNotFound {
		t.Fatalf("RejectMember() by unrelated wallet error = %v, want %v", err, group.ErrMemberNotFound)
	}
	if _, ok := repo.members[member.ID]; !ok {
		t.Fatal("member deleted by unrelated wallet")
	}

	if err := svc.RejectMember(ctx, invited, member.ID); err != nil {
		t.Fatalf("RejectMember() by invited wallet error = %v", err)
	}
	if _, ok := repo.members[member.ID]; ok {
		t.Fatal("member still exists after invited reject")
	}
}

func TestGroupServiceUpdateMemberNameRequiresOwnWallet(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	memberUser := &user.User{ID: "member-user", WalletAddress: "0x4444444444444444444444444444444444444444"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}
	other := &user.User{ID: "other-user", WalletAddress: "0x3333333333333333333333333333333333333333"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if err := svc.ApproveMember(ctx, invited, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember() error = %v", err)
	}
	groupMember, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: memberUser.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember(member) error = %v", err)
	}
	if err := svc.ApproveMember(ctx, memberUser, groupMember.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember(member) error = %v", err)
	}

	if _, err := svc.UpdateMemberName(ctx, other, member.ID, "Other Name"); err != group.ErrMemberNotFound {
		t.Fatalf("UpdateMemberName() by other user error = %v, want %v", err, group.ErrMemberNotFound)
	}
	if _, err := svc.UpdateMemberName(ctx, memberUser, member.ID, "Other Visible Name"); err != group.ErrGroupPermissionDenied {
		t.Fatalf("UpdateMemberName() by visible non-self user error = %v, want %v", err, group.ErrGroupPermissionDenied)
	}
	updated, err := svc.UpdateMemberName(ctx, invited, member.ID, "Night Member")
	if err != nil {
		t.Fatalf("UpdateMemberName() error = %v", err)
	}
	if updated.Name != "Night Member" || repo.members[member.ID].Name != "Night Member" {
		t.Fatalf("member name = %q / %q, want Night Member", updated.Name, repo.members[member.ID].Name)
	}
}

func TestGroupServiceMemberAliasIsViewerPrivate(t *testing.T) {
	ctx := context.Background()
	repo := newFakeGroupRepository()
	svc := NewGroupService(repo, nil)
	owner := &user.User{ID: "owner-user", WalletAddress: "0x1111111111111111111111111111111111111111"}
	invited := &user.User{ID: "invited-user", WalletAddress: "0x2222222222222222222222222222222222222222"}

	grp, err := svc.CreateGroup(ctx, owner, "team")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	member, err := svc.CreateMember(ctx, owner, CreateMemberInput{Target: invited.WalletAddress, GroupID: grp.ID})
	if err != nil {
		t.Fatalf("CreateMember() error = %v", err)
	}
	if err := svc.ApproveMember(ctx, invited, member.ID, "Member Name"); err != nil {
		t.Fatalf("ApproveMember() error = %v", err)
	}

	if _, err := svc.UpdateMemberAlias(ctx, invited, member.ID, "Self Alias"); err != group.ErrGroupPermissionDenied {
		t.Fatalf("UpdateMemberAlias() for self error = %v, want %v", err, group.ErrGroupPermissionDenied)
	}
	if _, err := svc.UpdateMemberAlias(ctx, owner, member.ID, "Client Alias"); err != nil {
		t.Fatalf("UpdateMemberAlias() error = %v", err)
	}
	ownerMembers, err := svc.ListMembers(ctx, owner)
	if err != nil {
		t.Fatalf("ListMembers(owner) error = %v", err)
	}
	invitedMembers, err := svc.ListMembers(ctx, invited)
	if err != nil {
		t.Fatalf("ListMembers(invited) error = %v", err)
	}
	if got := findTestMemberAlias(ownerMembers, member.ID); got != "Client Alias" {
		t.Fatalf("owner alias = %q, want Client Alias", got)
	}
	if got := findTestMemberAlias(invitedMembers, member.ID); got != "" {
		t.Fatalf("invited alias = %q, want empty", got)
	}
}

func findTestMemberAlias(members []*group.Member, id string) string {
	for _, member := range members {
		if member.ID == id {
			return member.Alias
		}
	}
	return ""
}

type fakeGroupRepository struct {
	groups  map[string]*group.Group
	members map[string]*group.Member
	aliases map[string]string
}

func newFakeGroupRepository() *fakeGroupRepository {
	return &fakeGroupRepository{
		groups:  make(map[string]*group.Group),
		members: make(map[string]*group.Member),
		aliases: make(map[string]string),
	}
}

func (r *fakeGroupRepository) CreateGroup(_ context.Context, grp *group.Group, ownerMember *group.Member) error {
	r.groups[grp.ID] = cloneGroup(grp)
	if ownerMember != nil {
		r.members[ownerMember.ID] = cloneMember(ownerMember)
	}
	return nil
}

func (r *fakeGroupRepository) GetGroupByID(_ context.Context, userID, groupID string) (*group.Group, error) {
	grp, ok := r.groups[groupID]
	if !ok || grp.UserID != userID {
		return nil, group.ErrGroupNotFound
	}
	return cloneGroup(grp), nil
}

func (r *fakeGroupRepository) GetVisibleGroupByID(_ context.Context, userID, walletAddress, groupID string) (*group.Group, error) {
	grp, ok := r.groups[groupID]
	if !ok {
		return nil, group.ErrGroupNotFound
	}
	if grp.UserID == userID {
		copied := cloneGroup(grp)
		copied.CanInvite = true
		return copied, nil
	}
	for _, member := range r.members {
		if member.GroupID == groupID && strings.EqualFold(member.WalletAddress, walletAddress) {
			copied := cloneGroup(grp)
			copied.CanInvite = r.isActiveGroupMember(groupID, walletAddress)
			return copied, nil
		}
	}
	return nil, group.ErrGroupNotFound
}

func (r *fakeGroupRepository) ListVisibleGroups(_ context.Context, userID, walletAddress string) ([]*group.Group, error) {
	groups := make([]*group.Group, 0, len(r.groups))
	for _, grp := range r.groups {
		if grp.UserID == userID {
			copied := cloneGroup(grp)
			copied.CanInvite = true
			groups = append(groups, copied)
			continue
		}
		for _, member := range r.members {
			if member.GroupID == grp.ID && member.Status == group.MemberStatusActive && strings.EqualFold(member.WalletAddress, walletAddress) {
				copied := cloneGroup(grp)
				copied.CanInvite = r.isActiveGroupMember(grp.ID, walletAddress)
				groups = append(groups, copied)
				break
			}
		}
	}
	return groups, nil
}

func (r *fakeGroupRepository) UpdateGroupName(_ context.Context, userID, groupID, name string) error {
	grp, ok := r.groups[groupID]
	if !ok || grp.UserID != userID {
		return group.ErrGroupNotFound
	}
	grp.Name = name
	return nil
}

func (r *fakeGroupRepository) DeleteGroup(_ context.Context, userID, groupID string) error {
	grp, ok := r.groups[groupID]
	if !ok || grp.UserID != userID {
		return group.ErrGroupNotFound
	}
	delete(r.groups, groupID)
	return nil
}

func (r *fakeGroupRepository) CreateMember(_ context.Context, member *group.Member) error {
	r.members[member.ID] = cloneMember(member)
	return nil
}

func (r *fakeGroupRepository) GetMemberByID(_ context.Context, userID, memberID string) (*group.Member, error) {
	member, ok := r.members[memberID]
	if !ok || member.UserID != userID {
		return nil, group.ErrMemberNotFound
	}
	return cloneMember(member), nil
}

func (r *fakeGroupRepository) ListVisibleMembers(_ context.Context, userID, walletAddress string) ([]*group.Member, error) {
	members := make([]*group.Member, 0, len(r.members))
	for _, member := range r.members {
		if member.UserID == userID ||
			strings.EqualFold(member.WalletAddress, walletAddress) ||
			(member.Status == group.MemberStatusActive && r.isActiveGroupMember(member.GroupID, walletAddress)) {
			copied := cloneMember(member)
			copied.Alias = r.aliases[aliasKey(userID, member.ID)]
			members = append(members, copied)
		}
	}
	return members, nil
}

func (r *fakeGroupRepository) isActiveGroupMember(groupID, walletAddress string) bool {
	for _, member := range r.members {
		if member.GroupID == groupID &&
			member.Status == group.MemberStatusActive &&
			strings.EqualFold(member.WalletAddress, walletAddress) {
			return true
		}
	}
	return false
}

func (r *fakeGroupRepository) UpdateMember(_ context.Context, member *group.Member) error {
	current, ok := r.members[member.ID]
	if !ok || current.UserID != member.UserID {
		return group.ErrMemberNotFound
	}
	r.members[member.ID] = cloneMember(member)
	return nil
}

func (r *fakeGroupRepository) UpdateMemberNameByWallet(_ context.Context, walletAddress, memberID, name string) error {
	member, ok := r.members[memberID]
	if !ok || !strings.EqualFold(member.WalletAddress, walletAddress) {
		return group.ErrMemberNotFound
	}
	member.Name = strings.TrimSpace(name)
	return nil
}

func (r *fakeGroupRepository) SetMemberAlias(_ context.Context, ownerUserID, memberID, alias string) error {
	if _, ok := r.members[memberID]; !ok {
		return group.ErrMemberNotFound
	}
	key := aliasKey(ownerUserID, memberID)
	alias = strings.TrimSpace(alias)
	if alias == "" {
		delete(r.aliases, key)
		return nil
	}
	r.aliases[key] = alias
	return nil
}

func (r *fakeGroupRepository) UpdateMemberStatusByWallet(_ context.Context, walletAddress, memberID, status, name string) error {
	member, ok := r.members[memberID]
	if !ok || member.Status != group.MemberStatusPending || !strings.EqualFold(member.WalletAddress, walletAddress) {
		return group.ErrMemberNotFound
	}
	member.Status = group.NormalizeMemberStatus(status)
	if name = strings.TrimSpace(name); name != "" {
		member.Name = name
	}
	return nil
}

func (r *fakeGroupRepository) DeleteMember(_ context.Context, userID, memberID string) error {
	member, ok := r.members[memberID]
	if !ok || member.UserID != userID {
		return group.ErrMemberNotFound
	}
	delete(r.members, memberID)
	return nil
}

func (r *fakeGroupRepository) DeleteMemberByWallet(_ context.Context, walletAddress, memberID string) error {
	member, ok := r.members[memberID]
	if !ok || member.Status != group.MemberStatusPending || !strings.EqualFold(member.WalletAddress, walletAddress) {
		return group.ErrMemberNotFound
	}
	delete(r.members, memberID)
	return nil
}

func cloneGroup(grp *group.Group) *group.Group {
	if grp == nil {
		return nil
	}
	copied := *grp
	return &copied
}

func cloneMember(member *group.Member) *group.Member {
	if member == nil {
		return nil
	}
	copied := *member
	return &copied
}

func aliasKey(ownerUserID, memberID string) string {
	return ownerUserID + "\x00" + memberID
}
