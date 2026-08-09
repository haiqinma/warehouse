package sharegrant

import (
	"testing"
	"time"
)

func TestAllowsRequiresAnEffectiveGrantForTheRequestedAction(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Second)
	grants := []Grant{
		{ID: "read", Permissions: "R", Status: StatusActive},
		{ID: "expired-update", Permissions: "U", Status: StatusActive, ExpiresAt: &expired},
		{ID: "revoked-delete", Permissions: "D", Status: "revoked"},
	}
	if !Allows(grants, "read", now) {
		t.Fatal("read grant should authorize read")
	}
	if Allows(grants, "update", now) {
		t.Fatal("expired update grant must not authorize update")
	}
	if Allows(grants, "delete", now) {
		t.Fatal("revoked delete grant must not authorize delete")
	}
}

func TestEffectivePermissionsIsOnlyAUnionOfCurrentGrants(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	grants := []Grant{{Permissions: "R", Status: StatusActive}, {Permissions: "CU", Status: StatusActive}, {Permissions: "D", Status: "disabled"}}
	permissions := EffectivePermissions(grants, now)
	if !permissions.Create || !permissions.Read || !permissions.Update || permissions.Delete {
		t.Fatalf("unexpected effective permissions: %s", permissions.String())
	}
}

func TestBlankPermissionPreservesLegacyReadDefault(t *testing.T) {
	now := time.Now()
	if !Allows([]Grant{{Status: StatusActive}}, "read", now) {
		t.Fatal("blank legacy permission should preserve read default")
	}
}
