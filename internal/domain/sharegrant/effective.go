// Package sharegrant contains the authorization semantics for V3 share grants.
// It deliberately has no database or HTTP dependency so every caller applies
// the same active/expiry and permission rules.
package sharegrant

import (
	"strings"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/user"
)

const StatusActive = "active"

// Grant is the policy portion of a shared resource authorization.
type Grant struct {
	ID          string
	Permissions string
	ExpiresAt   *time.Time
	Status      string
}

// Resource identifies the V3 object on which grants are evaluated.
type Resource struct {
	ID             string
	OwnerUserID    string
	NormalizedPath string
	IsDir          bool
}

// ReceivedResource is the single row shown for a recipient's shared resource.
type ReceivedResource struct {
	Resource
	OwnerUsername string
	GrantCount    int
	Permissions   string
	CreatedAt     time.Time
}

// IsEffective reports whether the grant can currently authorize any action.
func (g Grant) IsEffective(now time.Time) bool {
	if !strings.EqualFold(strings.TrimSpace(g.Status), StatusActive) {
		return false
	}
	return g.ExpiresAt == nil || !now.After(*g.ExpiresAt)
}

// EffectivePermissions returns the display-only union of currently effective
// grants. Callers authorizing an operation should use Allows, never cache this
// result across a request or grant lifecycle change.
func EffectivePermissions(grants []Grant, now time.Time) *user.Permissions {
	permissions := &user.Permissions{}
	for _, grant := range grants {
		if !grant.IsEffective(now) {
			continue
		}
		current := permissionsForGrant(grant.Permissions)
		permissions.Create = permissions.Create || current.Create
		permissions.Read = permissions.Read || current.Read
		permissions.Update = permissions.Update || current.Update
		permissions.Delete = permissions.Delete || current.Delete
	}
	return permissions
}

// Allows evaluates an action against currently effective grants. It is the
// operation-time guard required by V3; an expired or revoked grant contributes
// no permission even if an older list response displayed it as effective.
func Allows(grants []Grant, action string, now time.Time) bool {
	for _, grant := range grants {
		if grant.IsEffective(now) && permissionsForGrant(grant.Permissions).Has(action) {
			return true
		}
	}
	return false
}

func permissionsForGrant(raw string) *user.Permissions {
	if strings.TrimSpace(raw) == "" {
		return user.DefaultPermissions()
	}
	return user.ParsePermissions(raw)
}
