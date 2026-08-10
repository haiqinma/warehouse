package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/sharegrant"
)

// SharedResourceGrantRepository is intentionally separate from
// UserShareRepository while V3 is being introduced alongside the legacy path.
type SharedResourceGrantRepository interface {
	GetAccessibleGrants(ctx context.Context, resourceID, targetUserID string) (*sharegrant.Resource, []sharegrant.Grant, error)
	GetResourceIDByLegacyShareID(ctx context.Context, legacyShareID string) (string, error)
	ListReceivedResources(ctx context.Context, targetUserID string, now time.Time) ([]sharegrant.ReceivedResource, error)
	MoveOwnerPaths(ctx context.Context, ownerUserID, fromPath, toPath string) error
	DeleteOwnerPaths(ctx context.Context, ownerUserID, targetPath string) error
}

// MoveOwnerPaths keeps resource identities aligned when a shared user moves a
// nested path. The root resource currently being authorized is never movable,
// but independently shared descendants may be.
func (r *PostgresSharedResourceGrantRepository) MoveOwnerPaths(ctx context.Context, ownerUserID, fromPath, toPath string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE internal_shared_resources
		SET normalized_path = $3 || substr(normalized_path, length($2) + 1), updated_at = NOW()
		WHERE owner_user_id = $1 AND (normalized_path = $2 OR left(normalized_path, length($2) + 1) = $2 || '/')`, ownerUserID, fromPath, toPath)
	if err != nil {
		return fmt.Errorf("move shared resource paths: %w", err)
	}
	return nil
}

// DeleteOwnerPaths removes resources rooted at a deleted path. Cascading
// foreign keys remove grants and audiences, so no stale resource can remain
// accessible until the next startup reconciliation.
func (r *PostgresSharedResourceGrantRepository) DeleteOwnerPaths(ctx context.Context, ownerUserID, targetPath string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM internal_shared_resources
		WHERE owner_user_id = $1 AND (normalized_path = $2 OR left(normalized_path, length($2) + 1) = $2 || '/')`, ownerUserID, targetPath)
	if err != nil {
		return fmt.Errorf("delete shared resource paths: %w", err)
	}
	return nil
}

// ListReceivedResources returns one row per V3 resource, never one row per
// historical sharing action.
func (r *PostgresSharedResourceGrantRepository) ListReceivedResources(ctx context.Context, targetUserID string, now time.Time) ([]sharegrant.ReceivedResource, error) {
	const query = `SELECT r.id,r.owner_user_id,u.username,r.normalized_path,r.is_dir,g.id,g.permissions,g.expires_at,g.status,g.created_at
		FROM internal_shared_resources r JOIN users u ON u.id=r.owner_user_id JOIN internal_share_grants g ON g.resource_id=r.id
		WHERE r.owner_user_id<>$1 AND EXISTS (SELECT 1 FROM internal_share_audiences a WHERE a.grant_id=g.id AND (a.audience_type='all_users' OR (a.audience_type='user' AND a.source_group_id IS NULL AND a.target_user_id=$1) OR EXISTS (SELECT 1 FROM users tu JOIN group_members gm ON gm.group_id=a.source_group_id AND gm.status='active' AND TRIM(COALESCE(tu.wallet_address,''))<>'' AND LOWER(gm.wallet_address)=LOWER(tu.wallet_address) WHERE tu.id=$1)))
		ORDER BY r.created_at DESC,g.created_at ASC`
	rows, err := r.db.QueryContext(ctx, query, targetUserID)
	if err != nil {
		return nil, fmt.Errorf("query received shared resources: %w", err)
	}
	defer rows.Close()
	resources := map[string]*sharegrant.ReceivedResource{}
	grants := map[string][]sharegrant.Grant{}
	order := []string{}
	for rows.Next() {
		var resource sharegrant.ReceivedResource
		var grant sharegrant.Grant
		var expiry sql.NullTime
		if err := rows.Scan(&resource.ID, &resource.OwnerUserID, &resource.OwnerUsername, &resource.NormalizedPath, &resource.IsDir, &grant.ID, &grant.Permissions, &expiry, &grant.Status, &resource.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan received shared resource: %w", err)
		}
		if expiry.Valid {
			value := expiry.Time
			grant.ExpiresAt = &value
		}
		if _, ok := resources[resource.ID]; !ok {
			resources[resource.ID] = &resource
			order = append(order, resource.ID)
		}
		grants[resource.ID] = append(grants[resource.ID], grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate received shared resources: %w", err)
	}
	result := make([]sharegrant.ReceivedResource, 0, len(order))
	for _, id := range order {
		item := resources[id]
		effective := make([]sharegrant.Grant, 0, len(grants[id]))
		for _, grant := range grants[id] {
			if grant.IsEffective(now) {
				effective = append(effective, grant)
			}
		}
		if len(effective) == 0 || !sharegrant.Allows(effective, "read", now) {
			continue
		}
		item.GrantCount = len(effective)
		item.Permissions = sharegrant.EffectivePermissions(effective, now).String()
		result = append(result, *item)
	}
	return result, nil
}

func (r *PostgresSharedResourceGrantRepository) GetResourceIDByLegacyShareID(ctx context.Context, legacyShareID string) (string, error) {
	var resourceID string
	err := r.db.QueryRowContext(ctx, `SELECT resource_id FROM internal_share_grants WHERE legacy_share_id = $1`, legacyShareID).Scan(&resourceID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("resolve resource for legacy share: %w", err)
	}
	return resourceID, nil
}

type PostgresSharedResourceGrantRepository struct{ db *sql.DB }

func NewPostgresSharedResourceGrantRepository(db *sql.DB) *PostgresSharedResourceGrantRepository {
	return &PostgresSharedResourceGrantRepository{db: db}
}

// GetAccessibleGrants resolves direct-user, all-user and dynamic group
// audiences. The query intentionally returns policy state as stored; callers
// must use sharegrant.Allows at operation time to enforce expiry and status.
func (r *PostgresSharedResourceGrantRepository) GetAccessibleGrants(ctx context.Context, resourceID, targetUserID string) (*sharegrant.Resource, []sharegrant.Grant, error) {
	const query = `SELECT r.id, r.owner_user_id, r.normalized_path, r.is_dir,
		g.id, g.permissions, g.expires_at, g.status
		FROM internal_shared_resources r
		JOIN internal_share_grants g ON g.resource_id = r.id
		WHERE r.id = $1
		AND (r.owner_user_id = $2 OR EXISTS (
			SELECT 1
			FROM internal_share_audiences a
			WHERE a.grant_id = g.id AND (
				a.audience_type = 'all_users'
				OR (a.audience_type = 'user' AND a.source_group_id IS NULL AND a.target_user_id = $2)
				OR EXISTS (
					SELECT 1 FROM users u
					JOIN group_members gm ON gm.group_id = a.source_group_id
						AND gm.status = 'active'
						AND TRIM(COALESCE(u.wallet_address, '')) <> ''
						AND LOWER(gm.wallet_address) = LOWER(u.wallet_address)
					WHERE u.id = $2
				)
			)
		))
		ORDER BY g.created_at ASC`
	rows, err := r.db.QueryContext(ctx, query, resourceID, targetUserID)
	if err != nil {
		return nil, nil, fmt.Errorf("query accessible shared resource grants: %w", err)
	}
	defer rows.Close()
	var resource *sharegrant.Resource
	grants := []sharegrant.Grant{}
	for rows.Next() {
		var current sharegrant.Resource
		var grant sharegrant.Grant
		var expiresAt sql.NullTime
		if err := rows.Scan(&current.ID, &current.OwnerUserID, &current.NormalizedPath, &current.IsDir, &grant.ID, &grant.Permissions, &expiresAt, &grant.Status); err != nil {
			return nil, nil, fmt.Errorf("scan accessible shared resource grant: %w", err)
		}
		if expiresAt.Valid {
			value := expiresAt.Time
			grant.ExpiresAt = &value
		}
		if resource == nil {
			resource = &current
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate accessible shared resource grants: %w", err)
	}
	return resource, grants, nil
}

var _ SharedResourceGrantRepository = (*PostgresSharedResourceGrantRepository)(nil)
