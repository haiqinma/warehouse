package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yeying-community/warehouse/internal/domain/sharegrant"
)

// SharedResourceGrantRepository is intentionally separate from
// UserShareRepository while V3 is being introduced alongside the legacy path.
type SharedResourceGrantRepository interface {
	GetAccessibleGrants(ctx context.Context, resourceID, targetUserID string) (*sharegrant.Resource, []sharegrant.Grant, error)
	GetResourceIDByLegacyShareID(ctx context.Context, legacyShareID string) (string, error)
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
