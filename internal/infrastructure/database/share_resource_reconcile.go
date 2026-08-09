package database

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path"
	"strings"
	"time"
)

const shareResourceReconcileLockID int64 = 846273910528

type legacySharedResourceGrant struct {
	id, ownerUserID, resourcePath, permissions, status string
	isDir                                              bool
	expiresAt                                          *time.Time
	createdAt, updatedAt                               time.Time
}

// ReconcileSharedResources makes the V3 projection complete before this
// process starts serving traffic. It is safe to run on every startup.
func (p *PostgresDB) ReconcileSharedResources(ctx context.Context) error {
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin shared resource reconciliation: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, shareResourceReconcileLockID); err != nil {
		return fmt.Errorf("lock shared resource reconciliation: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, owner_user_id, path, is_dir, permissions, expires_at, status, created_at, updated_at FROM internal_share_items ORDER BY id`)
	if err != nil {
		return fmt.Errorf("load legacy shares: %w", err)
	}
	items := []legacySharedResourceGrant{}
	for rows.Next() {
		var item legacySharedResourceGrant
		if err := rows.Scan(&item.id, &item.ownerUserID, &item.resourcePath, &item.isDir, &item.permissions, &item.expiresAt, &item.status, &item.createdAt, &item.updatedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan legacy share: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close legacy share rows: %w", err)
	}
	for _, item := range items {
		normalizedPath := normalizeSharedResourcePath(item.resourcePath)
		resourceID := sharedResourceID(item.ownerUserID, normalizedPath, item.isDir)
		if _, err := tx.ExecContext(ctx, `INSERT INTO internal_shared_resources (id, owner_user_id, normalized_path, is_dir, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (owner_user_id, normalized_path, is_dir) DO NOTHING`, resourceID, item.ownerUserID, normalizedPath, item.isDir, item.createdAt, item.updatedAt); err != nil {
			return fmt.Errorf("upsert shared resource %s: %w", item.id, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO internal_share_grants (id, resource_id, legacy_share_id, permissions, expires_at, status, created_at, updated_at) VALUES ($1,$2,$1,$3,$4,$5,$6,$7) ON CONFLICT (legacy_share_id) DO NOTHING`, item.id, resourceID, item.permissions, item.expiresAt, item.status, item.createdAt, item.updatedAt); err != nil {
			return fmt.Errorf("upsert shared grant %s: %w", item.id, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE internal_share_audiences a SET grant_id = g.id FROM internal_share_grants g WHERE g.legacy_share_id = a.share_id AND a.grant_id IS DISTINCT FROM g.id`); err != nil {
		return fmt.Errorf("link shared audiences: %w", err)
	}
	var missingGrants, mismatchedResources, missingAudienceLinks, mismatchedAudienceLinks int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM internal_share_items s LEFT JOIN internal_share_grants g ON g.legacy_share_id=s.id WHERE g.id IS NULL`).Scan(&missingGrants); err != nil {
		return fmt.Errorf("verify shared grants: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM internal_share_items s JOIN internal_share_grants g ON g.legacy_share_id=s.id JOIN internal_shared_resources r ON r.id=g.resource_id WHERE r.owner_user_id<>s.owner_user_id OR r.is_dir<>s.is_dir`).Scan(&mismatchedResources); err != nil {
		return fmt.Errorf("verify shared resources: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE g.id IS NULL), count(*) FILTER (WHERE g.id IS NOT NULL AND a.grant_id IS DISTINCT FROM g.id) FROM internal_share_audiences a LEFT JOIN internal_share_grants g ON g.legacy_share_id=a.share_id`).Scan(&missingAudienceLinks, &mismatchedAudienceLinks); err != nil {
		return fmt.Errorf("verify shared audiences: %w", err)
	}
	if missingGrants != 0 || mismatchedResources != 0 || missingAudienceLinks != 0 || mismatchedAudienceLinks != 0 {
		return fmt.Errorf("shared resource reconciliation incomplete: missing_grants=%d mismatched_resources=%d missing_audience_links=%d mismatched_audience_links=%d", missingGrants, mismatchedResources, missingAudienceLinks, mismatchedAudienceLinks)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit shared resource reconciliation: %w", err)
	}
	return nil
}

func normalizeSharedResourcePath(raw string) string {
	return path.Clean("/" + strings.TrimLeft(strings.TrimSpace(raw), "/"))
}

func sharedResourceID(ownerUserID, normalizedPath string, isDir bool) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%t", ownerUserID, normalizedPath, isDir)))
	return fmt.Sprintf("shr_%x", sum[:20])
}
