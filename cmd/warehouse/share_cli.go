package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/infrastructure/database"
)

// legacyShareGrant is deliberately limited to the fields needed to seed V3.
// The legacy tables remain the runtime authority until a later dual-read rollout.
type legacyShareGrant struct {
	ID, OwnerUserID, Path, Permissions, Status string
	IsDir                                      bool
	ExpiresAt                                  *time.Time
	CreatedAt, UpdatedAt                       time.Time
}

type shareResourceVerifyResult struct {
	Command          string `json:"command"`
	LegacyShares     int    `json:"legacy_shares"`
	Grants           int    `json:"grants"`
	Resources        int    `json:"resources"`
	MissingGrants    int    `json:"missing_grants"`
	MismatchedGrants int    `json:"mismatched_grants"`
}

type shareAudienceVerifyResult struct {
	Command           string `json:"command"`
	Audiences         int    `json:"audiences"`
	LinkedAudiences   int    `json:"linked_audiences"`
	MissingGrantLinks int    `json:"missing_grant_links"`
	MismatchedLinks   int    `json:"mismatched_links"`
}

func runShareCommand(args []string) error {
	if len(args) == 0 {
		printShareHelp()
		return nil
	}
	switch args[0] {
	case "backfill-resources":
		return runShareBackfillResources(args[1:])
	case "verify-resources":
		return runShareVerifyResources(args[1:])
	case "backfill-audiences":
		return runShareBackfillAudiences(args[1:])
	case "verify-audiences":
		return runShareVerifyAudiences(args[1:])
	case "help", "-h", "--help":
		printShareHelp()
		return nil
	default:
		return fmt.Errorf("unsupported share subcommand %q", args[0])
	}
}

func printShareHelp() {
	fmt.Println("Usage:")
	fmt.Println("  warehouse share backfill-resources -c config.yaml [--dry-run]")
	fmt.Println("  warehouse share verify-resources -c config.yaml")
	fmt.Println("  warehouse share backfill-audiences -c config.yaml [--dry-run]")
	fmt.Println("  warehouse share verify-audiences -c config.yaml")
}

func runShareBackfillResources(args []string) error {
	flags := pflag.NewFlagSet("share-backfill-resources", pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.BoolP("help", "h", false, "Show help")
	dryRun := flags.Bool("dry-run", false, "Report the backfill plan without writing")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		printShareHelp()
		return nil
	}
	_, db, err := buildShareDependencies(flags)
	if err != nil {
		return err
	}
	defer db.Close()
	items, err := loadLegacyShareGrants(context.Background(), db.DB)
	if err != nil {
		return err
	}
	uniqueResources := make(map[string]struct{}, len(items))
	for _, item := range items {
		uniqueResources[shareResourceKey(item.OwnerUserID, normalizeSharedResourcePath(item.Path), item.IsDir)] = struct{}{}
	}
	if !*dryRun {
		if err := backfillSharedResources(context.Background(), db.DB, items); err != nil {
			return err
		}
	}
	printPrettyJSONFromAny(map[string]any{"command": "share backfill-resources", "dry_run": *dryRun, "legacy_shares": len(items), "resources_planned": len(uniqueResources), "grants_planned": len(items)})
	return nil
}

func runShareVerifyResources(args []string) error {
	flags := pflag.NewFlagSet("share-verify-resources", pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.BoolP("help", "h", false, "Show help")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		printShareHelp()
		return nil
	}
	_, db, err := buildShareDependencies(flags)
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := verifySharedResources(context.Background(), db.DB)
	if err != nil {
		return err
	}
	printPrettyJSONFromAny(result)
	if result.MissingGrants != 0 || result.MismatchedGrants != 0 {
		return fmt.Errorf("shared resource reconciliation failed")
	}
	return nil
}

func runShareBackfillAudiences(args []string) error {
	flags := pflag.NewFlagSet("share-backfill-audiences", pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.BoolP("help", "h", false, "Show help")
	dryRun := flags.Bool("dry-run", false, "Report the backfill plan without writing")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		printShareHelp()
		return nil
	}
	_, db, err := buildShareDependencies(flags)
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := inspectShareAudienceLinks(context.Background(), db.DB)
	if err != nil {
		return err
	}
	if !*dryRun {
		if err := backfillShareAudienceGrants(context.Background(), db.DB); err != nil {
			return err
		}
	}
	printPrettyJSONFromAny(map[string]any{"command": "share backfill-audiences", "dry_run": *dryRun, "audiences": result.Audiences, "links_planned": result.MissingGrantLinks + result.MismatchedLinks})
	return nil
}

func runShareVerifyAudiences(args []string) error {
	flags := pflag.NewFlagSet("share-verify-audiences", pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.BoolP("help", "h", false, "Show help")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		printShareHelp()
		return nil
	}
	_, db, err := buildShareDependencies(flags)
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := inspectShareAudienceLinks(context.Background(), db.DB)
	if err != nil {
		return err
	}
	result.Command = "share verify-audiences"
	printPrettyJSONFromAny(result)
	if result.MissingGrantLinks != 0 || result.MismatchedLinks != 0 {
		return fmt.Errorf("share audience reconciliation failed")
	}
	return nil
}

func buildShareDependencies(flags *pflag.FlagSet) (*config.Config, *database.PostgresDB, error) {
	configFile, _ := flags.GetString("config")
	cfg, err := loadConfig(configFile, flags)
	if err != nil {
		return nil, nil, err
	}
	db, err := database.NewPostgresDB(cfg.Database)
	if err != nil {
		return nil, nil, fmt.Errorf("connect database: %w", err)
	}
	return cfg, db, nil
}

func loadLegacyShareGrants(ctx context.Context, db *sql.DB) ([]legacyShareGrant, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, owner_user_id, path, is_dir, permissions, expires_at, status, created_at, updated_at FROM internal_share_items ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query legacy shares: %w", err)
	}
	defer rows.Close()
	items := []legacyShareGrant{}
	for rows.Next() {
		var item legacyShareGrant
		if err := rows.Scan(&item.ID, &item.OwnerUserID, &item.Path, &item.IsDir, &item.Permissions, &item.ExpiresAt, &item.Status, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan legacy share: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy shares: %w", err)
	}
	return items, nil
}

func backfillSharedResources(ctx context.Context, db *sql.DB, items []legacyShareGrant) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin shared resource backfill: %w", err)
	}
	defer tx.Rollback()
	for _, item := range items {
		normalizedPath := normalizeSharedResourcePath(item.Path)
		resourceID := sharedResourceID(item.OwnerUserID, normalizedPath, item.IsDir)
		if _, err := tx.ExecContext(ctx, `INSERT INTO internal_shared_resources (id, owner_user_id, normalized_path, is_dir, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (owner_user_id, normalized_path, is_dir) DO NOTHING`, resourceID, item.OwnerUserID, normalizedPath, item.IsDir, item.CreatedAt, item.UpdatedAt); err != nil {
			return fmt.Errorf("insert shared resource for legacy share %s: %w", item.ID, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO internal_share_grants (id, resource_id, legacy_share_id, permissions, expires_at, status, created_at, updated_at) VALUES ($1, $2, $1, $3, $4, $5, $6, $7) ON CONFLICT (legacy_share_id) DO NOTHING`, item.ID, resourceID, item.Permissions, item.ExpiresAt, item.Status, item.CreatedAt, item.UpdatedAt); err != nil {
			return fmt.Errorf("insert grant for legacy share %s: %w", item.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit shared resource backfill: %w", err)
	}
	return nil
}

func verifySharedResources(ctx context.Context, db *sql.DB) (shareResourceVerifyResult, error) {
	result := shareResourceVerifyResult{Command: "share verify-resources"}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM internal_share_grants WHERE legacy_share_id IS NOT NULL`).Scan(&result.Grants); err != nil {
		return result, fmt.Errorf("count shared grants: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM internal_shared_resources`).Scan(&result.Resources); err != nil {
		return result, fmt.Errorf("count shared resources: %w", err)
	}
	rows, err := db.QueryContext(ctx, `SELECT s.id, s.owner_user_id, s.path, s.is_dir, g.id, r.owner_user_id, r.normalized_path, r.is_dir
		FROM internal_share_items s
		LEFT JOIN internal_share_grants g ON g.legacy_share_id = s.id
		LEFT JOIN internal_shared_resources r ON r.id = g.resource_id`)
	if err != nil {
		return result, fmt.Errorf("verify shared resources: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var legacyID, ownerUserID, legacyPath string
		var legacyIsDir bool
		var grantID, resourceOwnerID, normalizedPath sql.NullString
		var resourceIsDir sql.NullBool
		if err := rows.Scan(&legacyID, &ownerUserID, &legacyPath, &legacyIsDir, &grantID, &resourceOwnerID, &normalizedPath, &resourceIsDir); err != nil {
			return result, fmt.Errorf("scan shared resource reconciliation row: %w", err)
		}
		result.LegacyShares++
		if !grantID.Valid {
			result.MissingGrants++
			continue
		}
		if !resourceOwnerID.Valid || !normalizedPath.Valid || !resourceIsDir.Valid || resourceOwnerID.String != ownerUserID || normalizedPath.String != normalizeSharedResourcePath(legacyPath) || resourceIsDir.Bool != legacyIsDir {
			result.MismatchedGrants++
		}
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate shared resource reconciliation rows: %w", err)
	}
	return result, nil
}

func backfillShareAudienceGrants(ctx context.Context, db *sql.DB) error {
	result, err := db.ExecContext(ctx, `UPDATE internal_share_audiences a SET grant_id = g.id FROM internal_share_grants g WHERE g.legacy_share_id = a.share_id AND a.grant_id IS DISTINCT FROM g.id`)
	if err != nil {
		return fmt.Errorf("backfill audience grant links: %w", err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count audience grant links: %w", err)
	}
	return nil
}

func inspectShareAudienceLinks(ctx context.Context, db *sql.DB) (shareAudienceVerifyResult, error) {
	result := shareAudienceVerifyResult{Command: "share backfill-audiences"}
	const query = `SELECT count(*),
		count(*) FILTER (WHERE a.grant_id IS NOT NULL),
		count(*) FILTER (WHERE g.id IS NULL),
		count(*) FILTER (WHERE g.id IS NOT NULL AND a.grant_id IS DISTINCT FROM g.id)
		FROM internal_share_audiences a
		LEFT JOIN internal_share_grants g ON g.legacy_share_id = a.share_id`
	if err := db.QueryRowContext(ctx, query).Scan(&result.Audiences, &result.LinkedAudiences, &result.MissingGrantLinks, &result.MismatchedLinks); err != nil {
		return result, fmt.Errorf("inspect audience grant links: %w", err)
	}
	return result, nil
}

func normalizeSharedResourcePath(raw string) string {
	clean := path.Clean("/" + strings.TrimLeft(strings.TrimSpace(raw), "/"))
	if clean == "." {
		return "/"
	}
	return clean
}

func shareResourceKey(ownerUserID, normalizedPath string, isDir bool) string {
	return fmt.Sprintf("%s\x00%s\x00%t", ownerUserID, normalizedPath, isDir)
}

func sharedResourceID(ownerUserID, normalizedPath string, isDir bool) string {
	sum := sha256.Sum256([]byte(shareResourceKey(ownerUserID, normalizedPath, isDir)))
	return fmt.Sprintf("shr_%x", sum[:20])
}
