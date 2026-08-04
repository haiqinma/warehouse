package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	appservice "github.com/yeying-community/warehouse/internal/application/service"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"github.com/yeying-community/warehouse/internal/infrastructure/database"
)

type recycleBackfillItem struct {
	ID        string
	Hash      string
	UserID    string
	Username  string
	Directory string
	Name      string
	Path      string
	IsDir     bool
	Size      int64
	DeletedAt time.Time
}

func runRecycleCommand(args []string) error {
	if len(args) == 0 {
		printRecycleHelp()
		return nil
	}

	switch args[0] {
	case "backfill-is-dir":
		return runRecycleBackfillIsDir(args[1:])
	case "clean-sync-artifacts":
		return runRecycleCleanSyncArtifacts(args[1:])
	case "-h", "--help", "help":
		printRecycleHelp()
		return nil
	default:
		return fmt.Errorf("unsupported recycle subcommand %q", args[0])
	}
}

func printRecycleHelp() {
	fmt.Println("Usage:")
	fmt.Println("  warehouse recycle backfill-is-dir -c config.yaml [--dry-run] [--limit N]")
	fmt.Println("  warehouse recycle clean-sync-artifacts -c config.yaml [--dry-run] [--limit N]")
}

func runRecycleBackfillIsDir(args []string) error {
	flags := pflag.NewFlagSet("recycle-backfill-is-dir", pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.BoolP("help", "h", false, "Show help")
	dryRun := flags.Bool("dry-run", false, "Only show what would be updated")
	limit := flags.Int("limit", 0, "Only process the first N recycle records")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		fmt.Println("Usage:")
		fmt.Println("  warehouse recycle backfill-is-dir -c config.yaml [--dry-run] [--limit N]")
		return nil
	}

	cfg, db, err := buildRecycleDependencies(flags)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	items, err := loadRecycleBackfillItems(ctx, db.DB, *limit)
	if err != nil {
		return err
	}

	recycleDir := filepath.Join(cfg.WebDAV.Directory, ".recycle")
	legacyFiles, err := loadLegacyRecycleCandidates(recycleDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("scan recycle dir: %w", err)
	}

	updated := 0
	unchanged := 0
	unresolved := 0
	skippedMissing := 0

	for _, item := range items {
		recyclePath, err := resolveRecycleBackfillPath(recycleDir, legacyFiles, item)
		if err != nil {
			if os.IsNotExist(err) {
				skippedMissing++
				continue
			}
			return fmt.Errorf("resolve recycle path for %s: %w", item.Hash, err)
		}

		info, err := os.Stat(recyclePath)
		if err != nil {
			if os.IsNotExist(err) {
				unresolved++
				continue
			}
			return fmt.Errorf("stat recycle path for %s: %w", item.Hash, err)
		}
		actualIsDir := info.IsDir()
		if actualIsDir == item.IsDir {
			unchanged++
			continue
		}

		if !*dryRun {
			if err := updateRecycleItemIsDir(ctx, db.DB, item.Hash, actualIsDir); err != nil {
				return err
			}
		}
		updated++
	}

	printPrettyJSONFromAny(map[string]any{
		"command":           "recycle backfill-is-dir",
		"dry_run":           *dryRun,
		"recycle_dir":       recycleDir,
		"scanned":           len(items),
		"updated":           updated,
		"unchanged":         unchanged,
		"unresolved":        unresolved,
		"skipped_missing":   skippedMissing,
		"legacy_candidates": len(legacyFiles),
	})
	return nil
}

func runRecycleCleanSyncArtifacts(args []string) error {
	flags := pflag.NewFlagSet("recycle-clean-sync-artifacts", pflag.ContinueOnError)
	flags.StringP("config", "c", "", "Config file path")
	flags.BoolP("help", "h", false, "Show help")
	dryRun := flags.Bool("dry-run", false, "Only show what would be deleted")
	limit := flags.Int("limit", 0, "Only process the first N recycle records")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if help, _ := flags.GetBool("help"); help {
		fmt.Println("Usage:")
		fmt.Println("  warehouse recycle clean-sync-artifacts -c config.yaml [--dry-run] [--limit N]")
		return nil
	}

	cfg, db, err := buildRecycleDependencies(flags)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	items, err := loadRecycleBackfillItems(ctx, db.DB, *limit)
	if err != nil {
		return err
	}

	recycleDir := filepath.Join(cfg.WebDAV.Directory, ".recycle")
	legacyFiles, err := loadLegacyRecycleCandidates(recycleDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("scan recycle dir: %w", err)
	}

	matched := 0
	deleted := 0
	fileMissing := 0
	skipped := 0
	releasedBytes := int64(0)

	for _, item := range items {
		if !isRecycleSyncArtifactItem(item) {
			skipped++
			continue
		}
		matched++

		recyclePath, err := resolveRecycleBackfillPath(recycleDir, legacyFiles, item)
		switch {
		case err == nil:
			if !*dryRun {
				if err := os.RemoveAll(recyclePath); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("delete recycle file for %s: %w", item.Hash, err)
				}
			}
		case os.IsNotExist(err):
			fileMissing++
		default:
			return fmt.Errorf("resolve recycle path for %s: %w", item.Hash, err)
		}

		if !*dryRun {
			if err := deleteRecycleSyncArtifactRecord(ctx, db.DB, item); err != nil {
				return err
			}
		}
		deleted++
		releasedBytes += item.Size
	}

	printPrettyJSONFromAny(map[string]any{
		"command":        "recycle clean-sync-artifacts",
		"dry_run":        *dryRun,
		"recycle_dir":    recycleDir,
		"scanned":        len(items),
		"matched":        matched,
		"deleted":        deleted,
		"file_missing":   fileMissing,
		"skipped":        skipped,
		"released_bytes": releasedBytes,
	})
	return nil
}

func buildRecycleDependencies(flags *pflag.FlagSet) (*config.Config, *database.PostgresDB, error) {
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

func loadRecycleBackfillItems(ctx context.Context, db *sql.DB, limit int) ([]recycleBackfillItem, error) {
	query := `
		SELECT id, hash, user_id, username, directory, name, path, is_dir, size, deleted_at
		FROM recycle_items
		ORDER BY deleted_at DESC
	`
	var rows *sql.Rows
	var err error
	if limit > 0 {
		query += ` LIMIT $1`
		rows, err = db.QueryContext(ctx, query, limit)
	} else {
		rows, err = db.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, fmt.Errorf("query recycle items: %w", err)
	}
	defer rows.Close()

	items := make([]recycleBackfillItem, 0)
	for rows.Next() {
		var item recycleBackfillItem
		if err := rows.Scan(
			&item.ID,
			&item.Hash,
			&item.UserID,
			&item.Username,
			&item.Directory,
			&item.Name,
			&item.Path,
			&item.IsDir,
			&item.Size,
			&item.DeletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan recycle item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recycle items: %w", err)
	}
	return items, nil
}

func updateRecycleItemIsDir(ctx context.Context, db *sql.DB, hash string, isDir bool) error {
	if _, err := db.ExecContext(ctx, `UPDATE recycle_items SET is_dir = $2 WHERE hash = $1`, hash, isDir); err != nil {
		return fmt.Errorf("update recycle item %s is_dir: %w", hash, err)
	}
	return nil
}

func deleteRecycleSyncArtifactRecord(ctx context.Context, db *sql.DB, item recycleBackfillItem) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin recycle sync artifact cleanup: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, `DELETE FROM recycle_items WHERE hash = $1`, item.Hash); err != nil {
		return fmt.Errorf("delete recycle item %s: %w", item.Hash, err)
	}
	if item.Size > 0 && strings.TrimSpace(item.UserID) != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE users SET used_space = GREATEST(used_space - $1, 0) WHERE id = $2`, item.Size, item.UserID); err != nil {
			return fmt.Errorf("release recycle quota for %s: %w", item.Hash, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit recycle sync artifact cleanup: %w", err)
	}
	return nil
}

func isRecycleSyncArtifactItem(item recycleBackfillItem) bool {
	for _, candidate := range recycleSyncArtifactCandidatePaths(item) {
		if appservice.IsEphemeralSyncArtifactPath(candidate) {
			return true
		}
	}
	return false
}

func recycleSyncArtifactCandidatePaths(item recycleBackfillItem) []string {
	candidates := make([]string, 0, 2)
	if strings.TrimSpace(item.Path) != "" {
		candidates = append(candidates, item.Path)
	}
	if strings.TrimSpace(item.Directory) != "" && strings.TrimSpace(item.Name) != "" {
		candidates = append(candidates, path.Join("/", filepath.ToSlash(item.Directory), item.Name))
	}
	return candidates
}

func loadLegacyRecycleCandidates(recycleDir string) ([]string, error) {
	candidates := make([]string, 0)
	err := filepath.WalkDir(recycleDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == recycleDir {
			return nil
		}
		candidates = append(candidates, path)
		if d.IsDir() {
			return filepath.SkipDir
		}
		return nil
	})
	return candidates, err
}

func resolveRecycleBackfillPath(recycleDir string, legacyFiles []string, item recycleBackfillItem) (string, error) {
	newPath := filepath.Join(recycleDir, fmt.Sprintf("%s_%s", item.Hash, item.Name))
	if _, err := os.Stat(newPath); err == nil {
		return newPath, nil
	}

	legacyPrefix := filepath.Join(recycleDir, fmt.Sprintf("%s_%s_%s_", item.Username, item.Directory, item.Name))
	matches := make([]string, 0)
	for _, candidate := range legacyFiles {
		if strings.HasPrefix(candidate, legacyPrefix) {
			matches = append(matches, candidate)
		}
	}
	if len(matches) == 0 {
		return "", os.ErrNotExist
	}
	best := matches[0]
	bestDelta := time.Duration(math.MaxInt64)
	for _, candidate := range matches {
		info, err := os.Stat(candidate)
		if err != nil {
			continue
		}
		delta := info.ModTime().Sub(item.DeletedAt)
		if delta < 0 {
			delta = -delta
		}
		if delta < bestDelta {
			bestDelta = delta
			best = candidate
		}
	}
	return best, nil
}
