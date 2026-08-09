package main

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNormalizeSharedResourcePath(t *testing.T) {
	tests := map[string]string{
		"":                  "/",
		"  reports//2026/ ": "/reports/2026",
		"/reports/../docs":  "/docs",
		"///":               "/",
	}
	for input, want := range tests {
		if got := normalizeSharedResourcePath(input); got != want {
			t.Errorf("normalizeSharedResourcePath(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestSharedResourceIDIsStableAndSeparatesOwnersAndTypes(t *testing.T) {
	first := sharedResourceID("owner-a", "/folder", true)
	if first != sharedResourceID("owner-a", "/folder", true) {
		t.Fatal("resource ID is not stable")
	}
	if first == sharedResourceID("owner-b", "/folder", true) {
		t.Fatal("resource ID must include owner")
	}
	if first == sharedResourceID("owner-a", "/folder", false) {
		t.Fatal("resource ID must include resource type")
	}
}

func TestBackfillSharedResourcesIsIdempotentAtDatabaseBoundary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	now := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	item := legacyShareGrant{ID: "share-1", OwnerUserID: "owner-1", Path: "//team/docs/", IsDir: true, Permissions: "read", Status: "active", CreatedAt: now, UpdatedAt: now}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO internal_shared_resources").WithArgs(sharedResourceID("owner-1", "/team/docs", true), "owner-1", "/team/docs", true, now, now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO internal_share_grants").WithArgs("share-1", sharedResourceID("owner-1", "/team/docs", true), "read", nil, "active", now, now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := backfillSharedResources(context.Background(), db, []legacyShareGrant{item}); err != nil {
		t.Fatalf("backfillSharedResources: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackfillShareAudienceGrantsUpdatesOnlyOutdatedLinks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec("UPDATE internal_share_audiences").WillReturnResult(sqlmock.NewResult(0, 2))
	if err := backfillShareAudienceGrants(context.Background(), db); err != nil {
		t.Fatalf("backfillShareAudienceGrants: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
