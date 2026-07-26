package database

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestVerifyNotificationDedupeConstraintAcceptsPartialUniqueIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT i.indisunique, a.attname, COALESCE(pg_get_expr(i.indpred, i.indrelid), '')")).
		WillReturnRows(sqlmock.NewRows([]string{"indisunique", "attname", "predicate"}).
			AddRow(true, "dedupe_key", "(dedupe_key IS NOT NULL)"))

	if err := verifyNotificationDedupeConstraint(context.Background(), db); err != nil {
		t.Fatalf("verifyNotificationDedupeConstraint() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestVerifyNotificationDedupeConstraintRejectsNonUniqueIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT i.indisunique, a.attname, COALESCE(pg_get_expr(i.indpred, i.indrelid), '')")).
		WillReturnRows(sqlmock.NewRows([]string{"indisunique", "attname", "predicate"}).
			AddRow(false, "dedupe_key", "(dedupe_key IS NOT NULL)"))

	if err := verifyNotificationDedupeConstraint(context.Background(), db); err == nil {
		t.Fatal("verifyNotificationDedupeConstraint() error = nil, want incompatible constraint")
	}
}

func TestEnsureNotificationDedupeConstraintSkipsValidIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT i.indisunique, a.attname, COALESCE(pg_get_expr(i.indpred, i.indrelid), '')")).
		WillReturnRows(sqlmock.NewRows([]string{"indisunique", "attname", "predicate"}).
			AddRow(true, "dedupe_key", "(dedupe_key IS NOT NULL)"))

	if err := ensureNotificationDedupeConstraint(context.Background(), db); err != nil {
		t.Fatalf("ensureNotificationDedupeConstraint() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnsureNotificationDedupeConstraintRepairsMissingIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	indexQuery := regexp.QuoteMeta("SELECT i.indisunique, a.attname, COALESCE(pg_get_expr(i.indpred, i.indrelid), '')")
	mock.ExpectQuery(indexQuery).WillReturnRows(sqlmock.NewRows([]string{"indisunique", "attname", "predicate"}))
	mock.ExpectExec("WITH duplicate_notifications AS").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DROP INDEX IF EXISTS idx_notifications_dedupe_key").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE UNIQUE INDEX idx_notifications_dedupe_key").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(indexQuery).
		WillReturnRows(sqlmock.NewRows([]string{"indisunique", "attname", "predicate"}).
			AddRow(true, "dedupe_key", "(dedupe_key IS NOT NULL)"))

	if err := ensureNotificationDedupeConstraint(context.Background(), db); err != nil {
		t.Fatalf("ensureNotificationDedupeConstraint() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
