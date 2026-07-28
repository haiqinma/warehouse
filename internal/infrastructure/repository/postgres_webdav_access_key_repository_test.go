package repository

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/yeying-community/warehouse/internal/domain/accesskey"
)

func newWebDAVAccessKeyForRepositoryTest(t *testing.T) *accesskey.WebDAVAccessKey {
	t.Helper()
	item, err := accesskey.New("owner-1", "backup", "ak_test", "hashed-secret", "/personal/backups", "R", nil)
	if err != nil {
		t.Fatalf("create access key fixture: %v", err)
	}
	return item
}

func TestCreateWebDAVAccessKeyWithBindingCommitsAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	item := newWebDAVAccessKeyForRepositoryTest(t)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO webdav_access_keys")).
		WithArgs(item.ID, item.OwnerUserID, item.Name, item.KeyID, item.SecretHash, item.RootPath,
			item.Permissions, item.Status, item.ExpiresAt, item.LastUsedAt, item.CreatedAt, item.UpdatedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO webdav_access_key_bindings")).
		WithArgs(item.ID, item.OwnerUserID, item.RootPath).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	repo := NewPostgresWebDAVAccessKeyRepository(db)
	if err := repo.CreateWithBinding(context.Background(), item, item.RootPath); err != nil {
		t.Fatalf("CreateWithBinding: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCreateWebDAVAccessKeyWithBindingRollsBackOnBindingFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	item := newWebDAVAccessKeyForRepositoryTest(t)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO webdav_access_keys")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO webdav_access_key_bindings")).
		WillReturnError(errors.New("binding insert failed"))
	mock.ExpectRollback()

	repo := NewPostgresWebDAVAccessKeyRepository(db)
	if err := repo.CreateWithBinding(context.Background(), item, item.RootPath); err == nil {
		t.Fatal("expected binding failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
