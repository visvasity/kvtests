package kvtests

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestCommitAfterRollbackIgnored verifies that calling Commit after Rollback is ignored (no error).
func TestCommitAfterRollbackIgnored(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestCommitAfterRollbackIgnored/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "key"

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}

	// Perform a write that would normally be visible after commit
	if err := tx.Set(ctx, key, strings.NewReader("value")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Explicitly rollback first
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Now attempt to commit — must be ignored (no error)
	if err := tx.Commit(ctx); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Errorf("Commit after Rollback returned unexpected error: %v", err)
	}

	// Data must NOT be visible in a fresh snapshot
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	if _, err := snap.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Key is visible after rollback-then-commit; got %v, want os.ErrNotExist", err)
	}
}
