package kvtests

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestRollbackAfterCommitIgnored verifies that calling Rollback after a successful Commit
// is ignored and does not affect already-committed data.
func TestRollbackAfterCommitIgnored(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestRollbackAfterCommitIgnored/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "key"
	const value = "committed-value"

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}

	// Write data and commit it
	if err := tx.Set(ctx, key, strings.NewReader(value)); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now call Rollback on the already-committed transaction — must be ignored
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Errorf("Rollback after successful Commit returned unexpected error: %v", err)
	}

	// Verify the data is still visible and correct
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	r, err := snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}
	data, _ := io.ReadAll(r)
	if string(data) != value {
		t.Errorf("Got value %q after commit; want %q", data, value)
	}
}
