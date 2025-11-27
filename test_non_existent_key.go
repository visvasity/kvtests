package kvtests

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestNonExistentKey verifies that Get on a non-existent key returns os.ErrNotExist
// for both transaction and snapshot reads.
func TestNonExistentKey(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestNonExistentKey/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	nonExistentKey := prefix + "missing"
	existingKey := prefix + "present"

	// 1. Transaction read of non-existent key
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Get(ctx, nonExistentKey); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Transaction.Get(non-existent key) = %v; want os.ErrNotExist", err)
	}

	// 2. Snapshot read of non-existent key
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	if _, err := snap.Get(ctx, nonExistentKey); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Snapshot.Get(non-existent key) = %v; want os.ErrNotExist", err)
	}

	// 3. Sanity check: write a real key and confirm it becomes visible
	if err := tx.Set(ctx, existingKey, strings.NewReader("data")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Re-open transaction to see committed data
	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction (post-commit): %v", err)
	}
	defer tx2.Rollback(ctx)

	if _, err := tx2.Get(ctx, existingKey); err != nil {
		t.Errorf("Get(existing key) after commit failed: %v", err)
	}
}
