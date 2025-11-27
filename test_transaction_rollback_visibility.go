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

// TestTransactionRollbackVisibility verifies that:
//   - All writes (Set) and deletes performed in a transaction
//   - are completely undone when Rollback() is called
//   - The database state after rollback is exactly the same as before the transaction
func TestTransactionRollbackVisibility(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestTransactionRollbackVisibility/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key1 = prefix + "key1"
	const key2 = prefix + "key2"
	const key3 = prefix + "key3"
	const value = "should-disappear"

	// Phase 1: Write two keys that will survive (committed)
	txSetup, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Setup transaction: %v", err)
	}
	if err := txSetup.Set(ctx, key1, strings.NewReader("committed-value")); err != nil {
		t.Fatalf("Setup Set key1: %v", err)
	}
	if err := txSetup.Set(ctx, key2, strings.NewReader("also-committed")); err != nil {
		t.Fatalf("Setup Set key2: %v", err)
	}
	if err := txSetup.Commit(ctx); err != nil {
		t.Fatalf("Setup Commit: %v", err)
	}

	// Phase 2: Start a transaction that does multiple writes and a delete
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}

	// 1. Create a new key
	if err := tx.Set(ctx, key3, strings.NewReader(value)); err != nil {
		t.Fatalf("Set key3: %v", err)
	}

	// 2. Modify an existing key
	if err := tx.Set(ctx, key1, strings.NewReader("modified-but-rolled-back")); err != nil {
		t.Fatalf("Modify key1: %v", err)
	}

	// 3. Delete an existing key
	if err := tx.Delete(ctx, key2); err != nil {
		t.Fatalf("Delete key2: %v", err)
	}

	// Verify changes are visible inside the transaction (read-your-writes)
	r, err := tx.Get(ctx, key3)
	if err != nil {
		t.Fatalf("Cannot read own write (key3): %v", err)
	}
	data, _ := io.ReadAll(r)
	if string(data) != value {
		t.Errorf("Own write key3 has wrong value: got %q", data)
	}

	if _, err := tx.Get(ctx, key2); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Deleted key2 still visible in transaction")
	}

	// Phase 3: Rollback the transaction
	if err := tx.Rollback(ctx); err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	// Phase 4: After rollback — all changes must be gone
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot after rollback: %v", err)
	}
	defer snap.Discard(ctx)

	// 1. New key must not exist
	if _, err := snap.Get(ctx, key3); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Fatal("New key key3 still exists after rollback")
		} else {
			t.Errorf("Get key3 after rollback returned unexpected error: %v", err)
		}
	}

	// 2. Modified key must have original committed value
	r, err = snap.Get(ctx, key1)
	if err != nil {
		t.Fatalf("Original key1 missing after rollback: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != "committed-value" {
		t.Errorf("After rollback, key1 = %q; want %q", data, "committed-value")
	}

	// 3. Deleted key must still exist with its original value
	r, err = snap.Get(ctx, key2)
	if err != nil {
		t.Fatalf("Deleted key2 missing after rollback: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != "also-committed" {
		t.Errorf("After rollback, key2 = %q; want %q", data, "also-committed")
	}

	// Bonus: Rollback is idempotent — second call must not fail
	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Second transaction: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Errorf("First Rollback on fresh tx failed: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Errorf("Second Rollback returned unexpected error: %v", err)
	}
}
