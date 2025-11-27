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

// TestTransactionDeleteVisibility verifies that:
//   - A key deleted in a transaction is not visible via Get in the same transaction
//   - The delete is not visible externally until commit
//   - After rollback, the key remains
func TestTransactionDeleteVisibility(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestTransactionDeleteVisibility/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "key"
	const value = "original-value"

	// Phase 1: Insert the key
	txSetup, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Setup transaction: %v", err)
	}
	if err := txSetup.Set(ctx, key, strings.NewReader(value)); err != nil {
		t.Fatalf("Setup Set: %v", err)
	}
	if err := txSetup.Commit(ctx); err != nil {
		t.Fatalf("Setup Commit: %v", err)
	}

	// === Test 1: Delete + Commit ===
	tx1, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Test1 transaction: %v", err)
	}

	if err := tx1.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Key must be invisible in same transaction
	if _, err := tx1.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Deleted key still visible in same transaction")
	}

	// External readers must still see it (uncommitted)
	snap1, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot during uncommitted delete: %v", err)
	}
	r, err := snap1.Get(ctx, key)
	if err != nil {
		t.Fatalf("Uncommitted delete visible externally: %v", err)
	}
	data, _ := io.ReadAll(r)
	if string(data) != value {
		t.Errorf("External snapshot saw wrong value: %q", data)
	}
	snap1.Discard(ctx)

	// Commit the delete
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	// After commit: key must be gone
	snapAfterCommit, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot after commit: %v", err)
	}
	if _, err := snapAfterCommit.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Key still exists after committed delete")
	}
	snapAfterCommit.Discard(ctx)

	// === Test 2: Recreate key for rollback test ===
	txRecreate, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Recreate transaction: %v", err)
	}
	if err := txRecreate.Set(ctx, key, strings.NewReader(value)); err != nil {
		t.Fatalf("Recreate Set: %v", err)
	}
	if err := txRecreate.Commit(ctx); err != nil {
		t.Fatalf("Recreate Commit: %v", err)
	}

	// === Test 3: Delete + Rollback ===
	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Rollback test transaction: %v", err)
	}

	if err := tx2.Delete(ctx, key); err != nil {
		t.Fatalf("Delete for rollback test: %v", err)
	}

	// Key invisible in transaction
	if _, err := tx2.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Deleted key visible in transaction before rollback")
	}

	// Rollback — delete must be discarded
	if err := tx2.Rollback(ctx); err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	// Key must STILL exist after rollback
	finalSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Final snapshot: %v", err)
	}
	defer finalSnap.Discard(ctx)

	r, err = finalSnap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Key disappeared after rollback: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != value {
		t.Errorf("Key value wrong after rollback: got %q", data)
	}
}
