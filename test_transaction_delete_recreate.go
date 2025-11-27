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

// TestTransactionDeleteRecreate verifies that:
//   - Delete + Set on the same key in one transaction is treated as an atomic update
//   - The key remains visible externally with the old value until commit
//   - The key never disappears (os.ErrNotExist) during the transaction
//   - The new value is not leaked externally until commit
//   - After commit, the new value is globally visible
//   - After rollback, the original state is fully restored
func TestTransactionDeleteRecreate(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestTransactionDeleteRecreate/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "key"
	const initialValue = "initial-value"
	const newValue = "recreated-value"

	// Phase 1: Insert initial value
	txSetup, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Setup transaction: %v", err)
	}
	if err := txSetup.Set(ctx, key, strings.NewReader(initialValue)); err != nil {
		t.Fatalf("Setup Set: %v", err)
	}
	if err := txSetup.Commit(ctx); err != nil {
		t.Fatalf("Setup Commit: %v", err)
	}

	// Phase 2: Start transaction — Delete then immediately recreate
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}

	if err := tx.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Key must be invisible in the same transaction after delete
	if _, err := tx.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Key still visible after Delete in same transaction")
	}

	// Recreate with new value
	if err := tx.Set(ctx, key, strings.NewReader(newValue)); err != nil {
		t.Fatalf("Set after Delete failed: %v", err)
	}

	// Key must now be visible again — with the NEW value
	r, err := tx.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed after recreate: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading recreated value: %v", err)
	}
	if string(data) != newValue {
		t.Errorf("After delete+recreate, got %q; want %q", data, newValue)
	}

	// Critical: External snapshot taken DURING the transaction
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot during transaction: %v", err)
	}
	defer snap.Discard(ctx)

	// External reader MUST see the old value — never missing, never new
	r, err = snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("External snapshot saw key as missing during atomic update: %v — atomicity violation", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != initialValue {
		t.Fatalf("External snapshot saw uncommitted value %q during atomic update — isolation violation (want %q)", data, initialValue)
	}

	// Phase 3: Commit — new value becomes visible
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// After commit: new value is globally visible
	finalSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Final snapshot: %v", err)
	}
	defer finalSnap.Discard(ctx)

	r, err = finalSnap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Key missing after commit: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != newValue {
		t.Errorf("After commit, final value = %q; want %q", data, newValue)
	}

	// Phase 4: Rollback test — recreate key first
	txRecreate, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Recreate for rollback test: %v", err)
	}
	if err := txRecreate.Set(ctx, key, strings.NewReader(initialValue)); err != nil {
		t.Fatalf("Recreate Set: %v", err)
	}
	if err := txRecreate.Commit(ctx); err != nil {
		t.Fatalf("Recreate Commit: %v", err)
	}

	// Now delete + recreate + rollback
	txRollback, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Rollback test transaction: %v", err)
	}
	if err := txRollback.Delete(ctx, key); err != nil {
		t.Fatalf("Delete in rollback test: %v", err)
	}
	if err := txRollback.Set(ctx, key, strings.NewReader("should-not-appear")); err != nil {
		t.Fatalf("Set in rollback test: %v", err)
	}
	if err := txRollback.Rollback(ctx); err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	// After rollback: original value must be restored
	postRollbackSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Post-rollback snapshot: %v", err)
	}
	defer postRollbackSnap.Discard(ctx)

	r, err = postRollbackSnap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Key missing after rollback: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != initialValue {
		t.Errorf("After rollback of delete+recreate, value = %q; want %q", data, initialValue)
	}
}
