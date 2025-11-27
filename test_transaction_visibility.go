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

// TestTransactionVisibility verifies that writes performed inside a transaction
// are not visible to other readers (including snapshots and other transactions)
// until the transaction is successfully committed.
func TestTransactionVisibility(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestTransactionVisibility/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "secret"

	// === Phase 1: Start a transaction and write data — but do NOT commit yet ===
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	if err := tx.Set(ctx, key, strings.NewReader("hidden-value")); err != nil {
		t.Fatalf("Set in transaction: %v", err)
	}
	// Do NOT commit yet — data must remain invisible

	// === Phase 2: Verify the write is NOT visible externally ===

	// 2a: Other transaction must not see it
	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Second transaction: %v", err)
	}
	defer tx2.Rollback(ctx)

	_, err = tx2.Get(ctx, key)
	if err == nil {
		t.Fatal("Uncommitted write visible to another transaction — isolation broken")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Logf("Second transaction Get returned: %v (expected not found)", err)
	}

	// 2b: Snapshot taken now must not see it
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	_, err = snap.Get(ctx, key)
	if err == nil {
		t.Fatal("Uncommitted write visible in snapshot — isolation broken")
	}

	// 2c: But the same transaction can see its own write (read-your-writes)
	r, err := tx.Get(ctx, key)
	if err != nil {
		t.Fatalf("Transaction cannot read its own write: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading own write: %v", err)
	}
	if string(data) != "hidden-value" {
		t.Errorf("Transaction read wrong value: got %q, want %q", data, "hidden-value")
	}

	// === Phase 3: Commit the transaction ===
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// === Phase 4: Now the write MUST be visible everywhere ===

	// 4a: New transaction sees it
	tx3, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("New transaction after commit: %v", err)
	}
	defer tx3.Rollback(ctx)

	r, err = tx3.Get(ctx, key)
	if err != nil {
		t.Fatalf("Committed write not visible to new transaction: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != "hidden-value" {
		t.Errorf("New transaction saw wrong value: %q", data)
	}

	// 4b: New snapshot sees it
	snap2, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Second snapshot: %v", err)
	}
	defer snap2.Discard(ctx)

	r, err = snap2.Get(ctx, key)
	if err != nil {
		t.Fatalf("Committed write not visible in new snapshot: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != "hidden-value" {
		t.Errorf("New snapshot saw wrong value: %q", data)
	}
}
