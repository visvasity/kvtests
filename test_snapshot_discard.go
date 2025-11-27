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

// TestDiscardedSnapshotBehavior verifies that after Discard() is called:
//   - All subsequent operations on the snapshot return an error or yield no data
//   - The implementation does not panic or corrupt internal state
//   - Calling Discard() multiple times is safe (idempotent)
func TestDiscardedSnapshotBehavior(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestDiscardedSnapshotBehavior/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Write some data
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	if err := tx.Set(ctx, prefix+"key1", strings.NewReader("value1")); err != nil {
		t.Fatalf("Set key1: %v", err)
	}
	if err := tx.Set(ctx, prefix+"key2", strings.NewReader("value2")); err != nil {
		t.Fatalf("Set key2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Create a snapshot
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	// Use it once — must work
	r, err := snap.Get(ctx, prefix+"key1")
	if err != nil {
		t.Fatalf("First Get failed: %v", err)
	}
	data, _ := io.ReadAll(r)
	if string(data) != "value1" {
		t.Errorf("First Get returned wrong value: %q", data)
	}

	// === Discard the snapshot ===
	if err := snap.Discard(ctx); err != nil {
		t.Fatalf("First Discard failed: %v", err)
	}

	// Discard again — must be idempotent
	if err := snap.Discard(ctx); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Errorf("Second Discard returned unexpected error: %v", err)
	}

	// All operations after Discard must fail or return nothing

	// Get must fail
	if _, err := snap.Get(ctx, prefix+"key1"); err == nil {
		t.Error("Get on discarded snapshot succeeded — should have failed")
	} else if !errors.Is(err, os.ErrClosed) {
		t.Logf("Get on discarded snapshot returned: %v (acceptable)", err)
	}

	// Range iteration must be empty or error
	var count int
	var iterErr error
	for range snap.Ascend(ctx, "", "", &iterErr) {
		count++
	}
	if count > 0 {
		t.Errorf("Ascend on discarded snapshot yielded %d items — should be empty", count)
	}
	if iterErr != nil && !errors.Is(iterErr, os.ErrClosed) {
		t.Logf("Ascend on discarded snapshot returned error: %v (acceptable)", iterErr)
	}

	// Descend must also be empty or error
	count = 0
	iterErr = nil
	for range snap.Descend(ctx, "", "", &iterErr) {
		count++
	}
	if count > 0 {
		t.Errorf("Descend on discarded snapshot yielded %d items — should be empty", count)
	}

	// Final sanity: a fresh snapshot still works
	freshSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Fresh NewSnapshot failed: %v", err)
	}
	defer freshSnap.Discard(ctx)

	r, err = freshSnap.Get(ctx, prefix+"key2")
	if err != nil {
		t.Fatalf("Fresh snapshot Get failed: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != "value2" {
		t.Errorf("Fresh snapshot returned wrong value: %q", data)
	}
}
