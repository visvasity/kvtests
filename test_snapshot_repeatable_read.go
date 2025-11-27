package kvtests

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestSnapshotRepeatableRead verifies the mandatory guarantee:
// Once a snapshot has performed a read, all future reads in the same snapshot
// must return the same value — even if other transactions commit new versions.
func TestSnapshotRepeatableRead(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestSnapshotRepeatableRead/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "value"

	// Write initial value
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	if err := tx.Set(ctx, key, strings.NewReader("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit v1: %v", err)
	}

	// Create snapshot and perform first read — this establishes the view
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	r, err := snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("First Get from snapshot: %v", err)
	}
	firstValue, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading first value: %v", err)
	}

	// Perform several committed overwrites
	for i := 2; i <= 5; i++ {
		tx, err := db.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction (overwrite %d): %v", i, err)
		}
		if err := tx.Set(ctx, key, strings.NewReader(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Set v%d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit v%d: %v", i, err)
		}
	}

	// Second read in same snapshot MUST return the same value
	r, err = snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Second Get from snapshot: %v", err)
	}
	secondValue, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading second value: %v", err)
	}

	if string(secondValue) != string(firstValue) {
		t.Errorf("Snapshot not repeatable: first read %q, second read %q", firstValue, secondValue)
	}
}
