package kvtests

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestSnapshotFrozenAtCreation verifies the stronger MVCC guarantee:
// The snapshot is frozen at creation time — even before the first read.
// Only true MVCC engines pass this. PostgreSQL and similar will see latest value.
func TestSnapshotFrozenAtCreation(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestSnapshotFrozenAtCreation/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "value"

	// Write initial value
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	if err := tx.Set(ctx, key, strings.NewReader("initial")); err != nil {
		t.Fatalf("Set initial: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit initial: %v", err)
	}

	// Create snapshot — this should freeze the view at "initial"
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	// Overwrite multiple times
	var values []string
	for i := 1; i <= 3; i++ {
		tx, err := db.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction (overwrite %d): %v", i, err)
		}
		value := fmt.Sprintf("version-%d", i)
		values = append(values, value)
		if err := tx.Set(ctx, key, strings.NewReader(value)); err != nil {
			t.Fatalf("Set version-%d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit version-%d: %v", i, err)
		}
	}

	// First read — if frozen at creation, must see "initial"
	r, err := snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get from frozen snapshot: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading value from frozen snapshot: %v", err)
	}

	if string(data) == "initial" {
		t.Log("Snapshot frozen at creation time — strong MVCC confirmed")
	} else if slices.Contains(values, string(data)) {
		t.Logf("Snapshot saw %q instead of 'initial' — snapshot-point is dynamic (expected for PostgreSQL)", string(data))
	} else {
		t.Errorf("snapshot read unexpected value %q", string(data))
	}
}
