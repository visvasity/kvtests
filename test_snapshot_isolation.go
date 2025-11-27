package kvtests

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestSnapshotIsolation verifies that a snapshot sees exactly the state at creation time,
// even under concurrent/modifying writes afterward.
func TestSnapshotIsolation(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestSnapshotIsolation/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "counter"

	// 1. Write initial value: "initial"
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

	// 2. Create snapshot — this must freeze the view at "initial"
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	// 3. Now perform several new committed writes
	for i := 1; i <= 3; i++ {
		tx, err := db.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction (write %d): %v", i, err)
		}
		value := fmt.Sprintf("version-%d", i)
		if err := tx.Set(ctx, key, strings.NewReader(value)); err != nil {
			t.Fatalf("Set version-%d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit version-%d: %v", i, err)
		}
	}

	// 4. The original snapshot must STILL see "initial"
	r, err := snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Snapshot.Get failed: %v", err)
	}
	data, _ := io.ReadAll(r)
	if string(data) != "initial" {
		t.Errorf("Snapshot saw %q after modifications; want %q", data, "initial")
	}

	// 5. A new snapshot must see the latest version: "version-3"
	newSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot (latest): %v", err)
	}
	defer newSnap.Discard(ctx)

	r, err = newSnap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Latest snapshot Get failed: %v", err)
	}
	latest, _ := io.ReadAll(r)
	if string(latest) != "version-3" {
		t.Errorf("Latest snapshot saw %q; want %q", latest, "version-3")
	}
}
