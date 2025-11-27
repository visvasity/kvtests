package kvtests

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/visvasity/kv"
)

// TestNilValueInvalid verifies that Set with a nil value reader returns os.ErrInvalid.
func TestNilValueInvalid(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestNilValueInvalid/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "key"

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Attempt to set with nil reader
	if err := tx.Set(ctx, key, nil); !errors.Is(err, os.ErrInvalid) {
		t.Errorf("Set(ctx, key, nil) = %v; want os.ErrInvalid", err)
	}

	// Ensure the key was not created (sanity check)
	if _, err := tx.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("After failed Set(nil), Get returned %v; want os.ErrNotExist", err)
	}

	// Commit should succeed (no changes were made)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit after failed Set(nil) returned error: %v", err)
	}

	// Verify via snapshot that nothing was persisted
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	if _, err := snap.Get(ctx, key); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Snapshot.Get after failed Set(nil) returned %v; want os.ErrNotExist", err)
	}
}
