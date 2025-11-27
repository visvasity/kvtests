// Package kvtests contains black-box correctness tests for any kv.Database implementation.
package kvtests

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestEmptyKeyInvalid verifies that empty keys are rejected with os.ErrInvalid.
func TestEmptyKeyInvalid(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestEmptyKeyInvalid/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Set with empty key
	if err := tx.Set(ctx, "", strings.NewReader("value")); !errors.Is(err, os.ErrInvalid) {
		t.Errorf("Set(empty key) = %v; want os.ErrInvalid", err)
	}

	// Get with empty key
	if _, err := tx.Get(ctx, ""); !errors.Is(err, os.ErrInvalid) {
		t.Errorf("Get(empty key) = %v; want os.ErrInvalid", err)
	}

	// Delete with empty key
	if err := tx.Delete(ctx, ""); !errors.Is(err, os.ErrInvalid) {
		t.Errorf("Delete(empty key) = %v; want os.ErrInvalid", err)
	}

	// Snapshot path
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	if _, err := snap.Get(ctx, ""); !errors.Is(err, os.ErrInvalid) {
		t.Errorf("Snapshot.Get(empty key) = %v; want os.ErrInvalid", err)
	}
}
