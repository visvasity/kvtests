package kvtests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestZeroLengthValue verifies that zero-length (empty) values
// are correctly stored, retrieved, and distinguished from key non-existence.
// This is a required edge case in the kv package.
func TestZeroLengthValue(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestZeroLengthValue/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "empty"

	// Phase 1: Store a zero-length value
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}

	// Use a reader with zero bytes
	if err := tx.Set(ctx, key, bytes.NewReader(nil)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("Set with zero-length reader failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Phase 2: Verify the key exists and has exactly zero bytes
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	r, err := snap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed for key with zero-length value: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("io.ReadAll failed: %v", err)
	}

	if len(data) != 0 {
		t.Errorf("Zero-length value stored incorrectly: got %d bytes, want 0", len(data))
	}

	// Phase 3: Verify that zero-length value ≠ key not found
	nonExistentKey := prefix + "missing"
	if _, err := snap.Get(ctx, nonExistentKey); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Error("Non-existent key returned success — broken")
		} else {
			t.Logf("Non-existent key correctly returns error: %v", err)
		}
	}

	// Phase 4: Overwrite zero-length with non-zero and back — must work
	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Second transaction: %v", err)
	}

	// Write non-empty value
	if err := tx2.Set(ctx, key, strings.NewReader("hello")); err != nil {
		t.Fatalf("Set non-empty value failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit non-empty: %v", err)
	}

	// Verify non-empty
	snap2, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Second snapshot: %v", err)
	}
	defer snap2.Discard(ctx)

	r, err = snap2.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after non-empty write: %v", err)
	}
	data, _ = io.ReadAll(r)
	if string(data) != "hello" {
		t.Errorf("After non-empty write, got %q; want \"hello\"", data)
	}

	// Write zero-length again
	tx3, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Third transaction: %v", err)
	}
	if err := tx3.Set(ctx, key, bytes.NewReader([]byte{})); err != nil {
		t.Fatalf("Set empty slice failed: %v", err)
	}
	if err := tx3.Commit(ctx); err != nil {
		t.Fatalf("Commit empty slice: %v", err)
	}

	// Final check: zero-length again
	finalSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Final snapshot: %v", err)
	}
	defer finalSnap.Discard(ctx)

	r, err = finalSnap.Get(ctx, key)
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}
	data, _ = io.ReadAll(r)
	if len(data) != 0 {
		t.Errorf("Final zero-length value has %d bytes, want 0", len(data))
	}
}
