package kvtests

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/visvasity/kv"
)

// TestLargeValueRoundtrip verifies that values significantly larger than 4KB
// (common page size) are stored and retrieved correctly with no corruption,
// truncation, or memory issues. Uses 64KB, 1MB, and 10MB values.
func TestLargeValueRoundtrip(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestLargeValueRoundtrip/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "large"

	tests := []struct {
		name string
		size int64 // in bytes
	}{
		{"64KB", 64 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Generate random data of exact size
			original := make([]byte, tc.size)
			if _, err := rand.Read(original); err != nil {
				t.Fatalf("Failed to generate random data: %v", err)
			}

			// Write via transaction
			tx, err := db.NewTransaction(ctx)
			if err != nil {
				t.Fatalf("NewTransaction: %v", err)
			}

			if err := tx.Set(ctx, key, bytes.NewReader(original)); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Set failed for %s: %v", tc.name, err)
			}

			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Read back via snapshot
			snap, err := db.NewSnapshot(ctx)
			if err != nil {
				t.Fatalf("NewSnapshot: %v", err)
			}
			defer snap.Discard(ctx)

			r, err := snap.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get failed after commit: %v", err)
			}

			retrieved, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("io.ReadAll failed: %v", err)
			}

			// Compare byte-for-byte
			if len(retrieved) != len(original) {
				t.Errorf("Size mismatch: wrote %d bytes, read %d bytes", len(original), len(retrieved))
			}

			if !bytes.Equal(original, retrieved) {
				// Find first mismatch
				for i := 0; i < len(original) && i < len(retrieved); i++ {
					if original[i] != retrieved[i] {
						t.Errorf("Data corruption at byte %d: wrote 0x%x, read 0x%x", i, original[i], retrieved[i])
						break
					}
				}
				t.Errorf("Large value corrupted during storage/retrieval (%s)", tc.name)
			}
		})
	}
}
