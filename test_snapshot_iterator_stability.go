package kvtests

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/visvasity/kv"
)

// TestSnapshotIteratorStability verifies that a snapshot's iterator
// remains completely stable and unaffected by concurrent writes.
// Multiple iterations must see exactly the same keys and values.
func TestSnapshotIteratorStability(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestSnapshotIteratorStability/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Phase 1: Write initial known state
	initialKeys := []string{
		prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "e",
	}
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction (initial): %v", err)
	}
	for _, k := range initialKeys {
		if err := tx.Set(ctx, k, strings.NewReader("v1")); err != nil {
			t.Fatalf("Set initial %q: %v", k, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit initial: %v", err)
	}

	// Create snapshot — this must freeze the view
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	// Force a snapshot for database backends that pick any serializable snapshot
	// dynamically on first read (eg: Postgres).
	if _, err := snap.Get(ctx, initialKeys[0]); err != nil {
		t.Fatal(err)
	}

	// Phase 2: Concurrent writer goroutine to update the database
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			tx, err := db.NewTransaction(ctx)
			if err != nil {
				done <- err
				return
			}

			// Overwrite existing keys
			for _, k := range initialKeys {
				if err := tx.Set(ctx, k, strings.NewReader("v2-concurrent")); err != nil {
					tx.Rollback(ctx)
					done <- err
					return
				}
			}

			// Add new keys
			newKey := prefix + "new-" + string(rune('0'+i%10))
			if err := tx.Set(ctx, newKey, strings.NewReader("new")); err != nil {
				tx.Rollback(ctx)
				done <- err
				return
			}

			if err := tx.Commit(ctx); err != nil {
				done <- err
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Give writer time to start
	time.Sleep(50 * time.Millisecond)

	// Phase 3: Multiple full scans of the snapshot — must be 100% stable
	for round := 1; round <= 3; round++ {
		t.Run("iteration-"+string(rune('0'+round)), func(t *testing.T) {
			var seen []string
			var iterErr error

			for key, val := range snap.Ascend(ctx, "", "", &iterErr) {
				if !strings.HasPrefix(key, prefix) {
					continue
				}
				seen = append(seen, key)

				data, err := io.ReadAll(val)
				if err != nil {
					t.Fatalf("Round %d: io.ReadAll(%q) failed: %v", round, key, err)
				}
				if string(data) != "v1" {
					t.Errorf("Round %d: Key %q has value %q; want \"v1\" (snapshot corrupted)", round, key, data)
				}
			}
			if iterErr != nil {
				t.Fatalf("Round %d: iteration error: %v", round, iterErr)
			}

			expected := slices.Clone(initialKeys)
			slices.Sort(expected)
			slices.Sort(seen)

			if !slices.Equal(seen, expected) {
				t.Errorf("Round %d: key set mismatch\n got: %v\nwant: %v", round, seen, expected)
			}
		})

		time.Sleep(20 * time.Millisecond)
	}

	// Wait for background writer to finish and report any error
	if err := <-done; err != nil {
		t.Errorf("Background writer failed: %v", err)
	}

	// Final sanity: fresh snapshot sees new keys
	freshSnap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("Final NewSnapshot: %v", err)
	}
	defer freshSnap.Discard(ctx)

	var hasNew bool
	var scanErr error
	for range freshSnap.Ascend(ctx, prefix+"new-", prefix+"new0", &scanErr) {
		hasNew = true
		break
	}
	if scanErr != nil {
		t.Errorf("Final scan for new keys failed: %v", scanErr)
	}
	if !hasNew {
		t.Error("Fresh snapshot did not see any new keys written during test")
	}
}
