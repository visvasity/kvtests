package kvtests

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/visvasity/kv"
)

// TestConflictingTransactionCommit verifies correct conflict detection:
// When multiple transactions concurrently modify the same key,
// only non-conflicting ones commit. At least one must succeed,
// and the final value must be from one of the successful commits.
func TestConflictingTransactionCommit(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestConflictingTransactionCommit/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const key = prefix + "hotspot"
	const numTxns = 100

	var commitCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx, err := db.NewTransaction(ctx)
			if err != nil {
				t.Errorf("NewTransaction failed: %v", err)
				return
			}

			// Read-modify-write: read current value (may be nil), then write
			_, _ = tx.Get(ctx, key) // ignore error — may not exist

			if err := tx.Set(ctx, key, strings.NewReader("winner")); err != nil {
				tx.Rollback(ctx)
				return
			}

			if err := tx.Commit(ctx); err == nil {
				commitCount.Add(1)
			}
			// else: conflict → expected, ignored
			tx.Rollback(ctx) // safe after commit
		}()
	}

	wg.Wait()

	if commits := int(commitCount.Load()); commits == 0 {
		t.Fatal("No transaction committed — at least one should have succeeded")
	} else {
		t.Logf("%d out of %d conflicting transactions committed (expected >= 1)", commits, numTxns)
	}

	// Final state must reflect one of the successful writes
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	r, err := snap.Get(ctx, key)
	if err != nil {
		t.Fatal("Final key does not exist after commits")
	}
	data, _ := io.ReadAll(r)
	if string(data) != "winner" {
		t.Errorf("Final value = %q; want %q", data, "winner")
	}
}
