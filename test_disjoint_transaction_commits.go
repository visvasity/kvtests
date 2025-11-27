package kvtests

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/visvasity/kv"
)

// TestDisjointTransactionCommit verifies that concurrent transactions
// modifying completely disjoint keys all commit successfully.
// There must be no spurious conflicts when keys do not overlap.
func TestDisjointTransactionCommit(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestDisjointTransactionCommit/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	const numTxns = 100
	const value = "data"

	// Each transaction gets its own unique key → truly disjoint
	var wg sync.WaitGroup
	errs := make(chan error, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			key := prefix + string(rune('0'+id)) // Unique keys: /prefix/0, /prefix/1, ..., /prefix/99

			tx, err := db.NewTransaction(ctx)
			if err != nil {
				errs <- err
				return
			}

			if err := tx.Set(ctx, key, strings.NewReader(value)); err != nil {
				tx.Rollback(ctx)
				errs <- err
				return
			}

			if err := tx.Commit(ctx); err != nil {
				errs <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Must have zero errors — all disjoint txes must succeed
	for err := range errs {
		t.Errorf("Disjoint transaction failed to commit: %v", err)
	}

	// Verify all keys were written
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot for verification: %v", err)
	}
	defer snap.Discard(ctx)

	for i := 0; i < numTxns; i++ {
		key := prefix + string(rune('0'+i))
		r, err := snap.Get(ctx, key)
		if err != nil {
			t.Errorf("Key %s not found after successful commit: %v", key, err)
			continue
		}
		data, _ := io.ReadAll(r)
		if string(data) != value {
			t.Errorf("Key %s has wrong value: got %q, want %q", key, data, value)
		}
	}
}
