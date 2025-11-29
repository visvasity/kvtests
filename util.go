package kvtests

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kv/kvutil"
)

// warn logs a non-fatal warning (used only for best-effort cleanup).
func warn(t testing.TB, format string, args ...any) {
	t.Helper()
	t.Logf("WARNING (cleanup): "+format, args...)
}

// cleanupPrefix deletes all keys under the given prefix using the correct prefix range.
// Errors are non-fatal (best-effort) but are logged as warnings.
func cleanupPrefix(ctx context.Context, t testing.TB, db kv.Database, prefix string) {
	t.Helper()

	begin, end := kvutil.PrefixRange(prefix)

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		warn(t, "cleanupPrefix: NewTransaction failed: %v", err)
		return
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, os.ErrClosed) {
				warn(t, "cleanupPrefix: Transaction.Rollback failed: %v", err)
			}
		}
	}()

	var iterErr error
	for key := range tx.Ascend(ctx, begin, end, &iterErr) {
		if err := tx.Delete(ctx, key); err != nil {
			warn(t, "cleanupPrefix: Delete(%q) failed: %v", key, err)
			// keep trying to delete the rest
		}
	}
	if iterErr != nil {
		warn(t, "cleanupPrefix: iteration error: %v", iterErr)
	}

	if err := tx.Commit(ctx); err != nil {
		warn(t, "cleanupPrefix: final Commit failed: %v", err)
	}
}
