package kvtests

import (
	"context"
	"strings"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kv/kvutil"
)

// TestPrefixCleanupTrailingFF verifies that cleanupPrefix correctly deletes
// ALL keys that have the given prefix — including keys with embedded or trailing
// 0xFF bytes — and leaves keys that do *not* start with the prefix untouched.
func TestPrefixCleanupTrailingFF(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestPrefixCleanupTrailingFF/"
	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Keys that MUST be deleted — all start with our exact prefix
	keysToDelete := []string{
		prefix + "normal",
		prefix + "a\xff",
		prefix + "b\xffend",
		prefix + "c\xff\xff\xff",
		prefix + "deep\xffsub\xffkey",
		prefix + "x\xffabc\xffdef",
		prefix + "z", // perfectly valid under prefix
		prefix + "zz",
		prefix + "zzzzzzzz",
		prefix + "subdir/key",
	}

	// Foreign keys — do NOT start with our exact prefix
	foreignKeys := []string{
		"/OtherPrefix/normal",
		"/TestPrefixCleanupTrailingF/foo",   // missing last 'F'
		prefix[:len(prefix)-1] + "no-slash", // missing trailing '/'
		"/TestPrefixCleanupTrailingFF2/",    // different suffix
		"/testprefixcleanuptrailingff/",     // wrong case
	}

	// Insert all keys (both ours and foreign)
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	for _, k := range append(keysToDelete, foreignKeys...) {
		if err := tx.Set(ctx, k, strings.NewReader("data")); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Perform the cleanup using the standard helper
	cleanupPrefix(ctx, t, db, prefix)

	// Verify results with a fresh snapshot
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	// 1. No key under our prefix must remain
	begin, end := kvutil.PrefixRange(prefix)
	var remaining []string
	var iterErr error
	for key := range snap.Ascend(ctx, begin, end, &iterErr) {
		remaining = append(remaining, key)
	}
	if iterErr != nil {
		t.Fatalf("Verification iteration failed: %v", iterErr)
	}
	if len(remaining) > 0 {
		t.Errorf("cleanupPrefix failed — %d keys still exist under prefix:\n%v", len(remaining), remaining)
	}

	// 2. Explicitly check every key we expected to delete is gone
	for _, k := range keysToDelete {
		if _, err := snap.Get(ctx, k); err == nil {
			t.Errorf("Key %q survived cleanup — prefix deletion broken", k)
		}
	}

	// 3. Foreign keys must still exist
	for _, fk := range foreignKeys {
		if _, err := snap.Get(ctx, fk); err != nil {
			t.Errorf("Foreign key %q was incorrectly deleted: %v", fk, err)
		}
	}

	// 4. Cleanup must be idempotent — second call does nothing and doesn't error
	cleanupPrefix(ctx, t, db, prefix)

	remainingAfterSecond := 0
	for range snap.Ascend(ctx, begin, end, nil) {
		remainingAfterSecond++
	}
	if remainingAfterSecond != 0 {
		t.Errorf("Second cleanup left %d keys behind", remainingAfterSecond)
	}

	t.Log("cleanupPrefix correctly handles keys with embedded/trailing 0xFF and is fully prefix-safe")
}
