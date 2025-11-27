package kvtests

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kv/kvutil"
)

// TestSnapshotIteratorPrefixRange verifies that a snapshot iterator using
// kvutil.PrefixRange correctly returns all keys that have the given prefix
// and excludes any key that does not start with that prefix.
// This is the canonical, safe way to iterate over a logical namespace.
func TestSnapshotIteratorPrefixRange(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestSnapshotIteratorPrefixRange/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Keys that MUST be included — all start with our prefix
	ourKeys := []string{
		prefix + "aaa",
		prefix + "aab",
		prefix + "abc",
		prefix + "xyz",
		prefix + "x\xff",    // contains 0xff byte
		prefix + "x\xffabc", // deep under prefix with \xff
		prefix + "z",        // short key — still under prefix
		prefix + "zzza",     // longer key — still under prefix
		prefix + "zzzzzzzz", // very long — still under prefix
		prefix + "a/b/c",    // hierarchical path — valid
	}

	// Foreign keys — do NOT start with our prefix
	foreignKeys := []string{
		"/OtherPrefix/aaa",
		"/DifferentNamespace/xyz",
		"/TestSnapshotIteratorPrefixRange2/foo",
		"/TestSnapshotIteratorPrefixRang",      // one byte short at end
		"/TestSnapshotIteratorPrefixRange",     // missing trailing slash
		"/testsnapshotiteratorprefixrange/aaa", // wrong case
	}

	// Write both our keys and foreign keys
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	for _, k := range append(ourKeys, foreignKeys...) {
		if err := tx.Set(ctx, k, strings.NewReader("data")); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	// Use the official, safe prefix range from kvutil
	begin, end := kvutil.PrefixRange(prefix)

	var seen []string
	var iterErr error
	for key, val := range snap.Ascend(ctx, begin, end, &iterErr) {
		seen = append(seen, key)
		if val == nil {
			t.Error("Iterator returned nil value reader")
		}
	}
	if iterErr != nil {
		t.Fatalf("Ascend with PrefixRange failed: %v", iterErr)
	}

	// Expected: exactly our keys, in sorted order
	expected := slices.Clone(ourKeys)
	slices.Sort(expected)
	slices.Sort(seen)

	if !slices.Equal(seen, expected) {
		t.Errorf("Prefix range iteration mismatch\n got: %v\nwant: %v", seen, expected)
	}

	// Verify no foreign keys were included
	for _, fk := range foreignKeys {
		if slices.Contains(seen, fk) {
			t.Errorf("Foreign key %q incorrectly included in prefix range", fk)
		}
	}

	// Verify tricky \xff cases were included (they belong to us)
	for _, k := range []string{prefix + "x\xff", prefix + "x\xffabc"} {
		if !slices.Contains(seen, k) {
			t.Errorf("Key with embedded \\xff %q was NOT included — prefix range broken", k)
		}
	}
}
