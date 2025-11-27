package kvtests

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestRangeFullDatabaseScan verifies that Ascend/Descend with both begin and end empty
// correctly iterate over the entire database in ascending and descending order.
// This is a required special case in the Ranger contract.
func TestRangeFullDatabaseScan(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestRangeFullDatabaseScan/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Prepare a known set of keys with predictable byte order
	keys := []string{
		prefix + "a",
		prefix + "aaa",
		prefix + "aab",
		prefix + "b",
		prefix + "ba",
		prefix + "c",
		prefix + "ca",
		prefix + "cb",
		prefix + "x",
		prefix + "z",
		prefix + "za",
		prefix + "zb",
		prefix + "zc",
	}

	// Write all keys
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	for _, k := range keys {
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

	// === Ascend: "", "" => full database in ascending order
	var ascendOrder []string
	var ascendErr error
	for key, val := range snap.Ascend(ctx, "", "", &ascendErr) {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		ascendOrder = append(ascendOrder, key)
		if val == nil {
			t.Error("Ascend returned nil value reader")
		}
	}
	if ascendErr != nil {
		t.Fatalf("Ascend full scan error: %v", ascendErr)
	}

	// === Descend: "", "" => full database in descending order
	var descendOrder []string
	var descendErr error
	for key, val := range snap.Descend(ctx, "", "", &descendErr) {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		descendOrder = append(descendOrder, key)
		if val == nil {
			t.Error("Descend returned nil value reader")
		}
	}
	if descendErr != nil {
		t.Fatalf("Descend full scan error: %v", descendErr)
	}

	// Expected order (lexicographical byte order)
	expectedAsc := slices.Clone(keys)
	slices.Sort(expectedAsc)

	expectedDesc := slices.Clone(expectedAsc)
	slices.Reverse(expectedDesc)

	// Verify ascending order
	if !slices.Equal(ascendOrder, expectedAsc) {
		t.Errorf("Ascend full scan order mismatch\n got: %v\nwant: %v", ascendOrder, expectedAsc)
	}

	// Verify descending order
	if !slices.Equal(descendOrder, expectedDesc) {
		t.Errorf("Descend full scan order mismatch\n got: %v\nwant: %v", descendOrder, expectedDesc)
	}
}
