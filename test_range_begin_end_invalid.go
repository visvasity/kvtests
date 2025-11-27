package kvtests

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/visvasity/kv"
)

// TestRangeBeginEndInvalid verifies that Ascend/Descend reject only truly invalid ranges:
// when both begin and end are non-empty AND begin > end.
// Cases with empty begin or end are special and MUST be supported.
func TestRangeBeginEndInvalid(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestRangeBeginEndInvalid/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Discard(ctx)

	// Test cases: only these are invalid (both non-empty AND begin > end)
	invalidCases := []struct {
		name  string
		begin string
		end   string
	}{
		{"simple reverse", "b", "a"},
		{"same length", "zoo", "aaa"},
		{"different length", "abc", "ab"},
		{"with prefix", prefix + "z", prefix + "a"},
		{"byte order", "z", "a"},
	}

	for _, tc := range invalidCases {
		t.Run("Invalid/"+tc.name, func(t *testing.T) {
			var ascendErr, descendErr error

			// Must NOT iterate and must set error = os.ErrInvalid
			for range snap.Ascend(ctx, tc.begin, tc.end, &ascendErr) {
				t.Fatal("Ascend iterated on invalid range")
			}
			if !errors.Is(ascendErr, os.ErrInvalid) {
				t.Errorf("Ascend(%q, %q) = %v; want os.ErrInvalid", tc.begin, tc.end, ascendErr)
			}

			for range snap.Descend(ctx, tc.begin, tc.end, &descendErr) {
				t.Fatal("Descend iterated on invalid range")
			}
			if !errors.Is(descendErr, os.ErrInvalid) {
				t.Errorf("Descend(%q, %q) = %v; want os.ErrInvalid", tc.begin, tc.end, descendErr)
			}
		})
	}

	// Valid special cases — must be accepted and work correctly
	validSpecial := []struct {
		name        string
		begin, end  string
		description string
	}{
		{"both empty", "", "", "entire database"},
		{"empty begin", "", "zzz", "from smallest key to zzz"},
		{"empty end", "aaa", "", "from aaa to +∞"},
	}

	for _, tc := range validSpecial {
		t.Run("Valid/"+tc.name, func(t *testing.T) {
			var ascendErr, descendErr error
			ascendCount := 0
			descendCount := 0

			for range snap.Ascend(ctx, tc.begin, tc.end, &ascendErr) {
				ascendCount++
			}
			for range snap.Descend(ctx, tc.begin, tc.end, &descendErr) {
				descendCount++
			}

			if ascendErr != nil {
				t.Errorf("Ascend(%q, %q) failed: %v", tc.begin, tc.end, ascendErr)
			}
			if descendErr != nil {
				t.Errorf("Descend(%q, %q) failed: %v", tc.begin, tc.end, descendErr)
			}

			// Since DB is empty, both should return 0 items — but no error
			if ascendCount != 0 || descendCount != 0 {
				t.Logf("Warning: found %d/%d keys in empty DB (possible leftover data)", ascendCount, descendCount)
			}
		})
	}
}
