package kvtests

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestRangeDescendBounds verifies that Descend correctly respects the
// half-open [begin, end) interval in reverse order:
//   - begin key is INCLUDED
//   - end key is EXCLUDED
//   - iteration proceeds from highest to lowest key in the range
func TestRangeDescendBounds(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestRangeDescendBounds/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Keys in lexicographical order (a < b < c < ... < z)
	keys := []string{
		prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "e",
		prefix + "f", prefix + "g", prefix + "h", prefix + "i", prefix + "j",
		prefix + "x", prefix + "y", prefix + "z",
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

	tests := []struct {
		name     string
		begin    string
		end      string
		wantKeys []string // expected keys in descending order
	}{
		{
			name:     "full range reverse",
			begin:    prefix + "a",
			end:      prefix + "z",
			wantKeys: []string{prefix + "y", prefix + "x", prefix + "j", prefix + "i", prefix + "h", prefix + "g", prefix + "f", prefix + "e", prefix + "d", prefix + "c", prefix + "b", prefix + "a"},
		},
		{
			name:     "exclude end key",
			begin:    prefix + "c",
			end:      prefix + "g",
			wantKeys: []string{prefix + "f", prefix + "e", prefix + "d", prefix + "c"},
		},
		{
			name:     "include begin only",
			begin:    prefix + "x",
			end:      prefix + "z",
			wantKeys: []string{prefix + "y", prefix + "x"},
		},
		{
			name:     "begin == end => empty",
			begin:    prefix + "h",
			end:      prefix + "h",
			wantKeys: nil,
		},
		{
			name:     "open upper bound",
			begin:    prefix + "f",
			end:      "",
			wantKeys: []string{prefix + "z", prefix + "y", prefix + "x", prefix + "j", prefix + "i", prefix + "h", prefix + "g", prefix + "f"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			var iterErr error

			for key, val := range snap.Descend(ctx, tc.begin, tc.end, &iterErr) {
				if !strings.HasPrefix(key, prefix) {
					continue
				}
				got = append(got, key)
				if val == nil {
					t.Error("Descend returned nil value reader")
				}
			}
			if iterErr != nil {
				t.Fatalf("Descend(%q, %q) iteration error: %v", tc.begin, tc.end, iterErr)
			}

			if !slices.Equal(got, tc.wantKeys) {
				t.Errorf("Descend(%q, %q) wrong order\n got: %v\nwant: %v", tc.begin, tc.end, got, tc.wantKeys)
			}
		})
	}
}
