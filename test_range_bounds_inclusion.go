package kvtests

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestRangeBoundsInclusion verifies that:
//
//   - The begin key is INCLUDED in the range
//   - The end key is EXCLUDED from the range
//
// This must hold for both Ascend and Descend.
func TestRangeBoundsInclusion(ctx context.Context, t *testing.T, db kv.Database) {
	const prefix = "/TestRangeBoundsInclusion/"

	cleanupPrefix(ctx, t, db, prefix)
	defer cleanupPrefix(ctx, t, db, prefix)

	// Keys in strict lexicographical order
	keys := []string{
		prefix + "a",
		prefix + "b",
		prefix + "c",
		prefix + "d",
		prefix + "e",
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
		name       string
		begin, end string
		wantAsc    []string // expected keys in Ascend
		wantDesc   []string // expected keys in Descend (same set, reverse order)
	}{
		{
			name:    "include begin, exclude end",
			begin:   prefix + "b",
			end:     prefix + "d",
			wantAsc: []string{prefix + "b", prefix + "c"},
		},
		{
			name:    "begin == end → empty range",
			begin:   prefix + "c",
			end:     prefix + "c",
			wantAsc: nil,
		},
		{
			name:    "begin at first key",
			begin:   prefix + "a",
			end:     prefix + "c",
			wantAsc: []string{prefix + "a", prefix + "b"},
		},
		{
			name:    "end at last key",
			begin:   prefix + "d",
			end:     prefix + "e",
			wantAsc: []string{prefix + "d"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Ascend
			var gotAsc []string
			var ascErr error
			for key, val := range snap.Ascend(ctx, tc.begin, tc.end, &ascErr) {
				if !strings.HasPrefix(key, prefix) {
					continue
				}
				gotAsc = append(gotAsc, key)
				if val == nil {
					t.Error("Ascend returned nil value reader")
				}
			}
			if ascErr != nil {
				t.Fatalf("Ascend error: %v", ascErr)
			}
			if !slices.Equal(gotAsc, tc.wantAsc) {
				t.Errorf("Ascend(%q, %q)\n got: %v\nwant: %v", tc.begin, tc.end, gotAsc, tc.wantAsc)
			}

			// Descend — should see same keys, just reversed
			var gotDesc []string
			var descErr error
			for key, val := range snap.Descend(ctx, tc.begin, tc.end, &descErr) {
				if !strings.HasPrefix(key, prefix) {
					continue
				}
				gotDesc = append(gotDesc, key)
				if val == nil {
					t.Error("Descend returned nil value reader")
				}
			}
			if descErr != nil {
				t.Fatalf("Descend error: %v", descErr)
			}

			expectedDesc := slices.Clone(tc.wantAsc)
			slices.Reverse(expectedDesc)
			if !slices.Equal(gotDesc, expectedDesc) {
				t.Errorf("Descend(%q, %q)\n got: %v\nwant: %v", tc.begin, tc.end, gotDesc, expectedDesc)
			}
		})
	}
}
