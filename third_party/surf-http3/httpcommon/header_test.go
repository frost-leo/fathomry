package httpcommon

import (
	"reflect"
	"testing"

	"github.com/enetx/http"
)

func keysOf(kvs []HeaderKeyValues) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = kv.Key
	}
	return out
}

// SortedKeyValuesBy must place keys that appear in the order map first, in the
// given order, and keys absent from the order map afterwards (lexicographically).
func TestSortedKeyValuesBy_FollowsOrder(t *testing.T) {
	h := http.Header{
		"X-A": {"1"},
		"X-B": {"2"},
		"X-C": {"3"},
		"Foo": {"4"},
	}
	order := map[string]int{"x-c": 0, "x-a": 1, "x-b": 2} // lowercase: Less lowercases keys

	kvs, hs := SortedKeyValuesBy(h, order, map[string]bool{})
	defer ReturnSorter(hs)

	got := keysOf(kvs)
	want := []string{"X-C", "X-A", "X-B", "Foo"} // ordered ones first, undefined "Foo" last
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order mismatch:\n got=%v\nwant=%v", got, want)
	}
}

// SortedKeyValues (no custom order) must sort lexicographically by key.
func TestSortedKeyValues_Lexicographic(t *testing.T) {
	h := http.Header{
		"X-C": {"1"},
		"X-A": {"2"},
		"Foo": {"3"},
		"X-B": {"4"},
	}
	kvs, hs := SortedKeyValues(h, map[string]bool{})
	defer ReturnSorter(hs)

	got := keysOf(kvs)
	want := []string{"Foo", "X-A", "X-B", "X-C"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lexicographic mismatch:\n got=%v\nwant=%v", got, want)
	}
}

// Regression test for the pool-stale-order bug: a SortedKeyValuesBy call returns
// its sorter (with a custom order) to the pool; a subsequent SortedKeyValues call
// reusing that sorter must NOT inherit the stale order. We loop to maximize the
// chance of drawing the same pooled object.
func TestSortedKeyValues_NoStaleOrderFromPool(t *testing.T) {
	// Prime the pool with a custom order whose keys collide with the next request.
	order := map[string]int{"x-c": 0, "x-a": 1, "x-b": 2}
	for i := 0; i < 64; i++ {
		_, hs := SortedKeyValuesBy(http.Header{
			"X-A": {"1"}, "X-B": {"2"}, "X-C": {"3"},
		}, order, map[string]bool{})
		ReturnSorter(hs)
	}

	want := []string{"X-A", "X-B", "X-C"}
	for i := 0; i < 64; i++ {
		kvs, hs := SortedKeyValues(http.Header{
			"X-C": {"1"}, "X-A": {"2"}, "X-B": {"3"},
		}, map[string]bool{})
		got := keysOf(kvs)
		ReturnSorter(hs)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: stale order leaked from pool:\n got=%v\nwant=%v", i, got, want)
		}
	}
}

// Excluded keys (the magic order keys) must never appear in the sorted output.
func TestSortedKeyValues_Exclude(t *testing.T) {
	h := http.Header{
		HeaderOrderKey:  {"x-a"},
		PHeaderOrderKey: {":method"},
		"X-A":           {"1"},
	}
	exclude := map[string]bool{HeaderOrderKey: true, PHeaderOrderKey: true}

	kvs, hs := SortedKeyValues(h, exclude)
	defer ReturnSorter(hs)

	for _, kv := range kvs {
		if kv.Key == HeaderOrderKey || kv.Key == PHeaderOrderKey {
			t.Fatalf("excluded key %q leaked into output", kv.Key)
		}
	}
	if got := keysOf(kvs); !reflect.DeepEqual(got, []string{"X-A"}) {
		t.Fatalf("unexpected output: %v", got)
	}
}
