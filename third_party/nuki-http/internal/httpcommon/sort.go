package httpcommon

import (
	"slices"
	"strings"
	"sync"
)

// HeaderOrderKey is a magic Key for ResponseWriter.Header map keys
// that, if present, defines a header order that will be used to
// write the headers onto wire. The order of the slice defined how the headers
// will be sorted. A defined Key goes before an undefined Key.
//
// This is the only way to specify some order, because maps don't
// have a a stable iteration order. If no order is given, headers will
// be sorted lexicographically.
//
// According to RFC2616 it is good practice to send general-header fields
// first, followed by request-header or response-header fields and ending
// with entity-header fields.
const HeaderOrderKey = "Header-Order"

type KeyValues struct {
	Key    string
	Values []string
}

// headerSorter contains a slice of keyValues sorted by keyValues.key.
type HeaderSorter struct {
	kvs []KeyValues
}

var HeaderSorterPool = sync.Pool{
	New: func() any { return new(HeaderSorter) },
}

// sortedKeyValues returns h's keys sorted in the returned kvs
// slice. The headerSorter used to sort is also returned, for possible
// return to headerSorterCache.
func SortedKeyValues(header map[string][]string) (kvs []KeyValues, hs *HeaderSorter) {
	hs = HeaderSorterPool.Get().(*HeaderSorter)
	if cap(hs.kvs) < len(header) {
		hs.kvs = make([]KeyValues, 0, len(header))
	}
	var ordermap map[string]int
	if order, ok := header[HeaderOrderKey]; ok {
		ordermap = make(map[string]int, len(order))
		for i, k := range order {
			ordermap[strings.ToLower(k)] = i
		}
	}
	kvs = hs.kvs[:0]
	for k, vv := range header {
		if k != HeaderOrderKey {
			kvs = append(kvs, KeyValues{k, vv})
		}
	}
	hs.kvs = kvs
	slices.SortFunc(hs.kvs, func(a, b KeyValues) int {
		if ordermap == nil {
			return strings.Compare(a.Key, b.Key)
		}
		oa, oka := ordermap[strings.ToLower(a.Key)]
		ob, okb := ordermap[strings.ToLower(b.Key)]
		if oka && okb {
			return oa - ob
		}
		if oka {
			return -1
		}
		if okb {
			return 1
		}
		return strings.Compare(a.Key, b.Key)
	})
	return kvs, hs
}
