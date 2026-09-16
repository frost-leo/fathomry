package qpack

import "fmt"

// dynamicTableEntry is a single entry in the QPACK dynamic table.
type dynamicTableEntry struct {
	name  string
	value string
}

// size returns the size of the entry as defined in RFC 9204, Section 3.2.1:
// the length of its name plus the length of its value plus 32.
func (e dynamicTableEntry) size() uint64 {
	return uint64(len(e.name) + len(e.value) + 32)
}

// dynamicTable is the decoder's representation of the QPACK dynamic table
// (RFC 9204, Section 3.2). Entries are addressed by absolute index: the entry
// inserted i-th (0-based over the lifetime of the connection) has absolute
// index i, and insertCount is the total number of entries ever inserted (i.e.
// the absolute index the next inserted entry will receive).
//
// dynamicTable is not safe for concurrent use; callers synchronize access.
type dynamicTable struct {
	entries     []dynamicTableEntry // oldest first
	insertCount uint64
	size        uint64 // current total size of all entries
	capacity    uint64 // current capacity, set via Set Dynamic Table Capacity
	maxCapacity uint64 // upper bound, from SETTINGS_QPACK_MAX_TABLE_CAPACITY
}

func newDynamicTable(maxCapacity uint64) *dynamicTable {
	return &dynamicTable{maxCapacity: maxCapacity}
}

// firstIndex returns the absolute index of the oldest entry still in the table.
func (t *dynamicTable) firstIndex() uint64 {
	return t.insertCount - uint64(len(t.entries))
}

// setCapacity applies a Set Dynamic Table Capacity instruction (RFC 9204,
// Section 4.3.1). The new capacity must not exceed the value advertised in
// SETTINGS_QPACK_MAX_TABLE_CAPACITY.
func (t *dynamicTable) setCapacity(capacity uint64) error {
	if capacity > t.maxCapacity {
		return fmt.Errorf("qpack: dynamic table capacity %d exceeds maximum %d", capacity, t.maxCapacity)
	}
	t.capacity = capacity
	t.evictTo(capacity)
	return nil
}

// insert adds a new entry, evicting the oldest entries as needed to stay within
// capacity (RFC 9204, Section 3.2.2).
func (t *dynamicTable) insert(name, value string) error {
	e := dynamicTableEntry{name: name, value: value}
	if e.size() > t.capacity {
		return fmt.Errorf("qpack: entry of size %d exceeds dynamic table capacity %d", e.size(), t.capacity)
	}
	t.evictTo(t.capacity - e.size())
	t.entries = append(t.entries, e)
	t.size += e.size()
	t.insertCount++
	return nil
}

// duplicate applies a Duplicate instruction (RFC 9204, Section 4.3.4),
// re-inserting the entry at the given relative index.
func (t *dynamicTable) duplicate(relativeIndex uint64) error {
	e, ok := t.atRelative(relativeIndex)
	if !ok {
		return fmt.Errorf("qpack: Duplicate references invalid relative index %d", relativeIndex)
	}
	return t.insert(e.name, e.value)
}

// evictTo evicts the oldest entries until the total size is at most target.
func (t *dynamicTable) evictTo(target uint64) {
	for len(t.entries) > 0 && t.size > target {
		t.size -= t.entries[0].size()
		t.entries[0] = dynamicTableEntry{}
		t.entries = t.entries[1:]
	}
}

// at returns the entry at the given absolute index.
func (t *dynamicTable) at(absoluteIndex uint64) (dynamicTableEntry, bool) {
	first := t.firstIndex()
	if absoluteIndex < first || absoluteIndex >= t.insertCount {
		return dynamicTableEntry{}, false
	}
	return t.entries[absoluteIndex-first], true
}

// atRelative returns the entry at the given index relative to the most recently
// inserted entry (used on the encoder stream, RFC 9204, Section 3.2.5).
// Relative index 0 refers to the most recently inserted entry.
func (t *dynamicTable) atRelative(relativeIndex uint64) (dynamicTableEntry, bool) {
	if relativeIndex >= t.insertCount {
		return dynamicTableEntry{}, false
	}
	return t.at(t.insertCount - 1 - relativeIndex)
}
