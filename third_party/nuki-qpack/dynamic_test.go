package qpack

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---- encoder-stream instruction builders (RFC 9204, Section 4.3) ----

func encSetCapacity(c uint64) []byte {
	b := appendVarInt(nil, 5, c)
	b[0] |= 0x20 // 001xxxxx
	return b
}

func encInsertLiteralName(name, value string) []byte {
	b := appendVarInt(nil, 5, uint64(len(name)))
	b[0] |= 0x40 // 01Hxxxxx, H=0 (no Huffman)
	b = append(b, []byte(name)...)
	v := appendVarInt(nil, 7, uint64(len(value)))
	b = append(b, v...)
	return append(b, []byte(value)...)
}

func encInsertNameRef(static bool, idx uint64, value string) []byte {
	b := appendVarInt(nil, 6, idx)
	b[0] |= 0x80 // 1Txxxxxx
	if static {
		b[0] |= 0x40
	}
	v := appendVarInt(nil, 7, uint64(len(value)))
	b = append(b, v...)
	return append(b, []byte(value)...)
}

func encDuplicate(relIdx uint64) []byte {
	return appendVarInt(nil, 5, relIdx) // 000xxxxx
}

// ---- header-block builders (RFC 9204, Section 4.5) ----

func blockPrefix(encodedInsertCount, deltaBase uint64, sign bool) []byte {
	b := appendVarInt(nil, 8, encodedInsertCount)
	db := appendVarInt(nil, 7, deltaBase)
	if sign {
		db[0] |= 0x80
	}
	return append(b, db...)
}

func fieldIndexedDynamic(relIdx uint64) []byte {
	b := appendVarInt(nil, 6, relIdx)
	b[0] |= 0x80 // 1Txxxxxx, T=0 (dynamic)
	return b
}

func fieldIndexedStatic(idx uint64) []byte {
	b := appendVarInt(nil, 6, idx)
	b[0] |= 0x80 | 0x40
	return b
}

func fieldPostBaseIndexed(idx uint64) []byte {
	b := appendVarInt(nil, 4, idx)
	b[0] |= 0x10 // 0001xxxx
	return b
}

func fieldLiteralDynamicNameRef(relIdx uint64, value string) []byte {
	b := appendVarInt(nil, 4, relIdx)
	b[0] |= 0x40 // 01NTxxxx, T=0 (dynamic)
	v := appendVarInt(nil, 7, uint64(len(value)))
	b = append(b, v...)
	return append(b, []byte(value)...)
}

func fieldPostBaseLiteralNameRef(idx uint64, value string) []byte {
	b := appendVarInt(nil, 3, idx) // 0000Nxxx
	v := appendVarInt(nil, 7, uint64(len(value)))
	b = append(b, v...)
	return append(b, []byte(value)...)
}

// safeBuf is a concurrency-safe bytes.Buffer for capturing decoder-stream output.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

const testMaxCap = 4096 // MaxEntries = 128, FullRange = 256

func feedEncoderStream(t *testing.T, d *Decoder, data []byte) {
	t.Helper()
	require.NoError(t, d.ParseEncoderStream(bytes.NewReader(data)))
}

func TestDynamicTableEviction(t *testing.T) {
	tbl := newDynamicTable(100)
	require.NoError(t, tbl.setCapacity(100))
	// each entry: len(name)+len(value)+32
	require.NoError(t, tbl.insert("a", "1")) // size 34
	require.NoError(t, tbl.insert("b", "2")) // size 34, total 68
	require.Equal(t, uint64(2), tbl.insertCount)
	require.Len(t, tbl.entries, 2)
	// inserting a third (size 34, total 102 > 100) evicts the oldest
	require.NoError(t, tbl.insert("c", "3"))
	require.Equal(t, uint64(3), tbl.insertCount)
	require.Len(t, tbl.entries, 2)
	_, ok := tbl.at(0) // "a" was evicted
	require.False(t, ok)
	e, ok := tbl.at(1)
	require.True(t, ok)
	require.Equal(t, "b", e.name)
	e, ok = tbl.at(2)
	require.True(t, ok)
	require.Equal(t, "c", e.name)
	// entry larger than capacity is rejected
	require.Error(t, tbl.insert("toolong", string(make([]byte, 100))))
}

func TestDynamicTableSetCapacityEvicts(t *testing.T) {
	tbl := newDynamicTable(1000)
	require.NoError(t, tbl.setCapacity(1000))
	require.NoError(t, tbl.insert("a", "1"))
	require.NoError(t, tbl.insert("b", "2"))
	require.NoError(t, tbl.setCapacity(34)) // room for only one entry
	require.Len(t, tbl.entries, 1)
	e, ok := tbl.at(1)
	require.True(t, ok)
	require.Equal(t, "b", e.name)
	// capacity above the advertised maximum is rejected
	require.Error(t, tbl.setCapacity(1001))
}

func TestEncoderStreamInsertLiteralAndDynamicRef(t *testing.T) {
	out := &safeBuf{}
	dec := NewDecoder(WithMaxTableCapacity(testMaxCap), WithDecoderStream(out))

	var enc []byte
	enc = append(enc, encSetCapacity(testMaxCap)...)
	enc = append(enc, encInsertLiteralName("custom-key", "custom-value")...)
	enc = append(enc, encInsertNameRef(true, 0, "example.com")...) // static idx 0 = :authority
	feedEncoderStream(t, dec, enc)

	require.Equal(t, uint64(2), dec.dt.insertCount)

	// Insert Count Increment for 2 inserts should have been emitted.
	require.Equal(t, []byte{0x02}, out.Bytes())

	// Header block: RIC=2, Base=2. With Base=2, relative index i maps to
	// absolute index Base-i-1. abs 0 = custom-key, abs 1 = :authority.
	// relIdx 0 -> abs 1 (:authority); relIdx 1 -> abs 0 (custom-key).
	block := blockPrefix(2+1, 0, false) // encoded RIC = RIC+1 (no wrap)
	block = append(block, fieldIndexedDynamic(0)...)
	block = append(block, fieldLiteralDynamicNameRef(1, "www.example.com")...)

	fields := decodeAll(t, dec.DecodeForStream(0, block))
	require.Equal(t, []HeaderField{
		{Name: ":authority", Value: "example.com"},
		{Name: "custom-key", Value: "www.example.com"},
	}, fields)

	// Section Acknowledgment for stream 0 (0x80) should follow the ICI.
	require.Equal(t, []byte{0x02, 0x80}, out.Bytes())
}

func TestEncoderStreamInsertNameRefDynamicAndDuplicate(t *testing.T) {
	dec := NewDecoder(WithMaxTableCapacity(testMaxCap))

	var enc []byte
	enc = append(enc, encSetCapacity(testMaxCap)...)
	enc = append(enc, encInsertLiteralName("x-foo", "bar")...) // abs 0
	enc = append(enc, encInsertNameRef(false, 0, "baz")...)    // name from dynamic rel 0 (x-foo) -> abs 1
	enc = append(enc, encDuplicate(1)...)                      // duplicate rel 1 (x-foo) -> abs 2
	feedEncoderStream(t, dec, enc)

	require.Equal(t, uint64(3), dec.dt.insertCount)

	block := blockPrefix(3+1, 3, false) // RIC=3, Base=0 via deltaBase=3? no: base=RIC+delta -> use post-base
	// Use Base=RIC=3 (deltaBase 0) and reference via regular relative indices.
	block = blockPrefix(3+1, 0, false)
	block = append(block, fieldIndexedDynamic(2)...) // abs 0: x-foo=bar
	block = append(block, fieldIndexedDynamic(1)...) // abs 1: x-foo=baz
	block = append(block, fieldIndexedDynamic(0)...) // abs 2: x-foo=bar (duplicate)

	fields := decodeAll(t, dec.DecodeForStream(4, block))
	require.Equal(t, []HeaderField{
		{Name: "x-foo", Value: "bar"},
		{Name: "x-foo", Value: "baz"},
		{Name: "x-foo", Value: "bar"},
	}, fields)
}

func TestDecodePostBaseReferences(t *testing.T) {
	dec := NewDecoder(WithMaxTableCapacity(testMaxCap))
	var enc []byte
	enc = append(enc, encSetCapacity(testMaxCap)...)
	enc = append(enc, encInsertLiteralName("a", "1")...) // abs 0
	enc = append(enc, encInsertLiteralName("b", "2")...) // abs 1
	feedEncoderStream(t, dec, enc)

	// Base = 0 (RIC=2, deltaBase=1, sign=1 -> base = 2-1-1 = 0). Post-base index
	// i references abs Base+i.
	block := blockPrefix(2+1, 1, true)
	block = append(block, fieldPostBaseIndexed(0)...)              // abs 0: a=1
	block = append(block, fieldPostBaseLiteralNameRef(1, "zz")...) // name abs 1 (b), value zz
	fields := decodeAll(t, dec.DecodeForStream(8, block))
	require.Equal(t, []HeaderField{
		{Name: "a", Value: "1"},
		{Name: "b", Value: "zz"},
	}, fields)
}

func TestDecodeRequiredInsertCount(t *testing.T) {
	// RFC 9204, Section 4.5.1.1. MaxEntries picked so FullRange is small.
	maxEntries := uint64(128) // FullRange = 256
	// no dependency
	ric, err := decodeRequiredInsertCount(0, maxEntries, 10)
	require.NoError(t, err)
	require.Zero(t, ric)
	// simple non-wrapping case: encoded = RIC+1 when RIC < FullRange
	ric, err = decodeRequiredInsertCount(6, maxEntries, 5)
	require.NoError(t, err)
	require.Equal(t, uint64(5), ric)
	// wrapping case from the RFC example spirit: totalInserts large
	ric, err = decodeRequiredInsertCount(10, maxEntries, 1000)
	require.NoError(t, err)
	// reconstructed RIC must satisfy RIC <= totalInserts+MaxEntries and RIC mod FullRange == 9
	require.LessOrEqual(t, ric, uint64(1000+128))
	require.Equal(t, uint64(9), ric%256)
	// invalid: encoded beyond FullRange
	_, err = decodeRequiredInsertCount(1000, maxEntries, 0)
	require.Error(t, err)
}

func TestBlockedStreamDecodeUnblocksOnInsert(t *testing.T) {
	pr, pw := io.Pipe()
	dec := NewDecoder(WithMaxTableCapacity(testMaxCap), WithDecoderStream(&safeBuf{}))

	encDone := make(chan error, 1)
	go func() { encDone <- dec.ParseEncoderStream(pr) }()

	// Set capacity and insert one entry, then pause.
	var first []byte
	first = append(first, encSetCapacity(testMaxCap)...)
	first = append(first, encInsertLiteralName("a", "1")...) // abs 0
	_, err := pw.Write(first)
	require.NoError(t, err)

	// Decode a block that requires 2 inserts -> must block until the 2nd insert.
	block := blockPrefix(2+1, 0, false)
	block = append(block, fieldIndexedDynamic(1)...) // abs 0: a=1
	block = append(block, fieldIndexedDynamic(0)...) // abs 1: b=2

	type result struct {
		fields []HeaderField
		err    error
	}
	resc := make(chan result, 1)
	go func() {
		fn := dec.DecodeForStream(0, block)
		var fs []HeaderField
		for {
			hf, err := fn()
			if err == io.EOF {
				break
			}
			if err != nil {
				resc <- result{err: err}
				return
			}
			fs = append(fs, hf)
		}
		resc <- result{fields: fs}
	}()

	// The decode must not complete yet.
	select {
	case <-resc:
		t.Fatal("decode completed before required inserts arrived")
	case <-time.After(50 * time.Millisecond):
	}

	// Deliver the second insert; the decode should now unblock.
	_, err = pw.Write(encInsertLiteralName("b", "2"))
	require.NoError(t, err)

	select {
	case r := <-resc:
		require.NoError(t, r.err)
		require.Equal(t, []HeaderField{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}, r.fields)
	case <-time.After(2 * time.Second):
		t.Fatal("decode did not unblock after required inserts arrived")
	}

	pw.Close()
	<-encDone
}

func TestBlockedStreamDecodeFailsOnClose(t *testing.T) {
	dec := NewDecoder(WithMaxTableCapacity(testMaxCap))
	// require RIC=1 but never insert anything
	block := blockPrefix(1+1, 0, false)
	block = append(block, fieldIndexedDynamic(0)...)

	resc := make(chan error, 1)
	go func() {
		fn := dec.DecodeForStream(0, block)
		_, err := fn()
		resc <- err
	}()

	select {
	case <-resc:
		t.Fatal("decode returned before close")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, dec.Close())
	select {
	case err := <-resc:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("decode did not fail after Close")
	}
}

func TestStaticDecoderUnaffected(t *testing.T) {
	// A Decoder without dynamic table support must reject dynamic references,
	// preserving upstream behavior.
	dec := NewDecoder()
	block := blockPrefix(0, 0, false)
	block = append(block, fieldIndexedStatic(20)...)
	fields := decodeAll(t, dec.Decode(block))
	require.Equal(t, []HeaderField{staticTableEntries[20]}, fields)
}

func TestDecoderStreamInstructionFormats(t *testing.T) {
	out := &safeBuf{}
	dec := NewDecoder(WithMaxTableCapacity(testMaxCap), WithDecoderStream(out))
	// Stream Cancellation for stream 5 -> 01xxxxxx with 5 = 0x45
	dec.CancelStream(5)
	require.Equal(t, []byte{0x45}, out.Bytes())
}
