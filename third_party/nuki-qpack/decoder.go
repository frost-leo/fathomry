package qpack

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/net/http2/hpack"
)

// An invalidIndexError is returned when decoding encounters an invalid index
// (e.g., an index that is out of bounds for the static table).
type invalidIndexError int

func (e invalidIndexError) Error() string {
	return fmt.Sprintf("invalid indexed representation index %d", int(e))
}

var errNoDynamicTable = errors.New("no dynamic table")

var errDecoderClosed = errors.New("qpack: decoder closed")

// A Decoder decodes QPACK header blocks.
// A Decoder can be reused to decode multiple header blocks on different streams
// on the same connection (e.g., headers then trailers).
//
// By default a Decoder only supports the static table. Passing
// [WithMaxTableCapacity] enables dynamic table support (RFC 9204): the encoder
// stream must then be fed to [Decoder.ParseEncoderStream], and decoder-stream
// instructions are emitted to the writer configured with [WithDecoderStream].
type Decoder struct {
	mutex sync.Mutex
	cond  *sync.Cond

	// dt is the dynamic table. It is nil when dynamic table support is
	// disabled, in which case the Decoder behaves exactly as the static-only
	// upstream decoder.
	dt *dynamicTable

	// decoderStr is the outgoing QPACK decoder stream, on which Section
	// Acknowledgment, Stream Cancellation and Insert Count Increment
	// instructions are emitted. It may be nil.
	decoderStr io.Writer
	writeMutex sync.Mutex

	// ackedInsertCount is the number of inserts acknowledged via Insert Count
	// Increment instructions so far.
	ackedInsertCount uint64

	closed    bool
	closeErr  error
	writeGate chan struct{}
}

// DecodeFunc is a function that decodes the next header field from a header block.
// It should be called repeatedly until it returns io.EOF.
// It returns io.EOF when all header fields have been decoded.
// Any error other than io.EOF indicates a decoding error.
type DecodeFunc func() (HeaderField, error)

// A DecoderOption configures a Decoder.
type DecoderOption func(*Decoder)

// WithMaxTableCapacity enables QPACK dynamic table support and sets the maximum
// dynamic table capacity, which must match the value advertised in
// SETTINGS_QPACK_MAX_TABLE_CAPACITY. A value of 0 leaves the dynamic table
// disabled (static-only decoding).
func WithMaxTableCapacity(max uint64) DecoderOption {
	return func(d *Decoder) {
		if max > 0 {
			d.dt = newDynamicTable(max)
		}
	}
}

// WithDecoderStream sets the writer for the outgoing QPACK decoder stream.
func WithDecoderStream(w io.Writer) DecoderOption {
	return func(d *Decoder) {
		d.decoderStr = w
	}
}

// SetDecoderStream sets the writer for the outgoing QPACK decoder stream after
// construction. This is useful when the stream is opened asynchronously. It is
// safe for concurrent use.
func (d *Decoder) SetDecoderStream(w io.Writer) {
	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()
	d.decoderStr = w
}

// NewDecoder returns a new Decoder.
func NewDecoder(opts ...DecoderOption) *Decoder {
	d := &Decoder{}
	for _, opt := range opts {
		opt(d)
	}
	d.cond = sync.NewCond(&d.mutex)
	d.writeGate = make(chan struct{}, 1)
	return d
}

// Close shuts the Decoder down, unblocking any header-block decode that is
// waiting for dynamic table inserts.
func (d *Decoder) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.closed = true
	if d.closeErr == nil {
		d.closeErr = errDecoderClosed
	}
	d.cond.Broadcast()
	return nil
}

// Decode returns a function that decodes header fields from the given header block.
// It does not copy the slice; the caller must ensure it remains valid during decoding.
//
// When dynamic table support is enabled, no Section Acknowledgment is emitted;
// use [Decoder.DecodeForStream] to acknowledge the decoded section.
func (d *Decoder) Decode(p []byte) DecodeFunc {
	if d.dt == nil {
		return d.decodeStatic(p)
	}
	return d.decodeDynamic(-1, p)
}

// DecodeForStream is like [Decoder.Decode], but on a Decoder with dynamic table
// support it emits a Section Acknowledgment for streamID once a header block
// that referenced the dynamic table has been fully decoded.
func (d *Decoder) DecodeForStream(streamID int64, p []byte) DecodeFunc {
	if d.dt == nil {
		return d.decodeStatic(p)
	}
	return d.decodeDynamic(streamID, p)
}

// decodeStatic is the static-table-only decode path. It matches the behavior of
// the upstream qpack decoder and rejects any use of the dynamic table.
func (d *Decoder) decodeStatic(p []byte) DecodeFunc {
	var readRequiredInsertCount bool
	var readDeltaBase bool

	return func() (HeaderField, error) {
		if !readRequiredInsertCount {
			requiredInsertCount, rest, err := readVarInt(8, p)
			if err != nil {
				return HeaderField{}, err
			}
			p = rest
			readRequiredInsertCount = true
			if requiredInsertCount != 0 {
				return HeaderField{}, errors.New("expected Required Insert Count to be zero")
			}
		}

		if !readDeltaBase {
			base, rest, err := readVarInt(7, p)
			if err != nil {
				return HeaderField{}, err
			}
			p = rest
			readDeltaBase = true
			if base != 0 {
				return HeaderField{}, errors.New("expected Base to be zero")
			}
		}

		if len(p) == 0 {
			return HeaderField{}, io.EOF
		}

		b := p[0]
		var hf HeaderField
		var rest []byte
		var err error
		switch {
		case (b & 0x80) > 0: // 1xxxxxxx
			hf, rest, err = d.parseIndexedHeaderField(p)
		case (b & 0xc0) == 0x40: // 01xxxxxx
			hf, rest, err = d.parseLiteralHeaderField(p)
		case (b & 0xe0) == 0x20: // 001xxxxx
			hf, rest, err = d.parseLiteralHeaderFieldWithoutNameReference(p)
		default:
			err = fmt.Errorf("unexpected type byte: %#x", b)
		}
		p = rest
		if err != nil {
			return HeaderField{}, err
		}
		return hf, nil
	}
}

func (d *Decoder) parseIndexedHeaderField(buf []byte) (_ HeaderField, rest []byte, _ error) {
	if buf[0]&0x40 == 0 {
		return HeaderField{}, buf, errNoDynamicTable
	}
	index, rest, err := readVarInt(6, buf)
	if err != nil {
		return HeaderField{}, buf, err
	}
	hf, ok := d.at(index)
	if !ok {
		return HeaderField{}, buf, invalidIndexError(index)
	}
	return hf, rest, nil
}

func (d *Decoder) parseLiteralHeaderField(buf []byte) (_ HeaderField, rest []byte, _ error) {
	if buf[0]&0x10 == 0 {
		return HeaderField{}, buf, errNoDynamicTable
	}
	// We don't need to check the value of the N-bit here.
	// It's only relevant when re-encoding header fields,
	// and determines whether the header field can be added to the dynamic table.
	index, rest, err := readVarInt(4, buf)
	if err != nil {
		return HeaderField{}, buf, err
	}
	hf, ok := d.at(index)
	if !ok {
		return HeaderField{}, buf, invalidIndexError(index)
	}
	buf = rest
	if len(buf) == 0 {
		return HeaderField{}, buf, io.ErrUnexpectedEOF
	}
	usesHuffman := buf[0]&0x80 > 0
	val, rest, err := d.readString(rest, 7, usesHuffman)
	if err != nil {
		return HeaderField{}, rest, err
	}
	hf.Value = val
	return hf, rest, nil
}

func (d *Decoder) parseLiteralHeaderFieldWithoutNameReference(buf []byte) (_ HeaderField, rest []byte, _ error) {
	usesHuffmanForName := buf[0]&0x8 > 0
	name, rest, err := d.readString(buf, 3, usesHuffmanForName)
	if err != nil {
		return HeaderField{}, rest, err
	}
	buf = rest
	if len(buf) == 0 {
		return HeaderField{}, rest, io.ErrUnexpectedEOF
	}
	usesHuffmanForVal := buf[0]&0x80 > 0
	val, rest, err := d.readString(buf, 7, usesHuffmanForVal)
	if err != nil {
		return HeaderField{}, rest, err
	}
	return HeaderField{Name: name, Value: val}, rest, nil
}

func (d *Decoder) readString(buf []byte, n uint8, usesHuffman bool) (string, []byte, error) {
	l, buf, err := readVarInt(n, buf)
	if err != nil {
		return "", nil, err
	}
	if uint64(len(buf)) < l {
		return "", nil, io.ErrUnexpectedEOF
	}
	var val string
	if usesHuffman {
		val, err = hpack.HuffmanDecodeToString(buf[:l])
		if err != nil {
			return "", nil, err
		}
	} else {
		val = string(buf[:l])
	}
	buf = buf[l:]
	return val, buf, nil
}

func (d *Decoder) at(i uint64) (hf HeaderField, ok bool) {
	if i >= uint64(len(staticTableEntries)) {
		return
	}
	return staticTableEntries[i], true
}
