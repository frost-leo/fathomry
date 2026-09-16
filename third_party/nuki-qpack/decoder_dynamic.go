package qpack

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"golang.org/x/net/http2/hpack"
)

// readIntFrom reads a prefixed variable-length integer (RFC 9204, Section 4.1.1)
// from a byte stream, given the already-read first byte and the prefix length n.
func readIntFrom(first byte, n byte, br io.ByteReader) (uint64, error) {
	mask := uint64(1<<n) - 1
	i := uint64(first) & mask
	if i < mask {
		return i, nil
	}
	var m uint
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		i += uint64(b&0x7f) << m
		if b&0x80 == 0 {
			break
		}
		m += 7
		if m >= 63 {
			return 0, errVarintOverflow
		}
	}
	return i, nil
}

// readStringFrom reads a length-prefixed string literal (RFC 9204, Section 4.1.2)
// from a byte stream, given the already-read first byte and the prefix length n.
// The Huffman bit is the bit at position n of the first byte.
func readStringFrom(first byte, n byte, r *bufio.Reader, limit ...uint64) (string, error) {
	huffman := first&(1<<n) != 0
	l, err := readIntFrom(first, n, r)
	if err != nil {
		return "", err
	}
	if len(limit) > 0 && l > limit[0] {
		return "", ErrHeaderLimit
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if huffman {
		return hpack.HuffmanDecodeToString(buf)
	}
	return string(buf), nil
}

// ParseEncoderStream reads and applies QPACK encoder-stream instructions
// (RFC 9204, Section 4.3) from r until r is closed or an error occurs. It
// mutates the decoder's dynamic table and emits Insert Count Increment
// instructions on the decoder stream. It blocks and is intended to be run on
// its own goroutine for the lifetime of the connection.
//
// It returns nil when the stream ends cleanly (io.EOF), and a non-nil error on
// a protocol violation or transport error. In either case any header-block
// decode that is blocked waiting for inserts is unblocked.
func (d *Decoder) ParseEncoderStream(r io.Reader) error {
	if d.dt == nil {
		// No dynamic table configured: drain and ignore.
		_, err := io.Copy(io.Discard, r)
		return err
	}
	br := bufio.NewReader(r)
	for {
		first, err := br.ReadByte()
		if err != nil {
			d.abort(err)
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := d.parseEncoderInstruction(first, br); err != nil {
			d.abort(err)
			return err
		}
		// Batch Insert Count Increments per read burst: only acknowledge once
		// the buffered instructions have been drained.
		if br.Buffered() == 0 {
			d.sendInsertCountIncrement()
		}
	}
}

func (d *Decoder) parseEncoderInstruction(first byte, br *bufio.Reader) error {
	switch {
	case first&0x80 != 0: // 1Txxxxxx: Insert with Name Reference (Section 4.3.2)
		fromStatic := first&0x40 != 0
		nameIdx, err := readIntFrom(first, 6, br)
		if err != nil {
			return err
		}
		vb, err := br.ReadByte()
		if err != nil {
			return err
		}
		value, err := readStringFrom(vb, 7, br, d.dt.maxCapacity)
		if err != nil {
			return err
		}
		var name string
		if fromStatic {
			if nameIdx >= uint64(len(staticTableEntries)) {
				return invalidIndexError(nameIdx)
			}
			name = staticTableEntries[nameIdx].Name
		} else {
			d.mutex.Lock()
			e, ok := d.dt.atRelative(nameIdx)
			d.mutex.Unlock()
			if !ok {
				return fmt.Errorf("qpack: Insert with Name Reference to invalid dynamic index %d", nameIdx)
			}
			name = e.name
		}
		return d.applyInsert(name, value)
	case first&0xc0 == 0x40: // 01HXxxxx: Insert with Literal Name (Section 4.3.3)
		name, err := readStringFrom(first, 5, br, d.dt.maxCapacity)
		if err != nil {
			return err
		}
		vb, err := br.ReadByte()
		if err != nil {
			return err
		}
		value, err := readStringFrom(vb, 7, br, d.dt.maxCapacity)
		if err != nil {
			return err
		}
		return d.applyInsert(name, value)
	case first&0xe0 == 0x20: // 001xxxxx: Set Dynamic Table Capacity (Section 4.3.1)
		capacity, err := readIntFrom(first, 5, br)
		if err != nil {
			return err
		}
		d.mutex.Lock()
		defer d.mutex.Unlock()
		return d.dt.setCapacity(capacity)
	default: // 000xxxxx: Duplicate (Section 4.3.4)
		relIdx, err := readIntFrom(first, 5, br)
		if err != nil {
			return err
		}
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if err := d.dt.duplicate(relIdx); err != nil {
			return err
		}
		d.cond.Broadcast()
		return nil
	}
}

// applyInsert inserts a new entry into the dynamic table and wakes any blocked
// header-block decodes.
func (d *Decoder) applyInsert(name, value string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if err := d.dt.insert(name, value); err != nil {
		return err
	}
	d.cond.Broadcast()
	return nil
}

// sendInsertCountIncrement emits an Insert Count Increment instruction
// (RFC 9204, Section 4.4.3) acknowledging inserts not yet acknowledged.
func (d *Decoder) sendInsertCountIncrement() {
	d.mutex.Lock()
	delta := d.dt.insertCount - d.ackedInsertCount
	if delta == 0 {
		d.mutex.Unlock()
		return
	}
	d.ackedInsertCount = d.dt.insertCount
	d.mutex.Unlock()

	buf := appendVarInt(nil, 6, delta) // 00xxxxxx
	d.writeDecoderStream(buf)
}

// sendSectionAcknowledgment emits a Section Acknowledgment instruction
// (RFC 9204, Section 4.4.1) for the given stream.
func (d *Decoder) sendSectionAcknowledgment(streamID uint64) {
	buf := appendVarInt(nil, 7, streamID)
	buf[0] |= 0x80 // 1xxxxxxx
	d.writeDecoderStream(buf)
}

// CancelStream emits a Stream Cancellation instruction (RFC 9204, Section 4.4.2)
// for the given stream. It should be called when a request stream that may have
// referenced the dynamic table is abruptly reset. It is a no-op if the decoder
// has no dynamic table or decoder stream.
func (d *Decoder) CancelStream(streamID uint64) {
	if d.dt == nil {
		return
	}
	buf := appendVarInt(nil, 6, streamID)
	buf[0] |= 0x40 // 01xxxxxx
	d.writeDecoderStream(buf)
}

func (d *Decoder) writeDecoderStream(b []byte) {
	if err := d.writeDecoderStreamContext(context.Background(), b); err != nil {
		d.abort(err)
	}
}

// abort records a terminal error on the encoder stream and wakes any blocked
// header-block decodes so they can fail instead of hanging forever.
func (d *Decoder) abort(err error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.closeErr == nil {
		d.closeErr = err
	}
	d.closed = true
	d.cond.Broadcast()
}
