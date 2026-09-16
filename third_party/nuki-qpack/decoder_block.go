package qpack

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// decodeDynamic returns a DecodeFunc for a header block, using the dynamic
// table. The block is decoded eagerly on the first call (so the dynamic table
// stays consistent for the whole block, and blocked streams wait exactly once),
// and header fields are then yielded one at a time.
func (d *Decoder) decodeDynamic(streamID int64, p []byte) DecodeFunc {
	return d.decodeDynamicContext(context.Background(), streamID, p, 0)
}

func (d *Decoder) decodeDynamicContext(ctx context.Context, streamID int64, p []byte, limit uint64) DecodeFunc {
	var fields []HeaderField
	var decodeErr error
	var started bool
	var idx int
	return func() (HeaderField, error) {
		if !started {
			started = true
			fields, decodeErr = d.decodeBlockContext(ctx, streamID, p, limit)
		}
		if decodeErr != nil {
			return HeaderField{}, decodeErr
		}
		if idx >= len(fields) {
			return HeaderField{}, io.EOF
		}
		hf := fields[idx]
		idx++
		return hf, nil
	}
}

// decodeBlock decodes an entire header block against the dynamic table. It
// blocks until the dynamic table has reached the block's Required Insert Count,
// then emits a Section Acknowledgment (if streamID >= 0 and the block used the
// dynamic table).
func (d *Decoder) decodeBlockContext(ctx context.Context, streamID int64, p []byte, limit uint64) ([]HeaderField, error) {
	woken := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(woken)
		d.mutex.Lock()
		d.cond.Broadcast()
		d.mutex.Unlock()
	})
	defer func() {
		if !stop() {
			<-woken
		}
	}()
	// Header Block Prefix (RFC 9204, Section 4.5.1).
	encodedInsertCount, rest, err := readVarInt(8, p)
	if err != nil {
		return nil, err
	}
	p = rest
	if len(p) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	signBit := p[0]&0x80 != 0
	deltaBase, rest, err := readVarInt(7, p)
	if err != nil {
		return nil, err
	}
	p = rest

	d.mutex.Lock()
	ric, err := decodeRequiredInsertCount(encodedInsertCount, d.dt.maxCapacity/32, d.dt.insertCount)
	if err != nil {
		d.mutex.Unlock()
		return nil, err
	}
	var base uint64
	if !signBit {
		base = ric + deltaBase
	} else {
		if deltaBase+1 > ric {
			d.mutex.Unlock()
			return nil, fmt.Errorf("qpack: invalid Base (Required Insert Count %d, Delta Base %d)", ric, deltaBase)
		}
		base = ric - deltaBase - 1
	}

	// Block this stream until the dynamic table has received enough inserts.
	for d.dt.insertCount < ric && !d.closed && ctx.Err() == nil {
		d.cond.Wait()
	}
	if ctx.Err() != nil {
		d.mutex.Unlock()
		return nil, context.Cause(ctx)
	}
	if d.dt.insertCount < ric {
		err := d.closeErr
		d.mutex.Unlock()
		if err == nil {
			err = errDecoderClosed
		}
		return nil, err
	}

	fields, err := d.decodeFieldLines(p, base, limit)
	d.mutex.Unlock()
	if err != nil {
		return nil, err
	}

	if streamID >= 0 && ric > 0 {
		buf := appendVarInt(nil, 7, uint64(streamID))
		buf[0] |= 0x80
		if err := d.writeDecoderStreamContext(ctx, buf); err != nil {
			return nil, err
		}
	}
	return fields, nil
}

// decodeFieldLines decodes all field line representations in p against the
// static and dynamic tables, using the given Base. It must be called with
// d.mutex held.
func (d *Decoder) decodeFieldLines(p []byte, base uint64, limit uint64) ([]HeaderField, error) {
	var fields []HeaderField
	var size uint64
	for len(p) > 0 {
		b := p[0]
		var hf HeaderField
		var rest []byte
		var err error
		switch {
		case b&0x80 != 0: // 1Txxxxxx: Indexed Field Line (Section 4.5.2)
			hf, rest, err = d.decodeIndexedLine(p, base)
		case b&0x40 != 0: // 01NTxxxx: Literal Field Line with Name Reference (Section 4.5.4)
			hf, rest, err = d.decodeLiteralWithNameRef(p, base)
		case b&0x20 != 0: // 001NHxxx: Literal Field Line with Literal Name (Section 4.5.6)
			hf, rest, err = d.parseLiteralHeaderFieldWithoutNameReference(p)
		case b&0x10 != 0: // 0001xxxx: Indexed Field Line with Post-Base Index (Section 4.5.3)
			hf, rest, err = d.decodePostBaseIndexedLine(p, base)
		default: // 0000Nxxx: Literal Field Line with Post-Base Name Reference (Section 4.5.5)
			hf, rest, err = d.decodePostBaseLiteralWithNameRef(p, base)
		}
		if err != nil {
			return nil, err
		}
		p = rest
		fieldSize := uint64(len(hf.Name)) + uint64(len(hf.Value)) + 32
		if limit > 0 && (size > limit || fieldSize > limit-size) {
			return nil, ErrHeaderLimit
		}
		size += fieldSize
		fields = append(fields, hf)
	}
	return fields, nil
}

// dynamicAt resolves an absolute dynamic table index to a header field.
// Must be called with d.mutex held.
func (d *Decoder) dynamicAt(absoluteIndex uint64) (HeaderField, bool) {
	e, ok := d.dt.at(absoluteIndex)
	if !ok {
		return HeaderField{}, false
	}
	return HeaderField{Name: e.name, Value: e.value}, true
}

func (d *Decoder) decodeIndexedLine(buf []byte, base uint64) (HeaderField, []byte, error) {
	static := buf[0]&0x40 != 0
	index, rest, err := readVarInt(6, buf)
	if err != nil {
		return HeaderField{}, buf, err
	}
	if static {
		hf, ok := d.at(index)
		if !ok {
			return HeaderField{}, buf, invalidIndexError(index)
		}
		return hf, rest, nil
	}
	if index+1 > base {
		return HeaderField{}, buf, fmt.Errorf("qpack: indexed field line references invalid relative index %d (Base %d)", index, base)
	}
	hf, ok := d.dynamicAt(base - index - 1)
	if !ok {
		return HeaderField{}, buf, invalidIndexError(index)
	}
	return hf, rest, nil
}

func (d *Decoder) decodePostBaseIndexedLine(buf []byte, base uint64) (HeaderField, []byte, error) {
	index, rest, err := readVarInt(4, buf)
	if err != nil {
		return HeaderField{}, buf, err
	}
	hf, ok := d.dynamicAt(base + index)
	if !ok {
		return HeaderField{}, buf, invalidIndexError(index)
	}
	return hf, rest, nil
}

func (d *Decoder) decodeLiteralWithNameRef(buf []byte, base uint64) (HeaderField, []byte, error) {
	static := buf[0]&0x10 != 0
	index, rest, err := readVarInt(4, buf)
	if err != nil {
		return HeaderField{}, buf, err
	}
	var hf HeaderField
	if static {
		var ok bool
		hf, ok = d.at(index)
		if !ok {
			return HeaderField{}, buf, invalidIndexError(index)
		}
	} else {
		if index+1 > base {
			return HeaderField{}, buf, fmt.Errorf("qpack: literal field line references invalid relative index %d (Base %d)", index, base)
		}
		var ok bool
		hf, ok = d.dynamicAt(base - index - 1)
		if !ok {
			return HeaderField{}, buf, invalidIndexError(index)
		}
	}
	buf = rest
	if len(buf) == 0 {
		return HeaderField{}, buf, io.ErrUnexpectedEOF
	}
	usesHuffman := buf[0]&0x80 > 0
	val, rest, err := d.readString(buf, 7, usesHuffman)
	if err != nil {
		return HeaderField{}, rest, err
	}
	hf.Value = val
	return hf, rest, nil
}

func (d *Decoder) decodePostBaseLiteralWithNameRef(buf []byte, base uint64) (HeaderField, []byte, error) {
	index, rest, err := readVarInt(3, buf)
	if err != nil {
		return HeaderField{}, buf, err
	}
	hf, ok := d.dynamicAt(base + index)
	if !ok {
		return HeaderField{}, buf, invalidIndexError(index)
	}
	buf = rest
	if len(buf) == 0 {
		return HeaderField{}, buf, io.ErrUnexpectedEOF
	}
	usesHuffman := buf[0]&0x80 > 0
	val, rest, err := d.readString(buf, 7, usesHuffman)
	if err != nil {
		return HeaderField{}, rest, err
	}
	hf.Value = val
	return hf, rest, nil
}

// decodeRequiredInsertCount reconstructs the Required Insert Count from its
// encoded form (RFC 9204, Section 4.5.1.1).
func decodeRequiredInsertCount(encoded, maxEntries, totalInserts uint64) (uint64, error) {
	if encoded == 0 {
		return 0, nil
	}
	if maxEntries == 0 {
		return 0, errors.New("qpack: dynamic table not available")
	}
	fullRange := 2 * maxEntries
	if encoded > fullRange {
		return 0, fmt.Errorf("qpack: invalid Required Insert Count encoding %d", encoded)
	}
	maxValue := totalInserts + maxEntries
	maxWrapped := (maxValue / fullRange) * fullRange
	ric := maxWrapped + encoded - 1
	if ric > maxValue {
		if ric <= fullRange {
			return 0, fmt.Errorf("qpack: invalid Required Insert Count %d", ric)
		}
		ric -= fullRange
	}
	if ric == 0 {
		return 0, errors.New("qpack: Required Insert Count must not be zero")
	}
	return ric, nil
}
