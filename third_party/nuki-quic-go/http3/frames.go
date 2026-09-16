package http3

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"

	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/http3/qlog"
	"github.com/nukilabs/quic-go/qlogwriter"
	"github.com/nukilabs/quic-go/quicvarint"
)

// FrameType is the frame type of a HTTP/3 frame
type FrameType uint64

type frame any

var errPriorityUpdateForPush = errors.New("http3: PRIORITY_UPDATE frame for push")

// The maximum length of an encoded HTTP/3 frame header is 16:
// The frame has a type and length field, both QUIC varints (maximum 8 bytes in length)
const frameHeaderLen = 16

type countingByteReader struct {
	quicvarint.Reader
	NumRead int
}

func (r *countingByteReader) ReadByte() (byte, error) {
	b, err := r.Reader.ReadByte()
	if err == nil {
		r.NumRead++
	}
	return b, err
}

func (r *countingByteReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	r.NumRead += n
	return n, err
}

func (r *countingByteReader) Reset() {
	r.NumRead = 0
}

type frameParser struct {
	r         io.Reader
	streamID  quic.StreamID
	closeConn func(quic.ApplicationErrorCode, string) error
}

func (p *frameParser) ParseNext(qlogger qlogwriter.Recorder) (frame, error) {
	r := &countingByteReader{Reader: quicvarint.NewReader(p.r)}
	for {
		t, err := quicvarint.Read(r)
		if err != nil {
			return nil, err
		}
		l, err := quicvarint.Read(r)
		if err != nil {
			return nil, err
		}

		switch t {
		case 0x0: // DATA
			if qlogger != nil {
				qlogger.RecordEvent(qlog.FrameParsed{
					StreamID: p.streamID,
					Raw: qlog.RawInfo{
						Length:        int(l) + r.NumRead,
						PayloadLength: int(l),
					},
					Frame: qlog.Frame{Frame: qlog.DataFrame{}},
				})
			}
			return &dataFrame{Length: l}, nil
		case 0x1: // HEADERS
			return &headersFrame{
				Length:    l,
				headerLen: r.NumRead,
			}, nil
		case 0x4: // SETTINGS
			return parseSettingsFrame(r, l, p.streamID, qlogger)
		case 0x3: // unsupported: CANCEL_PUSH
			if qlogger != nil {
				qlogger.RecordEvent(qlog.FrameParsed{
					StreamID: p.streamID,
					Raw:      qlog.RawInfo{Length: r.NumRead, PayloadLength: int(l)},
					Frame:    qlog.Frame{Frame: qlog.CancelPushFrame{}},
				})
			}
		case 0x5: // unsupported: PUSH_PROMISE
			if qlogger != nil {
				qlogger.RecordEvent(qlog.FrameParsed{
					StreamID: p.streamID,
					Raw:      qlog.RawInfo{Length: r.NumRead, PayloadLength: int(l)},
					Frame:    qlog.Frame{Frame: qlog.PushPromiseFrame{}},
				})
			}
		case 0x7: // GOAWAY
			return parseGoAwayFrame(r, l, p.streamID, qlogger)
		case 0xf0700: // PRIORITY_UPDATE for a request stream
			return parsePriorityUpdateFrame(r, l, p.streamID, qlogger)
		case 0xf0701: // PRIORITY_UPDATE for a push stream
			return nil, errPriorityUpdateForPush
		case 0xd: // unsupported: MAX_PUSH_ID
			if qlogger != nil {
				qlogger.RecordEvent(qlog.FrameParsed{
					StreamID: p.streamID,
					Raw:      qlog.RawInfo{Length: r.NumRead, PayloadLength: int(l)},
					Frame:    qlog.Frame{Frame: qlog.MaxPushIDFrame{}},
				})
			}
		case 0x2, 0x6, 0x8, 0x9: // reserved frame types
			if qlogger != nil {
				qlogger.RecordEvent(qlog.FrameParsed{
					StreamID: p.streamID,
					Raw:      qlog.RawInfo{Length: r.NumRead + int(l), PayloadLength: int(l)},
					Frame:    qlog.Frame{Frame: qlog.ReservedFrame{Type: t}},
				})
			}
			p.closeConn(quic.ApplicationErrorCode(ErrCodeFrameUnexpected), "")
			return nil, fmt.Errorf("http3: reserved frame type: %d", t)
		default:
			// unknown frame types
			if qlogger != nil {
				qlogger.RecordEvent(qlog.FrameParsed{
					StreamID: p.streamID,
					Raw:      qlog.RawInfo{Length: r.NumRead, PayloadLength: int(l)},
					Frame:    qlog.Frame{Frame: qlog.UnknownFrame{Type: t}},
				})
			}
		}

		// skip over the payload
		if _, err := io.CopyN(io.Discard, r, int64(l)); err != nil {
			return nil, err
		}
		r.Reset()
	}
}

type dataFrame struct {
	Length uint64
}

func (f *dataFrame) Append(b []byte) []byte {
	b = quicvarint.Append(b, 0x0)
	return quicvarint.Append(b, f.Length)
}

type headersFrame struct {
	Length    uint64
	headerLen int // number of bytes read for type and length field
}

func (f *headersFrame) Append(b []byte) []byte {
	b = quicvarint.Append(b, 0x1)
	return quicvarint.Append(b, f.Length)
}

const (
	// SETTINGS_MAX_FIELD_SECTION_SIZE
	settingMaxFieldSectionSize = 0x6
	// Extended CONNECT, RFC 9220
	settingExtendedConnect = 0x8
	// HTTP Datagrams, RFC 9297
	settingDatagram = 0x33
)

// Exported setting identifiers, used by callers that want full control over the
// SETTINGS frame (contents and order) for fingerprinting purposes.
const (
	// QPACK maximum table capacity, RFC 9204
	SettingQpackMaxTableCapacity = 0x1
	// Maximum field section size, RFC 9114
	SettingMaxFieldSectionSize = 0x6
	// QPACK blocked streams, RFC 9204
	SettingQpackBlockedStreams = 0x7
	// Extended CONNECT, RFC 9220
	SettingExtendedConnect = 0x8
	// HTTP Datagrams, RFC 9297
	SettingH3Datagram = 0x33
	// Enable Metadata, draft-beky-httpbis-metadata-02
	SettingEnableMetadata = 0x4d44
	// GREASE identifier, draft-edm-protocol-greasing-05
	SettingGrease = 0x1f*1 + 0x21
)

type Setting struct {
	ID  uint64
	Val uint64
}

type settingsFrame struct {
	MaxFieldSectionSize int64 // SETTINGS_MAX_FIELD_SECTION_SIZE, -1 if not set

	Datagram        bool              // HTTP Datagrams, RFC 9297
	ExtendedConnect bool              // Extended CONNECT, RFC 9220
	Other           map[uint64]uint64 // all settings that we don't explicitly recognize
	Order           []uint64          // order in which the settings in Other are sent; if nil, map order is used
}

const maxSettingsFrameSize = 8 << 10

func parseSettingsFrame(r *countingByteReader, l uint64, streamID quic.StreamID, qlogger qlogwriter.Recorder) (*settingsFrame, error) {
	if l > maxSettingsFrameSize {
		return nil, fmt.Errorf("unexpected size for SETTINGS frame: %d", l)
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	frame := &settingsFrame{MaxFieldSectionSize: -1}
	b := bytes.NewReader(buf)
	settingsFrame := qlog.SettingsFrame{MaxFieldSectionSize: -1}
	var readMaxFieldSectionSize, readDatagram, readExtendedConnect bool
	for b.Len() > 0 {
		id, err := quicvarint.Read(b)
		if err != nil { // should not happen. We allocated the whole frame already.
			return nil, err
		}
		val, err := quicvarint.Read(b)
		if err != nil { // should not happen. We allocated the whole frame already.
			return nil, err
		}

		switch id {
		case settingMaxFieldSectionSize:
			if readMaxFieldSectionSize {
				return nil, fmt.Errorf("duplicate setting: %d", id)
			}
			readMaxFieldSectionSize = true
			frame.MaxFieldSectionSize = int64(val)
			settingsFrame.MaxFieldSectionSize = int64(val)
		case settingExtendedConnect:
			if readExtendedConnect {
				return nil, fmt.Errorf("duplicate setting: %d", id)
			}
			readExtendedConnect = true
			if val != 0 && val != 1 {
				return nil, fmt.Errorf("invalid value for SETTINGS_ENABLE_CONNECT_PROTOCOL: %d", val)
			}
			frame.ExtendedConnect = val == 1
			if qlogger != nil {
				settingsFrame.ExtendedConnect = new(frame.ExtendedConnect)
			}
		case settingDatagram:
			if readDatagram {
				return nil, fmt.Errorf("duplicate setting: %d", id)
			}
			readDatagram = true
			if val != 0 && val != 1 {
				return nil, fmt.Errorf("invalid value for SETTINGS_H3_DATAGRAM: %d", val)
			}
			frame.Datagram = val == 1
			if qlogger != nil {
				settingsFrame.Datagram = new(frame.Datagram)
			}
		default:
			if _, ok := frame.Other[id]; ok {
				return nil, fmt.Errorf("duplicate setting: %d", id)
			}
			if frame.Other == nil {
				frame.Other = make(map[uint64]uint64)
			}
			frame.Other[id] = val
		}
	}
	if qlogger != nil {
		settingsFrame.Other = maps.Clone(frame.Other)

		qlogger.RecordEvent(qlog.FrameParsed{
			StreamID: streamID,
			Raw: qlog.RawInfo{
				Length:        r.NumRead,
				PayloadLength: int(l),
			},
			Frame: qlog.Frame{Frame: settingsFrame},
		})
	}
	return frame, nil
}

// greaseValues returns the (id, value) pair to send for a GREASE setting.
// The id is randomized per RFC-style 0x1f*N+0x21; the value defaults to a
// derived pseudo-random value unless the caller specified a non-zero one.
func greaseValues(val uint64) (uint64, uint64) {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<30))
	if err != nil {
		n = big.NewInt(1)
	}
	id := 0x1f*n.Uint64() + 0x21
	if val == 0 {
		return id, id % (1 << 30)
	}
	return id, val
}

func (f *settingsFrame) Append(b []byte) []byte {
	// When an explicit order is requested, emit exactly the settings listed in
	// Order (used for fingerprinting); MaxFieldSectionSize is not auto-added.
	if f.Order != nil {
		return f.appendInOrder(b)
	}

	b = quicvarint.Append(b, 0x4)
	var l int
	var datagramAdded, extendedConnectAdded bool
	var greaseID, greaseVal uint64
	if f.MaxFieldSectionSize >= 0 {
		l += quicvarint.Len(settingMaxFieldSectionSize) + quicvarint.Len(uint64(f.MaxFieldSectionSize))
	}
	for id, val := range f.Other {
		if id == SettingGrease {
			greaseID, greaseVal = greaseValues(val)
			l += quicvarint.Len(greaseID) + quicvarint.Len(greaseVal)
		} else {
			l += quicvarint.Len(id) + quicvarint.Len(val)
		}
		if id == SettingH3Datagram {
			datagramAdded = true
		}
		if id == SettingExtendedConnect {
			extendedConnectAdded = true
		}
	}
	if f.Datagram && !datagramAdded {
		l += quicvarint.Len(settingDatagram) + quicvarint.Len(1)
	}
	if f.ExtendedConnect && !extendedConnectAdded {
		l += quicvarint.Len(settingExtendedConnect) + quicvarint.Len(1)
	}
	b = quicvarint.Append(b, uint64(l))
	if f.MaxFieldSectionSize >= 0 {
		b = quicvarint.Append(b, settingMaxFieldSectionSize)
		b = quicvarint.Append(b, uint64(f.MaxFieldSectionSize))
	}
	for id, val := range f.Other {
		if id == SettingGrease {
			b = quicvarint.Append(b, greaseID)
			b = quicvarint.Append(b, greaseVal)
		} else {
			b = quicvarint.Append(b, id)
			b = quicvarint.Append(b, val)
		}
	}
	if f.Datagram && !datagramAdded {
		b = quicvarint.Append(b, settingDatagram)
		b = quicvarint.Append(b, 1)
	}
	if f.ExtendedConnect && !extendedConnectAdded {
		b = quicvarint.Append(b, settingExtendedConnect)
		b = quicvarint.Append(b, 1)
	}
	return b
}

// appendInOrder emits the settings listed in f.Order, in that exact order,
// followed by Datagram/ExtendedConnect if enabled but not already listed.
// It is used to reproduce a specific SETTINGS frame layout for fingerprinting.
func (f *settingsFrame) appendInOrder(b []byte) []byte {
	b = quicvarint.Append(b, 0x4)
	var l int
	var datagramAdded, extendedConnectAdded bool
	greaseIDs := make(map[int]uint64)
	greaseVals := make(map[int]uint64)
	for i, id := range f.Order {
		val, exists := f.Other[id]
		if !exists {
			continue
		}
		if id == SettingGrease {
			greaseIDs[i], greaseVals[i] = greaseValues(val)
			l += quicvarint.Len(greaseIDs[i]) + quicvarint.Len(greaseVals[i])
		} else {
			l += quicvarint.Len(id) + quicvarint.Len(val)
		}
		if id == SettingH3Datagram {
			datagramAdded = true
		}
		if id == SettingExtendedConnect {
			extendedConnectAdded = true
		}
	}
	if f.Datagram && !datagramAdded {
		l += quicvarint.Len(SettingH3Datagram) + quicvarint.Len(1)
	}
	if f.ExtendedConnect && !extendedConnectAdded {
		l += quicvarint.Len(SettingExtendedConnect) + quicvarint.Len(1)
	}
	b = quicvarint.Append(b, uint64(l))
	for i, id := range f.Order {
		val, exists := f.Other[id]
		if !exists {
			continue
		}
		if id == SettingGrease {
			b = quicvarint.Append(b, greaseIDs[i])
			b = quicvarint.Append(b, greaseVals[i])
		} else {
			b = quicvarint.Append(b, id)
			b = quicvarint.Append(b, val)
		}
	}
	if f.Datagram && !datagramAdded {
		b = quicvarint.Append(b, SettingH3Datagram)
		b = quicvarint.Append(b, 1)
	}
	if f.ExtendedConnect && !extendedConnectAdded {
		b = quicvarint.Append(b, SettingExtendedConnect)
		b = quicvarint.Append(b, 1)
	}
	return b
}

type goAwayFrame struct {
	StreamID quic.StreamID
}

func parseGoAwayFrame(r *countingByteReader, l uint64, streamID quic.StreamID, qlogger qlogwriter.Recorder) (*goAwayFrame, error) {
	frame := &goAwayFrame{}
	startLen := r.NumRead
	id, err := quicvarint.Read(r)
	if err != nil {
		return nil, err
	}
	if r.NumRead-startLen != int(l) {
		return nil, errors.New("GOAWAY frame: inconsistent length")
	}
	frame.StreamID = quic.StreamID(id)
	if qlogger != nil {
		qlogger.RecordEvent(qlog.FrameParsed{
			StreamID: streamID,
			Raw:      qlog.RawInfo{Length: r.NumRead, PayloadLength: int(l)},
			Frame:    qlog.Frame{Frame: qlog.GoAwayFrame{StreamID: frame.StreamID}},
		})
	}
	return frame, nil
}

func (f *goAwayFrame) Append(b []byte) []byte {
	b = quicvarint.Append(b, 0x7)
	b = quicvarint.Append(b, uint64(quicvarint.Len(uint64(f.StreamID))))
	return quicvarint.Append(b, uint64(f.StreamID))
}

// PRIORITY_UPDATE, RFC 9218
type priorityUpdateFrame struct {
	ElementID          uint64
	PriorityFieldValue string
}

const maxPriorityUpdateFrameSize = 4 << 10

func parsePriorityUpdateFrame(r *countingByteReader, l uint64, streamID quic.StreamID, qlogger qlogwriter.Recorder) (*priorityUpdateFrame, error) {
	if l > maxPriorityUpdateFrameSize {
		return nil, fmt.Errorf("unexpected size for PRIORITY_UPDATE frame: %d", l)
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	id, n, err := quicvarint.Parse(buf)
	if err != nil {
		return nil, err
	}
	frame := &priorityUpdateFrame{
		ElementID:          id,
		PriorityFieldValue: string(buf[n:]),
	}
	if qlogger != nil {
		qlogger.RecordEvent(qlog.FrameParsed{
			StreamID: streamID,
			Raw:      qlog.RawInfo{Length: r.NumRead, PayloadLength: int(l)},
			Frame: qlog.Frame{Frame: qlog.PriorityUpdateFrame{
				StreamID:           quic.StreamID(frame.ElementID),
				PriorityFieldValue: frame.PriorityFieldValue,
			}},
		})
	}
	return frame, nil
}
