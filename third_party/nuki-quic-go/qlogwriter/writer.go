package qlogwriter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/nukilabs/quic-go/qlogwriter/jsontext"
)

// Trace represents a qlog trace that can have multiple event producers.
// Each producer can record events to the trace independently.
// When the last producer is closed, the underlying trace is closed as well.
type Trace interface {
	// AddProducer creates a new Recorder for this trace.
	// Each Recorder can record events independently.
	AddProducer() Recorder

	// SupportsSchemas returns true if the trace supports the given schema.
	SupportsSchemas(schema string) bool
}

// Recorder is used to record events to a qlog trace.
// It is safe for concurrent use by multiple goroutines.
type Recorder interface {
	// RecordEvent records a single Event to the trace.
	// It must not be called after the recorder has been closed.
	RecordEvent(Event)
	// Close signals that this producer is done recording events.
	// When all producers are closed, the underlying trace is closed.
	// It must not be called concurrently with [Recorder.RecordEvent].
	io.Closer
}

// Event represents a qlog event that can be encoded to JSON.
// Each event must implement [Event.Name] and [Event.Encode].
type Event interface {
	// Name returns the name of the event, as it should appear in the qlog output
	Name() string
	// Encode writes the event's data to the provided jsontext.Encoder
	Encode(encoder *jsontext.Encoder, eventTime time.Time) error
}

// RecordSeparator is the record separator byte for the JSON-SEQ format
const RecordSeparator byte = 0x1e

var recordSeparator = []byte{RecordSeparator}

type event struct {
	Time  time.Time
	Event Event
}

const eventChanSize = 50

// FileSeq represents a qlog trace using the JSON-SEQ format,
// https://www.ietf.org/archive/id/draft-ietf-quic-qlog-main-schema-12.html#section-5
// qlog event producers can be created by calling [FileSeq.AddProducer].
// The underlying io.WriteCloser is closed when the last producer is removed.
type FileSeq struct {
	w             io.WriteCloser
	enc           *jsontext.Encoder
	referenceTime time.Time

	runStopped chan struct{}
	encodeErr  error
	events     chan event
	done       chan struct{}

	mx        sync.Mutex
	producers int
	closed    bool

	closeMu  sync.Mutex
	released bool
	closeErr error

	eventSchemas []string
}

var _ Trace = &FileSeq{}

// NewFileSeq creates a new JSON-SEQ qlog trace to log transport events.
func NewFileSeq(w io.WriteCloser) *FileSeq {
	return newFileSeq(w, "transport", nil, nil)
}

// NewConnectionFileSeq creates a new qlog trace to log connection events.
func NewConnectionFileSeq(w io.WriteCloser, isClient bool, odcid ConnectionID, eventSchemas []string) *FileSeq {
	pers := "server"
	if isClient {
		pers = "client"
	}
	return newFileSeq(w, pers, &odcid, eventSchemas)
}

func newFileSeq(w io.WriteCloser, pers string, odcid *ConnectionID, eventSchemas []string) *FileSeq {
	now := time.Now()
	buf := &bytes.Buffer{}
	enc := jsontext.NewEncoder(buf)
	if _, err := buf.Write(recordSeparator); err != nil {
		panic(fmt.Sprintf("qlog encoding into a bytes.Buffer failed: %s", err))
	}
	if err := (&traceHeader{
		VantagePointType: pers,
		GroupID:          odcid,
		ReferenceTime:    now,
		EventSchemas:     eventSchemas,
	}).Encode(enc); err != nil {
		panic(fmt.Sprintf("qlog encoding into a bytes.Buffer failed: %s", err))
	}
	count, encodeErr := w.Write(buf.Bytes())
	if count != buf.Len() && encodeErr == nil {
		encodeErr = io.ErrShortWrite
	}

	return &FileSeq{
		w:             w,
		referenceTime: now,
		enc:           jsontext.NewEncoder(completeWriter{w}),
		runStopped:    make(chan struct{}),
		encodeErr:     encodeErr,
		events:        make(chan event, eventChanSize),
		done:          make(chan struct{}),
		eventSchemas:  eventSchemas,
	}
}

func (t *FileSeq) SupportsSchemas(schema string) bool {
	return slices.Contains(t.eventSchemas, schema)
}

func (t *FileSeq) AddProducer() Recorder {
	t.mx.Lock()
	defer t.mx.Unlock()
	if t.closed {
		return nil
	}

	t.producers++

	return &Writer{t: t}
}

func (t *FileSeq) record(eventTime time.Time, details Event) {
	t.mx.Lock()

	if t.closed {
		t.mx.Unlock()
		return
	}
	t.mx.Unlock()

	t.events <- event{Time: eventTime, Event: details}
}

func (t *FileSeq) Run() {
	defer close(t.runStopped)

	for {
		select {
		case <-t.done:
			for {
				select {
				case e := <-t.events:
					t.encodeEvent(e)
				default:
					return
				}
			}
		case e := <-t.events:
			t.encodeEvent(e)
		}
	}
}

func (t *FileSeq) encodeEvent(e event) {
	if t.encodeErr != nil {
		return
	}
	count, err := t.w.Write(recordSeparator)
	if count != len(recordSeparator) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		t.encodeErr = err
		return
	}
	h := encoderHelper{enc: t.enc}
	h.WriteToken(jsontext.BeginObject)
	h.WriteToken(jsontext.String("time"))
	h.WriteToken(jsontext.Float(float64(e.Time.Sub(t.referenceTime).Nanoseconds()) / 1e6))
	h.WriteToken(jsontext.String("name"))
	h.WriteToken(jsontext.String(e.Event.Name()))
	h.WriteToken(jsontext.String("data"))
	if err := e.Event.Encode(t.enc, e.Time); err != nil {
		t.encodeErr = errors.Join(h.err, err)
		return
	}
	h.WriteToken(jsontext.EndObject)
	if h.err != nil {
		t.encodeErr = h.err
	}
}

func (t *FileSeq) removeProducer() bool {
	t.mx.Lock()
	t.producers--
	last := t.producers == 0
	if last {
		t.closed = true
	}
	t.mx.Unlock()

	if last {
		close(t.done)
		<-t.runStopped // wait for Run to drain and exit
	}
	return last
}

func (t *FileSeq) closeOutput() error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if !t.released {
		err := t.w.Close()
		t.closeErr = errors.Join(t.closeErr, err)
		t.released = err == nil
	}
	return errors.Join(t.encodeErr, t.closeErr)
}

type Writer struct {
	t      *FileSeq
	mu     sync.Mutex
	closed bool
	last   bool
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		w.last = w.t.removeProducer()
	}
	if w.last {
		return w.t.closeOutput()
	}
	return nil
}

func (w *Writer) RecordEvent(ev Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.t.record(time.Now(), ev)
}

// ReleaseConfirmed separates a joined, closed output from historical encoding
// or Close errors. A non-final producer confirms only its own relinquished use.
func (w *Writer) ReleaseConfirmed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		return false
	}
	if !w.last {
		return true
	}
	w.t.closeMu.Lock()
	defer w.t.closeMu.Unlock()
	return w.t.released
}
