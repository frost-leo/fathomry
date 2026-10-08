package http3

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sardanioss/quic-go"
)

const streamDatagramQueueLen = 32

// stateTrackingStream is an implementation of quic.Stream that delegates
// to an underlying stream
// it takes care of proxying send and receive errors onto an implementation of
// the errorSetter interface (intended to be occupied by a datagrammer)
// it is also responsible for clearing the stream based on its ID from its
// parent connection, this is done through the streamClearer interface when
// both the send and receive sides are closed
type stateTrackingStream struct {
	readEnded     chan struct{}
	readEndedOnce sync.Once
	onReadAbort   func(error)
	readWatchDone <-chan struct{}
	*quic.Stream

	sendDatagram        func([]byte) error
	sendDatagramContext func(context.Context, []byte) error
	hasData             chan struct{}
	queue               [][]byte // TODO: use a ring buffer

	mx                sync.Mutex
	sendErr           error
	recvErr           error
	readAbort         error
	readDeadline      time.Time
	readChanged       chan struct{}
	connectionContext context.Context

	clearer streamClearer
}

var _ datagramStream = &stateTrackingStream{}

type streamClearer interface {
	clearStream(quic.StreamID)
}

func newStateTrackingStream(s *quic.Stream, connectionContext context.Context, clearer streamClearer, sendDatagram func([]byte) error, sendContext func(context.Context, []byte) error) *stateTrackingStream {
	t := &stateTrackingStream{
		readEnded:           make(chan struct{}),
		Stream:              s,
		connectionContext:   connectionContext,
		readChanged:         make(chan struct{}),
		clearer:             clearer,
		sendDatagram:        sendDatagram,
		sendDatagramContext: sendContext,
		hasData:             make(chan struct{}, 1),
	}

	context.AfterFunc(s.Context(), func() {
		t.closeSend(context.Cause(s.Context()))
	})

	return t
}

func (s *stateTrackingStream) closeSend(e error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	// clear the stream the first time both the send
	// and receive are finished
	if s.sendErr == nil {
		if s.recvErr != nil {
			s.clearer.clearStream(s.StreamID())
		}
		s.sendErr = e
	}
}

func (s *stateTrackingStream) closeReceive(e error) {
	s.mx.Lock()
	aborted := s.abortReadLocked(e)
	defer func() {
		s.mx.Unlock()
		if e != nil {
			s.readEndedOnce.Do(func() { close(s.readEnded) })
		}
		if aborted && s.onReadAbort != nil {
			s.onReadAbort(e)
		}
	}()

	// clear the stream the first time both the send
	// and receive are finished
	if s.recvErr == nil {
		if s.sendErr != nil {
			s.clearer.clearStream(s.StreamID())
		}
		s.recvErr = e
		s.signalHasDatagram()
	}
}

func (s *stateTrackingStream) abortReadLocked(err error) bool {
	if err != nil && !errors.Is(err, io.EOF) && s.readAbort == nil {
		s.readAbort = err
		close(s.readChanged)
		return true
	}
	return false
}

func (s *stateTrackingStream) SetReadDeadline(deadline time.Time) error {
	if err := s.Stream.SetReadDeadline(deadline); err != nil {
		return err
	}
	s.mx.Lock()
	s.readDeadline = deadline
	if s.readAbort == nil {
		close(s.readChanged)
		s.readChanged = make(chan struct{})
	}
	s.mx.Unlock()
	return nil
}

func (s *stateTrackingStream) SetDeadline(deadline time.Time) error {
	return errors.Join(s.SetReadDeadline(deadline), s.Stream.SetWriteDeadline(deadline))
}

func (s *stateTrackingStream) waitQPACK(ctx context.Context, inserted <-chan struct{}) error {
	s.mx.Lock()
	err, deadline, changed := s.readAbort, s.readDeadline, s.readChanged
	s.mx.Unlock()
	if err != nil {
		return err
	}
	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-inserted:
		return nil
	case <-changed:
		return nil
	case <-expired:
		return os.ErrDeadlineExceeded
	case <-ctx.Done():
		return errors.Join(ctx.Err(), context.Cause(ctx))
	case <-s.connectionContext.Done():
		return context.Cause(s.connectionContext)
	case <-s.Stream.FathomryReceiveAbort():
		return s.Stream.FathomryReceiveError()
	}
}

func (s *stateTrackingStream) Close() error {
	s.closeSend(errors.New("write on closed stream"))
	return s.Stream.Close()
}

func (s *stateTrackingStream) CancelWrite(e quic.StreamErrorCode) {
	s.closeSend(&quic.StreamError{StreamID: s.StreamID(), ErrorCode: e})
	s.Stream.CancelWrite(e)
}

func (s *stateTrackingStream) Write(b []byte) (int, error) {
	n, err := s.Stream.Write(b)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		s.closeSend(err)
	}
	return n, err
}

func (s *stateTrackingStream) CancelRead(e quic.StreamErrorCode) {
	s.closeReceive(&quic.StreamError{StreamID: s.StreamID(), ErrorCode: e})
	s.Stream.CancelRead(e)
	if s.readWatchDone != nil {
		<-s.readWatchDone
	}
}

func (s *stateTrackingStream) Read(b []byte) (int, error) {
	n, err := s.Stream.Read(b)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		s.closeReceive(err)
	}
	return n, err
}

func (s *stateTrackingStream) SendDatagram(b []byte) error {
	s.mx.Lock()
	sendErr := s.sendErr
	s.mx.Unlock()
	if sendErr != nil {
		return sendErr
	}

	return s.sendDatagram(b)
}

func (s *stateTrackingStream) SendDatagramContext(ctx context.Context, b []byte) error {
	s.mx.Lock()
	err := s.sendErr
	s.mx.Unlock()
	if err != nil {
		return err
	}
	sendContext := s.Stream.Context()
	work, cancel := context.WithCancelCause(ctx)
	joined := make(chan struct{})
	stop := context.AfterFunc(sendContext, func() {
		defer close(joined)
		cancel(context.Cause(sendContext))
	})
	defer func() {
		if !stop() {
			<-joined
		}
		cancel(nil)
	}()
	if cause := context.Cause(sendContext); cause != nil {
		cancel(cause)
	}
	return s.sendDatagramContext(work, b)
}

func (s *stateTrackingStream) signalHasDatagram() {
	select {
	case s.hasData <- struct{}{}:
	default:
	}
}

func (s *stateTrackingStream) enqueueDatagram(data []byte) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.recvErr != nil {
		return
	}
	if len(s.queue) >= streamDatagramQueueLen {
		return
	}
	s.queue = append(s.queue, data)
	s.signalHasDatagram()
}

func (s *stateTrackingStream) ReceiveDatagram(ctx context.Context) ([]byte, error) {
start:
	s.mx.Lock()
	if len(s.queue) > 0 {
		data := s.queue[0]
		s.queue = s.queue[1:]
		s.mx.Unlock()
		return data, nil
	}
	if receiveErr := s.recvErr; receiveErr != nil {
		s.mx.Unlock()
		return nil, receiveErr
	}
	s.mx.Unlock()

	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-s.hasData:
	}
	goto start
}

func (s *stateTrackingStream) QUICStream() *quic.Stream {
	return s.Stream
}
