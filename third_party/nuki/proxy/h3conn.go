package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/http3"
	"github.com/nukilabs/quic-go/quicvarint"
)

type masqueAddr struct{ net.Addr }

const (
	maxTunnelPayload    = 65507
	tunnelDatagramSlots = 32
	// FathomryTunnelIngressBytes declares owned ingress queue, parser scratch and
	// receive-worker storage, separately from the native HTTP/3 datagram queue.
	FathomryTunnelIngressBytes = (tunnelDatagramSlots+2)*(64<<10) + 2*(8<<10)
)

func (m masqueAddr) Network() string { return "connect-udp" }

type http3Stream interface {
	io.ReadWriteCloser
	ReceiveDatagram(context.Context) ([]byte, error)
	SendDatagramContext(context.Context, []byte) error
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
}

type tunnelDeadline struct {
	context   context.Context
	cancel    context.CancelCauseFunc
	timer     *time.Timer
	timerDone chan struct{}
}

var errDeadlineChanged = errors.New("proxy: deadline changed")

func newTunnelDeadline() tunnelDeadline {
	ctx, cancel := context.WithCancelCause(context.Background())
	return tunnelDeadline{context: ctx, cancel: cancel}
}

func (deadline *tunnelDeadline) stop(cause error) {
	timer, done := deadline.timer, deadline.timerDone
	deadline.timer, deadline.timerDone = nil, nil
	if timer != nil && !timer.Stop() {
		<-done
	}
	deadline.cancel(cause)
}

type h3Conn struct {
	str                   http3Stream
	localAddr, remoteAddr net.Addr
	mu                    sync.Mutex
	closed                bool
	read, write           tunnelDeadline
	work                  sync.WaitGroup
	readDone, closeDone   chan struct{}
	datagramDone          chan struct{}
	datagrams             chan []byte
	receiveContext        context.Context
	receiveCancel         context.CancelCauseFunc
	terminalErr           error
	closeOnce             sync.Once
	closeErr              error
	release               func()
}

var _ net.PacketConn = (*h3Conn)(nil)

func newH3Conn(str http3Stream, local, remote net.Addr) *h3Conn {
	c := &h3Conn{str: str, localAddr: local, remoteAddr: remote, read: newTunnelDeadline(), write: newTunnelDeadline(),
		readDone: make(chan struct{}), closeDone: make(chan struct{}), datagramDone: make(chan struct{}), datagrams: make(chan []byte, tunnelDatagramSlots)}
	c.receiveContext, c.receiveCancel = context.WithCancelCause(context.Background())
	go func() {
		defer close(c.readDone)
		c.fail(c.readCapsules())
	}()
	go func() { defer close(c.datagramDone); c.receiveDatagrams() }()
	return c
}

func (c *h3Conn) fail(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	if c.closed || c.terminalErr != nil {
		c.mu.Unlock()
		return
	}
	c.terminalErr = err
	c.read.stop(err)
	c.write.stop(err)
	c.receiveCancel(err)
	c.mu.Unlock()
	c.str.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
	c.str.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
}

func (c *h3Conn) enqueue(data []byte) {
	select {
	case c.datagrams <- data:
	default:
	}
}

func (c *h3Conn) receiveDatagrams() {
	for {
		data, err := c.str.ReceiveDatagram(c.receiveContext)
		if err != nil {
			// CapsuleParser, not the concurrent datagram waiter, determines
			// whether the stream's EOF ended a complete or truncated capsule.
			return
		}
		id, count, err := quicvarint.Parse(data)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			c.fail(fmt.Errorf("masque: malformed datagram: %w", err))
			return
		}
		if id != 0 {
			continue
		}
		if len(data)-count > maxTunnelPayload {
			c.fail(&quic.DatagramTooLargeError{MaxDatagramPayloadSize: maxTunnelPayload})
			return
		}
		c.enqueue(bytes.Clone(data[count:]))
	}
}

func (c *h3Conn) enter() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	if c.terminalErr != nil {
		return c.terminalErr
	}
	c.work.Add(1)
	return nil
}
func (c *h3Conn) context(read bool) context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if read {
		return c.read.context
	}
	return c.write.context
}
func (c *h3Conn) retry(ctx context.Context, read bool, err error) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false, net.ErrClosed
	}
	if c.terminalErr != nil {
		return false, c.terminalErr
	}
	current := c.write.context
	if read {
		current = c.read.context
	}
	if current != ctx {
		return true, nil
	}
	if ctx.Err() != nil {
		return false, context.Cause(ctx)
	}
	return false, err
}

func (c *h3Conn) ReadFrom(data []byte) (int, net.Addr, error) {
	if err := c.enter(); err != nil {
		return 0, nil, err
	}
	defer c.work.Done()
	for {
		ctx := c.context(true)
		var payload []byte
		err := ctx.Err()
		if err == nil {
			select {
			case payload = <-c.datagrams:
			case <-ctx.Done():
				err = context.Cause(ctx)
			}
		}
		if err != nil {
			retry, err := c.retry(ctx, true, err)
			if retry {
				continue
			}
			return 0, nil, err
		}
		return copy(data, payload), c.remoteAddr, nil
	}
}
func (c *h3Conn) WriteTo(data []byte, address net.Addr) (int, error) {
	if err := c.enter(); err != nil {
		return 0, err
	}
	defer c.work.Done()
	if len(data) > maxTunnelPayload {
		return 0, &quic.DatagramTooLargeError{MaxDatagramPayloadSize: maxTunnelPayload}
	}
	if address != nil && address.String() != c.remoteAddr.String() {
		return 0, errors.New("masque: datagram target differs from tunnel")
	}
	payload := make([]byte, 1+len(data))
	copy(payload[1:], data)
	for {
		ctx := c.context(false)
		err := ctx.Err()
		if err == nil {
			err = c.str.SendDatagramContext(ctx, payload)
		}
		if err == nil {
			return len(data), nil
		}
		retry, err := c.retry(ctx, false, err)
		if retry {
			continue
		}
		return 0, err
	}
}
func (c *h3Conn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.read.stop(net.ErrClosed)
		c.write.stop(net.ErrClosed)
		c.receiveCancel(net.ErrClosed)
		terminated := c.terminalErr != nil
		c.mu.Unlock()
		c.str.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		if !terminated {
			c.closeErr = c.str.Close()
		}
		c.str.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		<-c.readDone
		<-c.datagramDone
		c.work.Wait()
		for len(c.datagrams) > 0 {
			<-c.datagrams
		}
		if c.release != nil {
			c.release()
		}
		close(c.closeDone)
	})
	<-c.closeDone
	return c.closeErr
}
func (c *h3Conn) ReleaseConfirmed() bool {
	select {
	case <-c.closeDone:
		return true
	default:
		return false
	}
}
func (c *h3Conn) LocalAddr() net.Addr { return &masqueAddr{c.localAddr} }
func (c *h3Conn) SetDeadline(at time.Time) error {
	if err := c.SetReadDeadline(at); err != nil {
		return err
	}
	return c.SetWriteDeadline(at)
}
func (c *h3Conn) setDeadline(at time.Time, read bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	if c.terminalErr != nil {
		return c.terminalErr
	}
	deadline := &c.write
	if read {
		deadline = &c.read
	}
	deadline.stop(errDeadlineChanged)
	*deadline = newTunnelDeadline()
	if !at.IsZero() {
		if !at.After(time.Now()) {
			deadline.cancel(os.ErrDeadlineExceeded)
			return nil
		}
		cancel := deadline.cancel
		done := make(chan struct{})
		deadline.timerDone = done
		deadline.timer = time.AfterFunc(time.Until(at), func() { defer close(done); cancel(os.ErrDeadlineExceeded) })
	}
	return nil
}
func (c *h3Conn) SetReadDeadline(at time.Time) error  { return c.setDeadline(at, true) }
func (c *h3Conn) SetWriteDeadline(at time.Time) error { return c.setDeadline(at, false) }
func (c *h3Conn) SetReadBuffer(int) error             { return nil }
func (c *h3Conn) SetWriteBuffer(int) error            { return nil }

func (c *h3Conn) readCapsules() error {
	parser := http3.NewCapsuleParser(c.str)
	for {
		kind, value, err := parser.Next()
		if err != nil {
			return err
		}
		if kind != 0 {
			if err := value.Discard(); err != nil {
				return err
			}
			continue
		}
		id, err := quicvarint.Read(value)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("masque: malformed datagram capsule: %w", err)
		}
		if id != 0 {
			if err := value.Discard(); err != nil {
				return err
			}
			continue
		}
		if value.Remaining() > maxTunnelPayload {
			return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: maxTunnelPayload}
		}
		data := make([]byte, int(value.Remaining()))
		if _, err := io.ReadFull(value, data); err != nil {
			return err
		}
		c.enqueue(data)
	}
}
