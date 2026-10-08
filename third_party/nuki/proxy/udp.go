package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dunglas/httpsfv"
	"github.com/nukilabs/http"
	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/http3"
)

const (
	uriTemplateTargetHost = "target_host"
	uriTemplateTargetPort = "target_port"
)

func (d *Dialer) ListenPacket(ctx context.Context, network, addr string) (result net.PacketConn, resultErr error) {
	release := func() {}
	if d.acquireTunnel != nil {
		var err error
		release, err = d.acquireTunnel()
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if result == nil {
			release()
		}
	}()
	setup, cancel := context.WithCancel(ctx)
	if d.timeout > 0 {
		cancel()
		setup, cancel = context.WithTimeout(ctx, d.timeout)
	}
	defer cancel()
	ctx = setup
	d.createMu.Lock()
	defer d.createMu.Unlock()
	if d.closed {
		return nil, net.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	proxy, dst := opAddr(d.proxyURL.Host), opAddr(addr)

	u, err := d.expandTemplate(addr)
	if err != nil {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: err}
	}
	if u.Scheme != "https" || u.Host != d.proxyURL.Host || u.Fragment != "" {
		return nil, errors.New("invalid CONNECT-UDP template authority")
	}
	target, err := d.resolveProxy(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	d.h3DialLock.Lock()
	if d.h3Conn == nil || d.h3Conn.Context().Err() != nil {
		var cleanup error
		if d.h3ClientConn != nil {
			cleanup = errors.Join(cleanup, d.h3ClientConn.CloseWithError(0, ""))
		}
		if d.h3Transport != nil {
			cleanup = errors.Join(cleanup, d.h3Transport.Close())
		}
		if d.h3Socket != nil {
			cleanup = errors.Join(cleanup, d.h3Socket.Close())
		}
		if !closedOnly(cleanup) {
			d.cleanup = errors.Join(d.cleanup, cleanup)
			d.h3DialLock.Unlock()
			return nil, cleanup
		}
		tlsConf := d.tlsConf.Clone()
		if tlsConf.ServerName == "" {
			tlsConf.ServerName = d.proxyURL.Hostname()
		}
		tlsConf.NextProtos = []string{http3.NextProtoH3}
		remote, err := d.resolveProxy(ctx, network, u.Host)
		if err != nil {
			d.h3DialLock.Unlock()
			return nil, err
		}
		socket, err := d.base.ListenPacket(ctx, network, remote.String())
		if err != nil {
			d.h3DialLock.Unlock()
			return nil, err
		}
		transport := &quic.Transport{Conn: socket}
		conn, err := transport.Dial(ctx, remote, tlsConf, &quic.Config{
			EnableDatagrams:      true,
			InitialPacketSize:    1350,
			HandshakeIdleTimeout: d.timeout,
		})
		if err != nil {
			err = errors.Join(err, transport.Close(), socket.Close())
			d.h3DialLock.Unlock()
			return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("dialing quic connection failed: %w", err)}
		}
		d.h3Conn = conn
		d.h3Transport, d.h3Socket = transport, socket
		tr := &http3.Transport{EnableDatagrams: true, MaxResponseHeaderBytes: int(d.maxHeader)}
		d.h3ClientConn = tr.NewClientConn(conn)
	}
	h3Conn, clientConn := d.h3Conn, d.h3ClientConn
	d.h3DialLock.Unlock()

	var timeoutCh <-chan time.Time
	if d.timeout > 0 {
		timer := time.NewTimer(d.timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: context.Cause(ctx)}
	case <-timeoutCh:
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: os.ErrDeadlineExceeded}
	case <-clientConn.Context().Done():
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: context.Cause(clientConn.Context())}
	case <-clientConn.ReceivedSettings():
	}
	settings := clientConn.Settings()
	if !settings.EnableExtendedConnect {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: errors.New("server didn't enable extended connect")}
	}
	if !settings.EnableDatagrams {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: errors.New("server didn't enable datagrams")}
	}

	rstr, err := clientConn.OpenRequestStream(ctx)
	if err != nil {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("failed to open request stream: %w", err)}
	}
	transferred := false
	defer func() {
		if !transferred {
			rstr.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
			rstr.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
			_ = rstr.Close()
		}
	}()
	if deadline, ok := d.deadline(ctx); ok {
		if err := rstr.SetDeadline(deadline); err != nil {
			return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("setting stream deadline failed: %w", err)}
		}
	}
	finishSetup := watchSetup(ctx, 0, func() error {
		rstr.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		rstr.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		return nil
	})
	setupFinished := false
	defer func() {
		if !setupFinished {
			resultErr = errors.Join(resultErr, finishSetup())
		}
	}()
	hdr := http.Header{
		http3.CapsuleProtocolHeader: {"?1"},
	}
	if d.authHeader != "" {
		hdr.Set("Proxy-Authorization", d.authHeader)
	}
	for key, values := range d.headers {
		hdr[key] = append([]string(nil), values...)
	}
	if err := rstr.SendRequestHeader(&http.Request{
		Method: http.MethodConnect,
		Proto:  "connect-udp",
		Host:   u.Host,
		Header: hdr,
		URL:    u,
	}); err != nil {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("failed to send request: %w", err)}
	}
	res, err := rstr.ReadResponse()
	if err != nil {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("failed to read response: %w", err)}
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("server responded with %d", res.StatusCode)}
	}
	if err := validateCapsuleResponse(res); err != nil {
		return nil, err
	}
	resultErr = finishSetup()
	setupFinished = true
	if resultErr != nil {
		return nil, resultErr
	}
	if err := rstr.SetDeadline(noDeadline); err != nil {
		return nil, &net.OpError{Op: "listen", Net: network, Source: proxy, Addr: dst, Err: fmt.Errorf("clearing stream deadline failed: %w", err)}
	}
	transferred = true
	tunnel := newH3Conn(rstr, h3Conn.LocalAddr(), target)
	tunnel.release = release
	return tunnel, nil
}

func validateCapsuleResponse(response *http.Response) error {
	if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusResetContent || response.StatusCode == http.StatusPartialContent {
		return errors.New("proxy returned an invalid capsule response status")
	}
	for name := range response.Header {
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Content-Type") || strings.EqualFold(name, "Transfer-Encoding") {
			return errors.New("proxy returned forbidden capsule response metadata")
		}
	}
	item, err := httpsfv.UnmarshalItem(response.Header.Values(http3.CapsuleProtocolHeader))
	if enabled, ok := item.Value.(bool); err != nil || !ok || !enabled {
		return errors.Join(errors.New("proxy did not negotiate Capsule-Protocol"), err)
	}
	return nil
}

func (d *Dialer) resolveProxy(ctx context.Context, network, address string) (*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return nil, errors.New("invalid proxy UDP port")
	}
	if literal := net.ParseIP(host); literal != nil {
		if network == "udp4" && literal.To4() == nil || network == "udp6" && literal.To4() != nil {
			return nil, errors.New("proxy address is outside enabled IP family")
		}
		return &net.UDPAddr{IP: literal, Port: int(number)}, nil
	}
	resolver := d.resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, candidate := range addresses {
		if network == "udp4" && candidate.IP.To4() == nil || network == "udp6" && candidate.IP.To4() != nil {
			continue
		}
		return &net.UDPAddr{IP: candidate.IP, Port: int(number), Zone: candidate.Zone}, nil
	}
	return nil, errors.New("no proxy address in enabled IP family")
}

func (d *Dialer) SupportHTTP3() bool {
	var host, port bool
	for _, name := range d.template.Varnames() {
		switch name {
		case uriTemplateTargetHost:
			host = true
		case uriTemplateTargetPort:
			port = true
		}
	}
	return host && port
}
