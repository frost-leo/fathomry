package socks

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"time"
)

// A Dialer holds SOCKS-specific options.
type Dialer struct {
	proxyNetwork string // network between a proxy server and a client
	proxyAddress string // proxy server address

	// ProxyDial specifies the optional dial function for
	// establishing the transport connection.
	ProxyDial func(context.Context, string, string) (net.Conn, error)

	// ProxyListenPacket specifies the optional listen function for
	// establishing the transport connection for UDP associate.
	ProxyListenPacket func(context.Context, string, string) (net.PacketConn, error)

	// AuthMethods specifies the list of request authentication
	// methods.
	// If empty, SOCKS client requests only AuthMethodNotRequired.
	AuthMethods []AuthMethod

	// Authenticate specifies the optional authentication
	// function. It must be non-nil when AuthMethods is not empty.
	// It must return an error when the authentication is failed.
	Authenticate func(context.Context, io.ReadWriter, AuthMethod) error

	// Timeout bounds each phase of connection establishment (the dial
	// to the proxy server and the SOCKS handshake), not their sum. It
	// applies in addition to any deadline on the dial context, whichever
	// is sooner. A Timeout of zero means no timeout.
	Timeout time.Duration
}

// DialContext connects to the provided address on the provided
// network.
//
// The returned error value may be a net.OpError. When the Op field of
// net.OpError contains "socks", the Source field contains a proxy
// server address and the Addr field contains a command target
// address.
//
// See func Dial of the net package of standard library for a
// description of the network and address parameters.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	cmd := CmdConnect
	switch network {
	case "tcp", "tcp6", "tcp4":
	default:
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: errors.New("unsupported network")}
	}
	if ctx == nil {
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: errors.New("nil context")}
	}
	var err error
	var conn net.Conn
	if d.ProxyDial != nil {
		conn, err = d.ProxyDial(ctx, d.proxyNetwork, d.proxyAddress)
	} else {
		dd := net.Dialer{Timeout: d.Timeout}
		conn, err = dd.DialContext(ctx, d.proxyNetwork, d.proxyAddress)
	}
	if err != nil {
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: err}
	}
	addr, err := d.connect(ctx, conn, address, cmd)
	if err != nil {
		conn.Close()
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: err}
	}
	return &Conn{Conn: conn, boundAddr: addr}, nil
}

// PacketDialContext connects to the provided address on the provided
// network for UDP associate.
func (d *Dialer) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	cmd := CmdAssociate
	switch network {
	case "udp", "udp6", "udp4":
	default:
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: errors.New("unsupported network")}
	}
	if ctx == nil {
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: errors.New("nil context")}
	}
	var err error
	var conn net.Conn
	if d.ProxyDial != nil {
		conn, err = d.ProxyDial(ctx, d.proxyNetwork, d.proxyAddress)
	} else {
		dd := net.Dialer{Timeout: d.Timeout}
		conn, err = dd.DialContext(ctx, d.proxyNetwork, d.proxyAddress)
	}
	if err != nil {
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: err}
	}
	addr, err := d.connect(ctx, conn, address, cmd)
	if err != nil {
		conn.Close()
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: err}
	}
	var udpConn net.PacketConn
	if d.ProxyListenPacket != nil {
		udpConn, err = d.ProxyListenPacket(ctx, "udp", ":0")
	} else {
		var l net.ListenConfig
		udpConn, err = l.ListenPacket(ctx, "udp", ":0")
	}
	if err != nil {
		conn.Close()
		proxy, dst, _ := d.pathAddrs(address)
		return nil, &net.OpError{Op: cmd.String(), Net: network, Source: proxy, Addr: dst, Err: err}
	}
	boundAddr := &net.UDPAddr{
		IP:   addr.IP,
		Port: addr.Port,
	}
	ip, port, _ := splitHostPort(address)
	remoteAddr := &net.UDPAddr{
		IP:   net.ParseIP(ip),
		Port: port,
	}
	if boundAddr.IP.IsUnspecified() {
		if peer, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			boundAddr.IP = append(net.IP(nil), peer.IP...)
		}
	}
	wrapConn := &PacketConn{PacketConn: udpConn, boundAddr: boundAddr, remoteAddr: remoteAddr,
		control: conn, done: make(chan struct{})}
	go func() {
		defer close(wrapConn.done)
		var buf [1]byte
		for {
			_, err := conn.Read(buf[:])
			if err != nil {
				wrapConn.monitorClose = errors.Join(conn.Close(), udpConn.Close())
				break
			}
		}
	}()
	return wrapConn, nil
}

func (d *Dialer) SupportHTTP3() bool {
	return true
}

// NewDialer returns a new Dialer that dials through the provided
// proxy server's address.
func NewDialer(u *url.URL) (*Dialer, error) {
	if u == nil {
		return nil, errors.New("nil url")
	}
	switch u.Scheme {
	case "socks5", "socks5h":
	default:
		return nil, errors.New("unsupported scheme")
	}
	network := "tcp"
	address := u.Host
	port := u.Port()
	if port == "" {
		host := u.Hostname()
		port = "1080"
		address = net.JoinHostPort(host, port)
	}
	d := &Dialer{proxyNetwork: network, proxyAddress: address}
	if u.User != nil {
		auth := &UsernamePassword{}
		auth.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			auth.Password = password
		}
		d.AuthMethods = []AuthMethod{
			AuthMethodNotRequired,
			AuthMethodUsernamePassword,
		}
		d.Authenticate = auth.Authenticate
	}
	return d, nil
}
