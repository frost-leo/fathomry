package socks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
)

// Server is accepting connections and handling the details of the SOCKS5 protocol
type Server struct {
	// Context is the context for the server.
	Context context.Context

	// Authentication is the optional authentication function
	// that is called when a client requests authentication.
	Authentication Authentication

	// ProxyDial specifies the optional dial function for
	// establishing the transport connection.
	ProxyDial func(context.Context, string, string) (net.Conn, error)

	// ProxyListen specifies the optional proxyListen function for
	// establishing the transport connection.
	ProxyListen func(context.Context, string, string) (net.Listener, error)

	// ProxyListenPacket specifies the optional listen function for
	// establishing the transport connection for UDP associate.
	ProxyListenPacket func(context.Context, string, string) (net.PacketConn, error)

	pool sync.Pool
}

// NewServer creates a new Server
func NewServer() *Server {
	return &Server{
		Context: context.Background(),
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 32*1024)
				return &b
			},
		},
	}
}

// ListenAndServe is used to create a listener and serve on it
func (s *Server) ListenAndServe(network, addr string) error {
	var l net.Listener
	var err error
	if s.ProxyListen != nil {
		l, err = s.ProxyListen(context.Background(), network, addr)
	} else {
		var lc net.ListenConfig
		l, err = lc.Listen(context.Background(), network, addr)
	}
	if err != nil {
		return err
	}
	defer l.Close()
	return s.Serve(l)
}

// Serve is used to serve connections from a listener
func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(c net.Conn) error {
	defer c.Close()

	b := make([]byte, 3)
	if _, err := io.ReadFull(c, b[:2]); err != nil {
		return err
	}
	if b[0] != Version5 {
		return errors.New("unexpected protocol version " + strconv.Itoa(int(b[0])))
	}

	l := int(b[1])
	if _, err := io.ReadFull(c, b[:l]); err != nil {
		return err
	}
	methods := b[:l]

	if s.Authentication != nil && bytes.IndexByte(methods, byte(AuthMethodUsernamePassword)) != -1 {
		_, err := c.Write([]byte{Version5, byte(AuthMethodUsernamePassword)})
		if err != nil {
			return err
		}
		if err := s.Authentication.Validate(s.Context, c); err != nil {
			if _, err := c.Write([]byte{AuthUsernamePasswordVersion, byte(AuthStatusFailed)}); err != nil {
				return err
			}
			return fmt.Errorf("authentication failed: %w", err)
		}
		if _, err := c.Write([]byte{AuthUsernamePasswordVersion, byte(AuthStatusSucceeded)}); err != nil {
			return err
		}
	} else if s.Authentication == nil && bytes.IndexByte(methods, byte(AuthMethodNotRequired)) != -1 {
		_, err := c.Write([]byte{Version5, byte(AuthMethodNotRequired)})
		if err != nil {
			return err
		}
	} else {
		_, err := c.Write([]byte{Version5, byte(AuthMethodNoAcceptableMethods)})
		if err != nil {
			return err
		}
		return errors.New("no acceptable authentication methods")
	}

	_, err := io.ReadFull(c, b[:3])
	if err != nil {
		return err
	}

	if b[0] != Version5 {
		return errors.New("unexpected protocol version " + strconv.Itoa(int(b[0])))
	}
	if b[2] != 0 {
		return errors.New("non-zero reserved field")
	}
	addr, err := readAddr(c)
	if err != nil {
		if errors.Is(err, ErrUnknownAddressType) {
			if err := sendReply(c, StatusAddrTypeNotSupported, nil); err != nil {
				return err
			}
		}
		return err
	}

	switch Command(b[1]) {
	case CmdConnect:
		return s.handleConnect(c, addr)
	case CmdBind:
		return s.handleBind(c, addr)
	case CmdAssociate:
		return s.handleAssociate(c, addr)
	default:
		if err := sendReply(c, StatusCommandNotSupported, nil); err != nil {
			return err
		}
		return errors.New("unsupported command: " + strconv.Itoa(int(b[1])))
	}
}

func (s *Server) handleConnect(conn net.Conn, addr *Addr) error {
	var err error
	var target net.Conn
	if s.ProxyDial != nil {
		target, err = s.ProxyDial(s.Context, "tcp", addr.String())
	} else {
		var dd net.Dialer
		target, err = dd.DialContext(s.Context, "tcp", addr.String())
	}
	if err != nil {
		if err := sendReply(conn, StatusNetworkUnreachable, nil); err != nil {
			return fmt.Errorf("failed to send reply: %v", err)
		}
		return fmt.Errorf("connect to %v failed: %w", addr, err)
	}
	defer target.Close()

	localAddr := target.LocalAddr()
	local, ok := localAddr.(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("connect to %v failed: local address is %s://%s", addr, localAddr.Network(), localAddr.String())
	}
	bind := Addr{IP: local.IP, Port: local.Port}
	if err := sendReply(conn, StatusSucceeded, &bind); err != nil {
		return fmt.Errorf("failed to send reply: %v", err)
	}

	buf1 := s.pool.Get().(*[]byte)
	buf2 := s.pool.Get().(*[]byte)
	defer func() {
		s.pool.Put(buf1)
		s.pool.Put(buf2)
	}()
	return tunnel(s.Context, target, conn, *buf1, *buf2)
}

func (s *Server) handleBind(conn net.Conn, addr *Addr) error {
	var err error
	var listener net.Listener
	if s.ProxyListen != nil {
		listener, err = s.ProxyListen(s.Context, "tcp", addr.String())
	} else {
		var lc net.ListenConfig
		listener, err = lc.Listen(s.Context, "tcp", addr.String())
	}
	if err != nil {
		if err := sendReply(conn, StatusNetworkUnreachable, nil); err != nil {
			return fmt.Errorf("failed to send reply: %v", err)
		}
		return fmt.Errorf("connect to %v failed: %w", addr, err)
	}

	localAddr := listener.Addr()
	local, ok := localAddr.(*net.TCPAddr)
	if !ok {
		listener.Close()
		return fmt.Errorf("connect to %v failed: local address is %s://%s", addr, localAddr.Network(), localAddr.String())
	}
	bind := Addr{IP: local.IP, Port: local.Port}
	if err := sendReply(conn, StatusSucceeded, &bind); err != nil {
		listener.Close()
		return fmt.Errorf("failed to send reply: %v", err)
	}

	c, err := listener.Accept()
	if err != nil {
		listener.Close()
		if err := sendReply(conn, StatusNetworkUnreachable, nil); err != nil {
			return fmt.Errorf("failed to send reply: %v", err)
		}
		return fmt.Errorf("connect to %v failed: %w", addr, err)
	}
	listener.Close()

	remoteAddr := c.RemoteAddr()
	local, ok = remoteAddr.(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("connect to %v failed: remote address is %s://%s", addr, localAddr.Network(), localAddr.String())
	}
	bind = Addr{IP: local.IP, Port: local.Port}
	if err := sendReply(conn, StatusSucceeded, &bind); err != nil {
		return fmt.Errorf("failed to send reply: %v", err)
	}

	buf1 := s.pool.Get().(*[]byte)
	buf2 := s.pool.Get().(*[]byte)
	defer func() {
		s.pool.Put(buf1)
		s.pool.Put(buf2)
	}()
	return tunnel(s.Context, c, conn, *buf1, *buf2)
}

func (s *Server) handleAssociate(conn net.Conn, addr *Addr) error {
	var err error
	var udpConn net.PacketConn
	if s.ProxyListenPacket != nil {
		udpConn, err = s.ProxyListenPacket(s.Context, "udp", ":0")
	} else {
		var lc net.ListenConfig
		udpConn, err = lc.ListenPacket(s.Context, "udp", ":0")
	}
	if err != nil {
		if err := sendReply(conn, StatusNetworkUnreachable, nil); err != nil {
			return fmt.Errorf("failed to send reply: %v", err)
		}
		return fmt.Errorf("connect to %v failed: %w", addr, err)
	}
	defer udpConn.Close()

	udpLocal := udpConn.LocalAddr()
	udpLocalAddr, ok := udpLocal.(*net.UDPAddr)
	if !ok {
		return fmt.Errorf("connect to %v failed: local address is %s://%s", addr, udpLocal.Network(), udpLocal.String())
	}
	tcpLocal := conn.LocalAddr()
	tcpLocalAddr, ok := tcpLocal.(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("connect to %v failed: local address is %s://%s", addr, tcpLocal.Network(), tcpLocal.String())
	}
	bind := Addr{IP: tcpLocalAddr.IP, Port: udpLocalAddr.Port}
	if err := sendReply(conn, StatusSucceeded, &bind); err != nil {
		return fmt.Errorf("failed to send reply: %v", err)
	}

	go func() {
		var buf [1]byte
		for {
			_, err := conn.Read(buf[:])
			if err != nil {
				udpConn.Close()
				break
			}
		}
	}()

	var (
		sourceAddr  net.Addr
		wantSource  string
		targetAddr  net.Addr
		wantTarget  string
		replyPrefix []byte
		buf         [65507]byte
	)

	for {
		n, a, err := udpConn.ReadFrom(buf[:])
		if err != nil {
			return err
		}

		if sourceAddr == nil {
			sourceAddr = a
			wantSource = sourceAddr.String()
		}

		gotAddr := a.String()
		if wantSource == gotAddr {
			if n < 3 {
				continue
			}
			reader := bytes.NewBuffer(buf[3:n])
			a, err := readAddr(reader)
			if err != nil {
				continue
			}
			if targetAddr == nil {
				targetAddr = &net.UDPAddr{
					IP:   a.IP,
					Port: a.Port,
				}
				wantTarget = targetAddr.String()
			}
			if a.String() != wantTarget {
				continue
			}
			_, err = udpConn.WriteTo(reader.Bytes(), targetAddr)
			if err != nil {
				return err
			}
		} else if targetAddr != nil && wantTarget == gotAddr {
			if replyPrefix == nil {
				b := bytes.NewBuffer(make([]byte, 3, 16))
				t, err := buildAddr(wantTarget)
				if err != nil {
					return err
				}
				err = writeAddr(b, t)
				if err != nil {
					return err
				}
				replyPrefix = b.Bytes()
			}
			copy(buf[len(replyPrefix):len(replyPrefix)+n], buf[:n])
			copy(buf[:len(replyPrefix)], replyPrefix)
			_, err = udpConn.WriteTo(buf[:len(replyPrefix)+n], sourceAddr)
			if err != nil {
				return err
			}
		}
	}
}
