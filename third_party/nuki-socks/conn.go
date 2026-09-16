package socks

import (
	"bytes"
	"errors"
	"net"
	"sync"
)

// A Conn represents a tcp forward proxy connection.
type Conn struct {
	net.Conn
	boundAddr net.Addr
}

// BoundAddr returns the address assigned by the proxy server for
// connecting to the command target address from the proxy server.
func (c *Conn) BoundAddr() net.Addr {
	if c == nil {
		return nil
	}
	return c.boundAddr
}

// A PacketConn represents a udp forward proxy connection.
type PacketConn struct {
	net.PacketConn
	control         net.Conn
	done            chan struct{}
	closeMu         sync.Mutex
	released        bool
	monitorReported bool
	monitorClose    error
	readMu          sync.Mutex
	writeMu         sync.Mutex
	boundAddr       net.Addr
	remoteAddr      net.Addr
	bufRead         [65507]byte
	bufWrite        [65507]byte
}

// BoundAddr returns the address assigned by the proxy server for
// connecting to the command target address from the proxy server.
func (c *PacketConn) BoundAddr() net.Addr {
	if c == nil {
		return nil
	}
	return c.boundAddr
}

// RemoteAddr returns the address of the command target address.
func (c *PacketConn) RemoteAddr() net.Addr {
	if c == nil {
		return nil
	}
	return c.remoteAddr
}

// ReadFrom implements the [net.PacketConn] interface.
func (c *PacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	n, sender, err := c.PacketConn.ReadFrom(c.bufRead[:])
	if err != nil {
		return 0, nil, err
	}
	if sender == nil || sender.String() != c.boundAddr.String() || n < 4 || c.bufRead[0] != 0 || c.bufRead[1] != 0 || c.bufRead[2] != 0 {
		return 0, nil, errors.New("socks: invalid UDP relay frame")
	}
	buf := bytes.NewBuffer(c.bufRead[3:n])
	addr, err := readAddr(buf)
	if err != nil {
		return 0, nil, err
	}
	n = copy(b, buf.Bytes())
	return n, addr, nil
}

// WriteTo implements the [net.PacketConn] interface.
func (c *PacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if addr == nil || len(b) > len(c.bufWrite)-262 {
		return 0, errors.New("socks: invalid UDP payload")
	}
	buf := bytes.NewBuffer(c.bufWrite[:0])
	buf.Write([]byte{0x00, 0x00, 0x00})
	a, err := buildAddr(addr.String())
	if err != nil {
		return 0, err
	}
	if err := writeAddr(buf, a); err != nil {
		return 0, err
	}
	n, err := buf.Write(b)
	if err != nil {
		return 0, err
	}
	if _, err := c.PacketConn.WriteTo(buf.Bytes(), c.boundAddr); err != nil {
		return 0, err
	}
	return n, nil
}

// Read implements the net.Conn Read method.
func (c *PacketConn) Read(b []byte) (int, error) {
	n, _, err := c.ReadFrom(b)
	return n, err
}

// Write implements the net.Conn Write method.
func (c *PacketConn) Write(b []byte) (int, error) {
	return c.WriteTo(b, c.remoteAddr)
}
