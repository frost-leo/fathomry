package socks

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
)

func (d *Dialer) pathAddrs(address string) (proxy, dst net.Addr, err error) {
	for i, s := range []string{d.proxyAddress, address} {
		host, port, err := splitHostPort(s)
		if err != nil {
			return nil, nil, err
		}
		a := &Addr{Port: port}
		a.IP = net.ParseIP(host)
		if a.IP == nil {
			a.Name = host
		}
		if i == 0 {
			proxy = a
		} else {
			dst = a
		}
	}
	return
}

func splitHostPort(address string) (string, int, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	portnum, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	if 1 > portnum || portnum > 0xffff {
		return "", 0, errors.New("port number out of range " + port)
	}
	return host, portnum, nil
}

func readAddr(r io.Reader) (*Addr, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(r, b[:1]); err != nil {
		return nil, err
	}
	l := 2
	var addr Addr
	switch b[0] {
	case AddrTypeIPv4:
		l += net.IPv4len
		addr.IP = make(net.IP, net.IPv4len)
	case AddrTypeIPv6:
		l += net.IPv6len
		addr.IP = make(net.IP, net.IPv6len)
	case AddrTypeFQDN:
		if _, err := io.ReadFull(r, b[:1]); err != nil {
			return nil, err
		}
		l += int(b[0])
	default:
		return nil, errors.New("unknown address type " + strconv.Itoa(int(b[0])))
	}
	if cap(b) < l {
		b = make([]byte, l)
	} else {
		b = b[:l]
	}
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	if addr.IP != nil {
		copy(addr.IP, b)
	} else {
		addr.Name = string(b[:len(b)-2])
	}
	addr.Port = int(b[len(b)-2])<<8 | int(b[len(b)-1])
	return &addr, nil
}

func writeAddr(w io.Writer, addr *Addr) error {
	if addr.IP != nil {
		if ip4 := addr.IP.To4(); ip4 != nil {
			if _, err := w.Write([]byte{AddrTypeIPv4}); err != nil {
				return err
			}
			if _, err := w.Write(ip4); err != nil {
				return err
			}
		} else if ip6 := addr.IP.To16(); ip6 != nil {
			if _, err := w.Write([]byte{AddrTypeIPv6}); err != nil {
				return err
			}
			if _, err := w.Write(ip6); err != nil {
				return err
			}
		} else {
			return ErrUnknownAddressType
		}
	} else if addr.Name != "" {
		if len(addr.Name) > 255 {
			return ErrFQDNTooLong
		}
		if _, err := w.Write([]byte{AddrTypeFQDN, byte(len(addr.Name))}); err != nil {
			return err
		}
		if _, err := w.Write([]byte(addr.Name)); err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte{byte(addr.Port >> 8), byte(addr.Port)}); err != nil {
		return err
	}
	return nil
}

func sendReply(w io.Writer, rep Reply, addr *Addr) error {
	_, err := w.Write([]byte{Version5, byte(rep), 0})
	if err != nil {
		return err
	}
	if addr == nil {
		addr = &Addr{IP: net.IPv4(0, 0, 0, 0), Port: 0}
	}
	return writeAddr(w, addr)
}

func buildAddr(addr string) (*Addr, error) {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		return &Addr{IP: ip, Port: port}, nil
	}
	return &Addr{Name: host, Port: port}, nil
}

func tunnel(ctx context.Context, c1, c2 io.ReadWriteCloser, buf1, buf2 []byte) error {
	errCh := make(chan error, 2)
	go func() {
		_, err := io.CopyBuffer(c1, c2, buf1)
		errCh <- err
	}()
	go func() {
		_, err := io.CopyBuffer(c2, c1, buf2)
		errCh <- err
	}()
	defer func() {
		c1.Close()
		c2.Close()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
