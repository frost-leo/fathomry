// Package socks provides a SOCKS version 5 client and server implementation.
//
// SOCKS protocol version 5 is defined in RFC 1928.
// Username/Password authentication for SOCKS version 5 is defined in
// RFC 1929.
package socks

import (
	"errors"
	"net"
	"strconv"
)

// Wire protocol constants.
const (
	Version5 = 0x05

	AddrTypeIPv4 = 0x01
	AddrTypeFQDN = 0x03
	AddrTypeIPv6 = 0x04
)

// A Command represents a SOCKS command.
type Command int

func (cmd Command) String() string {
	switch cmd {
	case CmdConnect:
		return "socks connect"
	case CmdBind:
		return "socks bind"
	case CmdAssociate:
		return "socks associate"
	default:
		return "socks " + strconv.Itoa(int(cmd))
	}
}

// Command constants.
const (
	CmdConnect   Command = 0x01 // establishes an active-open forward proxy connection
	CmdBind      Command = 0x02 // establishes a passive-open forward proxy connection
	CmdAssociate Command = 0x03 // establishes a UDP associate connection
)

// An AuthMethod represents a SOCKS authentication method.
type AuthMethod int

// AuthMethod constants.
const (
	AuthMethodNotRequired         AuthMethod = 0x00 // no authentication required
	AuthMethodUsernamePassword    AuthMethod = 0x02 // use username/password
	AuthMethodNoAcceptableMethods AuthMethod = 0xff // no acceptable authentication methods
)

// A Reply represents a SOCKS command reply code.
type Reply int

func (code Reply) String() string {
	switch code {
	case StatusSucceeded:
		return "succeeded"
	case StatusGeneralFailure:
		return "general socks server failure"
	case StatusConnectionNotAllowed:
		return "connection not allowed by ruleset"
	case StatusNetworkUnreachable:
		return "network unreachable"
	case StatusHostUnreachable:
		return "host unreachable"
	case StatusConnectionRefused:
		return "connection refused"
	case StatusTTLExpired:
		return "ttl expired"
	case StatusCommandNotSupported:
		return "command not supported"
	case StatusAddrTypeNotSupported:
		return "address type not supported"
	default:
		return "unknown code: " + strconv.Itoa(int(code))
	}
}

// Reply constants.
const (
	StatusSucceeded            Reply = 0x00 // request granted
	StatusGeneralFailure       Reply = 0x01 // general SOCKS server failure
	StatusConnectionNotAllowed Reply = 0x02 // connection not allowed by ruleset
	StatusNetworkUnreachable   Reply = 0x03 // network unreachable
	StatusHostUnreachable      Reply = 0x04 // host unreachable
	StatusConnectionRefused    Reply = 0x05 // connection refused
	StatusTTLExpired           Reply = 0x06 // TTL expired
	StatusCommandNotSupported  Reply = 0x07 // command not supported
	StatusAddrTypeNotSupported Reply = 0x08 // address type not supported
)

// An Addr represents a SOCKS-specific address.
// Either Name or IP is used exclusively.
type Addr struct {
	Name string // fully-qualified domain name
	IP   net.IP
	Port int
}

func (a *Addr) Network() string { return "socks" }

func (a *Addr) String() string {
	if a == nil {
		return "<nil>"
	}
	port := strconv.Itoa(a.Port)
	if a.IP == nil {
		return net.JoinHostPort(a.Name, port)
	}
	return net.JoinHostPort(a.IP.String(), port)
}

var ErrUnknownAddressType = errors.New("unknown address type")
var ErrFQDNTooLong = errors.New("fqdn too long")
