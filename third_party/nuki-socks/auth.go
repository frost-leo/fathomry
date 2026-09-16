package socks

import (
	"context"
	"errors"
	"io"
	"strconv"
)

type Authentication interface {
	Validate(ctx context.Context, rw io.ReadWriter) error
	Authenticate(ctx context.Context, rw io.ReadWriter, auth AuthMethod) error
}

const (
	AuthUsernamePasswordVersion = 0x01
	AuthStatusSucceeded         = 0x00
	AuthStatusFailed            = 0x01
)

// UsernamePassword are the credentials for the username/password
// authentication method.
type UsernamePassword struct {
	Username string
	Password string
}

// UserPass returns a new UsernamePassword instance with the given
// username and password.
func UserPass(username, password string) *UsernamePassword {
	return &UsernamePassword{
		Username: username,
		Password: password,
	}
}

// Authenticate authenticates a pair of username and password with the
// proxy server.
func (up *UsernamePassword) Authenticate(ctx context.Context, rw io.ReadWriter, auth AuthMethod) error {
	switch auth {
	case AuthMethodNotRequired:
		return nil
	case AuthMethodUsernamePassword:
		if len(up.Username) == 0 || len(up.Username) > 255 || len(up.Password) > 255 {
			return errors.New("invalid username/password")
		}
		b := []byte{AuthUsernamePasswordVersion}
		b = append(b, byte(len(up.Username)))
		b = append(b, up.Username...)
		b = append(b, byte(len(up.Password)))
		b = append(b, up.Password...)
		if _, err := rw.Write(b); err != nil {
			return err
		}
		if _, err := io.ReadFull(rw, b[:2]); err != nil {
			return err
		}
		if b[0] != AuthUsernamePasswordVersion {
			return errors.New("invalid username/password version")
		}
		if b[1] != AuthStatusSucceeded {
			return errors.New("username/password authentication failed")
		}
		return nil
	}
	return errors.New("unsupported authentication method " + strconv.Itoa(int(auth)))
}

func (up *UsernamePassword) Validate(ctx context.Context, rw io.ReadWriter) error {
	b := make([]byte, 2, 2+max(len(up.Username), len(up.Password)))
	if _, err := io.ReadFull(rw, b[:2]); err != nil {
		return err
	}
	if b[0] != AuthUsernamePasswordVersion {
		return errors.New("unexpected username/password version " + strconv.Itoa(int(b[0])))
	}
	l := int(b[1]) + 1
	if cap(b) < l {
		b = make([]byte, l)
	} else {
		b = b[:l]
	}
	if _, err := io.ReadFull(rw, b); err != nil {
		return err
	}
	username := string(b[:l-1])
	l = int(b[l-1])
	if cap(b) < l {
		b = make([]byte, l)
	} else {
		b = b[:l]
	}
	if _, err := io.ReadFull(rw, b); err != nil {
		return err
	}
	password := string(b[:l])
	if username != up.Username || password != up.Password {
		return errors.New("username/password mismatch")
	}
	return nil
}
