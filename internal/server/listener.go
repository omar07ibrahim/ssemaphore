package server

import (
	"errors"
	"net"
	"sync"
)

type boundedListener struct {
	net.Listener

	slots  chan struct{}
	closed chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newBoundedListener(listener net.Listener, maximum int) *boundedListener {
	return &boundedListener{
		Listener: listener,
		slots:    make(chan struct{}, maximum),
		closed:   make(chan struct{}),
	}
}

func (l *boundedListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.closed:
		return nil, net.ErrClosed
	}

	connection, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}
	select {
	case <-l.closed:
		_ = connection.Close()
		l.release()
		return nil, net.ErrClosed
	default:
	}
	return &boundedConnection{Conn: connection, release: l.release}, nil
}

func (l *boundedListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
		l.closeErr = l.Listener.Close()
	})
	return l.closeErr
}

func (l *boundedListener) release() {
	select {
	case <-l.slots:
	default:
		panic("server: connection slot released twice")
	}
}

type boundedConnection struct {
	net.Conn
	release func()

	closeOnce sync.Once
	closeErr  error
}

func (c *boundedConnection) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		c.release()
	})
	return c.closeErr
}

func validateListener(listener net.Listener) error {
	switch bound := listener.(type) {
	case *net.TCPListener:
		if bound == nil {
			return errors.New("TCP listener must not be nil")
		}
		address, ok := bound.Addr().(*net.TCPAddr)
		if !ok || address == nil || len(address.IP) == 0 || !address.IP.IsLoopback() || address.Zone != "" ||
			address.Port <= 0 || address.Port > 65535 {
			return errors.New("TCP listener must already be bound to a numeric loopback address")
		}
	case *net.UnixListener:
		if bound == nil {
			return errors.New("Unix listener must not be nil")
		}
		address, ok := bound.Addr().(*net.UnixAddr)
		if !ok || address == nil || address.Name == "" || address.Network() != "unix" {
			return errors.New("Unix listener must be a byte-stream socket")
		}
	default:
		return errors.New("listener must be a concrete bound TCP or Unix listener")
	}
	return nil
}
