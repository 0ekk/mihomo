package xhttp

import (
	"io"
	"net"
	"sync"
	"time"
)

type splitConn struct {
	writer  io.WriteCloser
	reader  io.ReadCloser
	remote  net.Addr
	local   net.Addr
	onClose func()
	closeOnce sync.Once
	closeErr  error
}

func (c *splitConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *splitConn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

func (c *splitConn) Close() error {
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		err := c.writer.Close()
		err2 := c.reader.Close()
		if err != nil {
			c.closeErr = err
			return
		}
		c.closeErr = err2
	})
	return c.closeErr
}

func (c *splitConn) LocalAddr() net.Addr  { return c.local }
func (c *splitConn) RemoteAddr() net.Addr { return c.remote }

func (*splitConn) SetDeadline(t time.Time) error     { _ = t; return nil }
func (*splitConn) SetReadDeadline(t time.Time) error { _ = t; return nil }
func (*splitConn) SetWriteDeadline(t time.Time) error {
	_ = t
	return nil
}
