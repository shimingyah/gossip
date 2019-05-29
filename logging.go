package gossip

import (
	"fmt"
	"net"
)

// LogAddress returns the net string address
func LogAddress(addr net.Addr) string {
	if addr == nil {
		return "from=<unknown address>"
	}

	return fmt.Sprintf("from=%s", addr.String())
}

// LogConn returns the connect string address
func LogConn(conn net.Conn) string {
	if conn == nil {
		return LogAddress(nil)
	}

	return LogAddress(conn.RemoteAddr())
}
