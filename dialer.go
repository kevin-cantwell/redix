package redix

import (
	"fmt"
	"net"
	"sync"
)

type Dialer struct {
	mu   sync.RWMutex
	IP   string
	Port string
	Auth string
}

// Call Lock before executing this method
func (dialer *Dialer) Reset(ip, port, auth string) {
	dialer.IP, dialer.Port, dialer.Auth = ip, port, auth
}

func (dialer *Dialer) Dial() (net.Conn, error) {
	dialer.mu.RLock()
	defer dialer.mu.RUnlock()

	conn, err := net.Dial("tcp", dialer.IP+":"+dialer.Port)
	if err != nil {
		return nil, err
	}
	if len(dialer.Auth) > 0 {
		if _, err := conn.Write([]byte(fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(dialer.Auth), dialer.Auth))); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}
