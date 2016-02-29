package redix

import (
	"bufio"
	"errors"
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
		if result, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
			conn.Close()
			return nil, err
		} else {
			if result != "+OK\r\n" {
				conn.Close()
				return nil, errors.New("invalid password")
			}
		}
	}
	return conn, nil
}
