package redix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type Proxy struct {
	dialer       *Dialer
	mgr          *ConnectionManager
	clientConn   net.Conn
	serverConn   net.Conn
	clientReader *RESPReader
	serverReader *RESPReader
}

func NewProxy(clientConn net.Conn, dialer *Dialer, mgr *ConnectionManager) Proxy {
	return Proxy{
		dialer:       dialer,
		mgr:          mgr,
		clientConn:   clientConn,
		clientReader: NewReader(clientConn),
	}
}

func (proxy *Proxy) String() string {
	return fmt.Sprintf("%s <> %s", proxy.clientName(), proxy.serverName())
}

func (proxy *Proxy) clientName() string {
	if proxy.clientConn == nil {
		return "<disconnected>"
	}
	return proxy.clientConn.RemoteAddr().String()
}

func (proxy *Proxy) serverName() string {
	if proxy.serverConn == nil {
		return "<disconnected>"
	}
	return proxy.serverConn.RemoteAddr().String()
}

func (proxy *Proxy) Open() error {
	// Try to open a connection to the server
	serverConn, err := proxy.dialer.Dial()
	if err != nil {
		proxy.WriteClientErr(err)
		return err
	}

	proxy.serverConn = proxy.mgr.Add(serverConn) // Manage server connections only
	proxy.serverReader = NewReader(proxy.serverConn)
	proxy.Println("proxy opened")
	return nil
}

func (proxy *Proxy) ReadClientObject() ([]byte, error) {
	body, err := proxy.clientReader.ReadObject()
	if err != nil {
		if err == io.EOF {
			proxy.Println("client sent EOF")
		} else {
			proxy.Println("ERR:", err)
		}
		return nil, err
	}
	return body, nil
}

func (proxy *Proxy) WriteClientObject(body []byte) error {
	fmt.Printf("%v <- %v %q\n", proxy.clientName(), proxy.serverName(), body)
	_, err := proxy.clientConn.Write(body)
	return err
}

func (proxy *Proxy) WriteClientErr(e error) error {
	resp := "-ERR " + e.Error() + "\r\n"
	fmt.Printf("%v <- %v %q\n", proxy.clientName(), proxy.serverName(), resp)
	_, err := proxy.clientConn.Write([]byte(resp))
	return err
}

func (proxy *Proxy) WriteServerObject(body []byte) error {
	fmt.Printf("%v -> %v %s\n", proxy.clientName(), proxy.serverName(), proxy.SprintRESP(body))
	_, err := proxy.serverConn.Write(body)
	if err != nil {
		proxy.Println("ERR:", err)
		return err
	}
	resp, err := proxy.serverReader.ReadObject()
	if err != nil {
		if err == io.EOF {
			proxy.Println("server sent EOF")
		} else {
			proxy.Println("ERR:", err)
		}
		return err
	}
	return proxy.WriteClientObject(resp)
}

func (proxy *Proxy) Println(msg ...interface{}) {
	args := make([]interface{}, len(msg)+1)
	args[0] = proxy
	for i := 1; i < len(args); i++ {
		args[i] = msg[i-1]
	}
	fmt.Println(args...)
}

func (proxy *Proxy) SprintRESP(body []byte) string {
	resp, err := NewReader(bytes.NewReader(body)).ParseObject()
	if err != nil {
		return err.Error()
	}
	if resp == nil {
		return `(null)`
	}
	var result string
	for i, p := range resp {
		if i != 0 {
			result += fmt.Sprintf(" ")
		}
		if p == nil {
			result += fmt.Sprintf(`(null)`)
		} else {
			result += fmt.Sprintf("%q", p)
		}
	}
	return result
}

func (proxy *Proxy) Close() {
	proxy.serverConn.Close()
	proxy.clientConn.Close()
	proxy.Println("proxy closed")
}

// 1. Lock dialer
// 2. closeAll in-flight proxies
// 3. execute promotion
// 4. Reset dialer with promoted vals
// 5. Unlock dialer
func (proxy *Proxy) Promote(ip, port, auth string) error {
	proxy.dialer.mu.Lock()
	defer proxy.dialer.mu.Unlock()

	proxy.Println("promoting", ip, port)

	// proxy.Println("dialing " + ip + ":" + port)
	dialer := Dialer{IP: ip, Port: port, Auth: auth}
	slaveConn, err := dialer.Dial()
	if err != nil {
		return err
	}
	defer slaveConn.Close()

	slaveReader := NewReader(slaveConn)

	// Close all server connections, thereby severing any in-flight requests
	// This proxy will thereby be closed by main when this function returns
	proxy.mgr.CloseAll()

	// Check replication lag in a loop until zero
	for range time.Tick(100 * time.Millisecond) {
		if _, err := slaveConn.Write([]byte("*1\r\n$4\r\nINFO\r\n")); err != nil {
			return err
		}
		parsed, err := slaveReader.ParseObject()
		if err != nil {
			return err
		}
		info, err := proxy.parseMasterReplOffset(parsed[0])
		if err != nil {
			return err
		}
		if offset, ok := info["master_repl_offset"]; !ok {
			return errors.New("no replication offset found")
		} else {
			proxy.Println("master_repl_offset:" + offset)
			if offset == "0" {
				break
			}
		}
	}

	if _, err := slaveConn.Write([]byte("*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n")); err != nil {
		return err
	}
	response, err := slaveReader.ReadObject()
	if err != nil {
		return err
	}
	if string(response) != "+OK\r\n" {
		return errors.New(string(response))
	}

	proxy.WriteClientObject(response)

	proxy.dialer.Reset(ip, port, auth)

	return nil
}

func (proxy *Proxy) parseMasterReplOffset(info []byte) (map[string]string, error) {
	// r := bufio.NewReader(bytes.NewReader(info))
	m := map[string]string{}

	scanner := bufio.NewScanner(bytes.NewReader(info))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		kv := strings.Split(line, ":")
		m[kv[0]] = kv[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return m, nil

	// for line, err := r.ReadSlice('\n'); err != io.EOF;

	// TODO: Actually parse, don't just hardcode
	// switch key {
	// case "slave0":
	// 	return "ip=127.0.0.1,port=6380,state=online,offset=26940,lag=1", nil
	// case "master_repl_offset":
	// 	return "0", nil
	// default:
	// 	return "", errors.New("no such info key: " + key)
	// }
}
