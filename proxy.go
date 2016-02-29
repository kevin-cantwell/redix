package redix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type Proxy struct {
	dialer       *Dialer
	mgr          *ConnectionManager
	clientConn   net.Conn
	serverConn   net.Conn
	clientReader *RESPReader
	Verbose      bool
}

func NewProxy(clientConn net.Conn, dialer *Dialer, mgr *ConnectionManager) Proxy {
	return Proxy{
		dialer:       dialer,
		mgr:          mgr,
		clientConn:   clientConn,
		clientReader: NewReader(clientConn),
		Verbose:      true,
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
	// Will return when serverConn is closed
	go io.Copy(proxy.clientConn, proxy.serverConn)
	proxy.Println("proxy opened")
	return nil
}

func (proxy *Proxy) ReadClientObject() ([]byte, error) {
	body, err := proxy.clientReader.ReadObject()
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (proxy *Proxy) WriteClientErr(e error) error {
	_, err := proxy.clientConn.Write([]byte("-ERR " + e.Error() + "\r\n"))
	return err
}

func (proxy *Proxy) WriteServerObject(body []byte) error {
	if proxy.Verbose {
		fmt.Printf("%v -> %v %s\n", proxy.clientName(), proxy.serverName(), proxy.SprintRESP(body))
	}
	_, err := proxy.serverConn.Write(body)
	if err != nil {
		return err
	}
	return nil
}

func (proxy *Proxy) Println(msg ...interface{}) {
	if proxy.Verbose {
		args := make([]interface{}, len(msg)+1)
		args[0] = proxy
		for i := 1; i < len(args); i++ {
			args[i] = msg[i-1]
		}
		fmt.Println(args...)
	}
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
	if proxy.serverConn != nil {
		proxy.serverConn.Close()
	}
	if proxy.clientConn != nil {
		proxy.clientConn.Close()
	}
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

	proxy.Println("promoting:", ip, port)

	// Create a connection to the slave
	slaveDialer := Dialer{IP: ip, Port: port, Auth: auth}
	slaveConn, err := slaveDialer.Dial()
	if err != nil {
		return err
	}
	defer slaveConn.Close()

	slaveReader := NewReader(slaveConn)

	// Create a new connection to the master
	masterDialer := Dialer{IP: proxy.dialer.IP, Port: proxy.dialer.Port, Auth: proxy.dialer.Auth}
	masterConn, err := masterDialer.Dial()
	if err != nil {
		return err
	}
	defer masterConn.Close()

	masterReader := NewReader(masterConn)

	// Close all server connections, thereby severing any in-flight requests
	// This proxy will thereby be closed by main when this function returns
	proxy.mgr.CloseAll()

	// Check replication lag in a loop until zero
	for range time.Tick(100 * time.Millisecond) {
		if _, err := masterConn.Write([]byte("*1\r\n$4\r\nINFO\r\n")); err != nil {
			return err
		}
		parsed, err := masterReader.ParseObject()
		if err != nil {
			return err
		}
		info, err := proxy.parseInfo(parsed[0])
		if err != nil {
			return err
		}
		masterOffset, ok := info["master_repl_offset"]
		if !ok {
			return errors.New("no master_repl_offset found")
		}
		// Search the first ten slaves
		slaveOffset := "-1"
		for i := 0; i < 10; i++ {
			slaveInfo, ok := info[fmt.Sprintf("slave%d", i)]
			if !ok {
				return errors.New(ip + ":" + port + " is not a slave")
			}
			slaveMap := map[string]string{}
			for _, slaveKV := range strings.Split(slaveInfo, ",") {
				kv := strings.Split(slaveKV, "=")
				slaveMap[kv[0]] = kv[1]
			}
			if slaveMap["ip"] == ip && slaveMap["port"] == port {
				slaveOffset = slaveMap["offset"]
				break
			}
		}
		// If no matching slave found
		if slaveOffset == "-1" {
			return errors.New(ip + ":" + port + " is not a slave")
		}
		moff, _ := strconv.Atoi(masterOffset)
		soff, _ := strconv.Atoi(slaveOffset)
		proxy.Println("promoting: slave is behind by", moff-soff)

		if slaveOffset == masterOffset {
			break
		}
	}

	if _, err := slaveConn.Write([]byte("*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n")); err != nil {
		return err
	}
	response, err := slaveReader.ParseObject()
	if err != nil {
		return err
	}
	if string(response[0]) != "OK" {
		return errors.New(string(response[0]))
	}

	proxy.clientConn.Write([]byte("+OK\r\n"))

	proxy.dialer.Reset(ip, port, auth)

	return nil
}

func (proxy *Proxy) parseInfo(info []byte) (map[string]string, error) {
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
}
