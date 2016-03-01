package redix

import (
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

func (proxy *Proxy) ParseClientObject() (Array, error) {
	resp, err := proxy.clientReader.ParseObject()
	if err != nil {
		return nil, err
	}
	array, ok := resp.(Array)
	if !ok {
		return nil, ErrInvalidSyntax
	}
	return array, nil
}

func (proxy *Proxy) WriteClientErr(e error) error {
	_, err := proxy.clientConn.Write([]byte("-ERR " + e.Error() + "\r\n"))
	return err
}

func (proxy *Proxy) WriteServerObject(body []byte) error {
	if proxy.Verbose {
		fmt.Printf("%v -> %v %q\n", proxy.clientName(), proxy.serverName(), body) // proxy.SprintRESP(body))
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
func (proxy *Proxy) Promote(slaveID, auth, timeout string) error {
	proxy.dialer.mu.Lock()
	defer proxy.dialer.mu.Unlock()

	proxy.Println("promote:", "id="+slaveID, "timeout="+timeout)

	// Create a new connection to the master
	dialer := Dialer{IP: proxy.dialer.IP, Port: proxy.dialer.Port, Auth: proxy.dialer.Auth}
	masterConn, err := dialer.Dial()
	if err != nil {
		return err
	}
	defer masterConn.Close()
	masterReader := NewReader(masterConn)

	millis, err := strconv.Atoi(timeout)
	if err != nil {
		return errors.New("timeout is not an integer or out of range")
	}
	cancel := time.After(time.Duration(millis) * time.Millisecond)

	multiExec := [][]byte{
		[]byte("*1\r\n$5\r\nMULTI\r\n"),
		[]byte("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n"),
		[]byte("*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n" + fmt.Sprintf("$%d\r\n%s\r\n", len(timeout), timeout)),
		[]byte("*1\r\n$4\r\nEXEC\r\n"),
	}

	proxy.Println("promote:", "pausing clients for", timeout, "milliseconds")
	var info string
	for _, cmd := range multiExec {
		proxy.Println("promote:", fmt.Sprintf("-> %q", cmd))
		if _, err := masterConn.Write(cmd); err != nil {
			proxy.Println("promote:", "ERR", err)
			return err
		}
		resp, err := masterReader.ParseObject()
		if err != nil {
			proxy.Println("promote:", "ERR", err)
			return errors.New("unable to pause clients")
		}
		switch t := resp.(type) {
		case Error:
			proxy.Println("promote:", fmt.Sprintf("<- %q", t.String()))
			return errors.New(t.String())
		// EXEC returns an array, where the first item is a Bulk String INFO result
		case Array:
			for _, r := range t {
				if e, ok := r.(Error); ok {
					proxy.Println("promote:", fmt.Sprintf("<- %q", e.String()))
					return errors.New(e.String())
				}
				proxy.Println("promote:", fmt.Sprintf("<- %q", r.String()))
			}
			info = t[0].String() // Just the info
		default:
			proxy.Println("promote:", fmt.Sprintf("<- %q", t.String()))
		}
	}

	repl, err := proxy.parseInfo(info)
	if err != nil {
		return err
	}
	slave, ok := repl[slaveID]
	if !ok {
		return fmt.Errorf("no slave '%s' connected", slaveID)
	}
	var ip, port string
	for _, part := range strings.Split(slave, ",") {
		kv := strings.Split(part, "=")
		switch kv[0] {
		case "ip":
			ip = kv[1]
		case "port":
			port = kv[1]
		}
	}

	masterReplOffset, ok := repl["master_repl_offset"]
	if !ok {
		return errors.New("no 'master_repl_offset' value")
	}

	proxy.Println("promote:", "master_repl_offset:"+masterReplOffset)

	// Create a new connection to the slave
	dialer = Dialer{IP: ip, Port: port, Auth: auth}
	slaveConn, err := dialer.Dial()
	if err != nil {
		return err
	}
	defer slaveConn.Close()
	slaveReader := NewReader(slaveConn)

	for range time.Tick(100 * time.Millisecond) {
		select {
		case <-cancel:
			return errors.New("timed out")
		default:
		}
		if _, err := slaveConn.Write([]byte("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n")); err != nil {
			return err
		}
		resp, err := slaveReader.ParseObject()
		if err != nil {
			proxy.Println("promote:", "ERR:", err)
			return errors.New("unable to discover slave replication offset")
		}
		info, ok := resp.(BulkString)
		if !ok {
			return errors.New(resp.HumanReadable())
		}
		repl, err := proxy.parseInfo(info.String())
		if err != nil {
			return err
		}

		slaveReplOffset, ok := repl["slave_repl_offset"]
		if !ok {
			return fmt.Errorf("no slave '%s' replicating", slaveID)
		}
		proxy.Println("promote:", "slave_repl_offset:"+slaveReplOffset)
		if len(masterReplOffset) <= len(slaveReplOffset) || masterReplOffset < slaveReplOffset {
			break
		}
	}

	select {
	case <-cancel:
		return errors.New("timed out")
	default:
	}

	if _, err := slaveConn.Write([]byte("*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n")); err != nil {
		return err
	}
	resp, err := slaveReader.ParseObject()
	if err != nil {
		return err
	}
	if e, ok := resp.(Error); ok {
		return errors.New(string(e))
	}

	proxy.clientConn.Write([]byte("+OK\r\n"))
	proxy.dialer.Reset(ip, port, auth)

	return nil
}

func (proxy *Proxy) parseInfo(info string) (map[string]string, error) {
	m := map[string]string{}
	for _, line := range strings.Split(info, "\r\n") {
		kv := strings.Split(string(line), ":")
		if len(kv) == 2 {
			m[kv[0]] = kv[1]
		}
	}
	return m, nil
}
