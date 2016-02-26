package redix

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

type RespObject struct {
	Body []byte
	Err  error
}

type Client struct {
	conn   net.Conn
	reader *RESPReader
}

func (client *Client) String() string {
	if client == nil {
		return "<disconnected>"
	}
	return client.conn.RemoteAddr().String()
}

type Server struct {
	conn   net.Conn
	reader *RESPReader
}

func (server *Server) String() string {
	if server == nil {
		return "<disconnected>"
	}
	return server.conn.RemoteAddr().String()
}

type Proxy struct {
	client *Client
	server *Server

	// Communication channels used to manage all connections
	// during promotion
	promoting chan<- struct{}
	promoted  chan<- string
}

type ProxyConfig struct {
	Promoting chan<- struct{}
	Promoted  chan<- string
}

func NewProxy(clientConn net.Conn, config ProxyConfig) Proxy {
	return Proxy{
		client:    &Client{conn: clientConn, reader: NewReader(clientConn)},
		promoting: config.Promoting,
		promoted:  config.Promoted,
	}
}

func (proxy *Proxy) ReadClientObject() <-chan RespObject {
	resp := make(chan RespObject)
	go func() {
		body, err := proxy.client.reader.ReadObject()
		resp <- RespObject{Body: body, Err: err}
	}()
	return resp
}

func (proxy *Proxy) WriteClientObject(body []byte) error {
	fmt.Printf("%v <- %v %q\n", proxy.client, proxy.server, body)
	_, err := proxy.client.conn.Write(body)
	return err
}

func (proxy *Proxy) WriteClientErr(e error) error {
	resp := "-PROXYERR " + e.Error() + "\r\n"
	fmt.Printf("%v <- %v %q\n", proxy.client, proxy.server, resp)
	_, err := proxy.client.conn.Write([]byte(resp))
	return err
}

func (proxy *Proxy) WriteServerObject(body []byte) <-chan RespObject {
	resp := make(chan RespObject)
	go func() {
		fmt.Printf("%v -> %v %q\n", proxy.client, proxy.server, body)
		_, err := proxy.server.conn.Write(body)
		if err != nil {
			resp <- RespObject{Err: err}
			return
		}
		body, err := proxy.server.reader.ReadObject()
		resp <- RespObject{Body: body, Err: err}
	}()
	return resp
}

func (proxy *Proxy) Println(msg ...interface{}) {
	args := make([]interface{}, len(msg)+3)
	args[0] = proxy.client
	args[1] = "<>"
	args[2] = proxy.server
	for i := 3; i < len(args); i++ {
		args[i] = msg[i-3]
	}
	fmt.Println(args...)
}

func (proxy *Proxy) Open(serverURL string) error {
	// Try to open a connection to the server
	serverConn, err := net.Dial("tcp", serverURL)
	if err != nil {
		return err
	}
	proxy.server = &Server{conn: serverConn, reader: NewReader(serverConn)}
	proxy.Println("proxy opened")
	return nil
}

func (proxy *Proxy) Close() {
	defer proxy.Println("proxy closed")
	if proxy.server != nil {
		proxy.server.conn.Close()
	}
	if proxy.client != nil {
		proxy.client.conn.Close()
	}
}

type Slave struct {
	ID     string // slaveXXX
	IP     string // 127.0.0.1
	Port   string // 6379
	State  string // online
	Offset string // XXXXX
	Lag    string // XXXXX
}

func (proxy *Proxy) Promote(slaveID string) error {
	select {
	case proxy.promoting <- struct{}{}:
	default:
		return errors.New("promotion already in progress")
	}

	newServerURL := proxy.server.conn.RemoteAddr().String() // default to current server
	defer func() {
		proxy.promoted <- newServerURL
	}()

	slave, err := proxy.parseSlave(slaveID)
	if err != nil {
		return err
	}

	slaveURL := slave.IP + ":" + slave.Port
	proxy.Println("dialing " + slaveURL)
	slaveConn, err := net.Dial("tcp", slaveURL)
	if err != nil {
		return err
	}
	proxy.Println("disabling replication", slaveURL)
	if _, err := slaveConn.Write([]byte("*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n")); err != nil {
		slaveConn.Close()
		return err
	}
	slaveReader := NewReader(slaveConn)
	response, err := slaveReader.ReadObject()
	if err != nil {
		return err
	}
	if string(response) != "+OK\r\n" {
		return errors.New(string(response))
	}

	if err := proxy.WriteClientObject([]byte("+OK\r\n")); err != nil {
		return err
	}

	demotedConn := proxy.server.conn
	proxy.server = &Server{conn: slaveConn, reader: slaveReader}
	demotedConn.Close()

	newServerURL = slaveURL
	return nil
}

func (proxy *Proxy) parseSlave(slaveID string) (*Slave, error) {
	resp := <-proxy.WriteServerObject([]byte("*1\r\n$4\r\nINFO\r\n"))
	if resp.Err != nil {
		return nil, resp.Err
	}
	slaveValue, err := proxy.parseInfo(resp.Body, slaveID)
	if err != nil {
		return nil, err
	}

	slave := Slave{ID: slaveID}
	for _, keyValue := range strings.Split(slaveValue, ",") {
		kv := strings.Split(keyValue, "=")
		switch kv[0] {
		case "ip":
			slave.IP = kv[1]
		case "port":
			slave.Port = kv[1]
		case "state":
			slave.State = kv[1]
		case "offset":
			slave.Offset = kv[1]
		case "lag":
			slave.Lag = kv[1]
		}
	}
	return &slave, nil
}

func (proxy *Proxy) parseInfo(info []byte, key string) (string, error) {
	// TODO: Actually parse, don't just hardcode
	switch key {
	case "slave0":
		return "ip=127.0.0.1,port=6380,state=online,offset=26940,lag=1", nil
	case "master_repl_offset":
		return "0", nil
	default:
		return "", errors.New("no such info key: " + key)
	}
}

// func (proxy *RedisProxy) Promote(slave string) (err error) {
// 	if err := proxy.WriteServerObject([]byte("*1\r\n$4\r\nINFO\r\n")); err != nil {
// 		return err
// 	}

// 	info, err := proxy.ReadServerObject()
// 	if err != nil {
// 		return err
// 	}

// 	if proxy.getInfo(info, "master_repl_offset") != "0" {
// 		return errors.New("replication not ready")
// 	}

// 	slaveURL := "127.0.0.1:6380" // TODO: source this from env vars???
// 	slaveConn, err := net.Dial("tcp", slaveURL)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		if err != nil {
// 			slaveConn.Close()
// 		}
// 	}()

// 	if _, err := slaveConn.Write([]byte("*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n")); err != nil {
// 		return err
// 	}
// 	slaveReader := NewReader(slaveConn)
// 	response, err := slaveReader.ReadObject()
// 	if err != nil {
// 		return err
// 	}
// 	if response[0] == '-' {
// 		return errors.New(string(response[1 : len(response)-2]))
// 	}

// 	demotedConn := proxy.serverConn
// 	proxy.serverConn = slaveConn
// 	proxy.serverReader = slaveReader
// 	demotedConn.Close()

// 	return proxy.WriteClientObject(response)
// 	// return proxy.WriteClientObject([]byte("$14\r\nTODO: PROMOTED\r\n"))
// }

// func (proxy *RedisProxy) getInfo(info []byte, key string) string {
// 	return "0"
// }

// func (proxy *RedisProxy) Monitor() {
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		// Returns when clientConn is closed or EOF is sent
// 		io.Copy(proxy.serverConn, proxy.clientConn)
// 		// Necessary to stop server->client copy when client breaks connection
// 		proxy.serverConn.Close()
// 	}()
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		// Returns when serverConn is closed or EOF is sent
// 		io.Copy(proxy.clientConn, proxy.serverConn)
// 		// Necessary to stop client->server copy when server breaks connection.
// 		proxy.clientConn.Close()
// 	}()
// 	wg.Wait()
// }
