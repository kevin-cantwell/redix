package redix

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"sync"
)

// type RedisProxy struct {
// 	clientConn      net.Conn
// 	serverConn      net.Conn
// 	clientReader    *RESPReader
// 	serverReader    *RESPReader
// 	serverWriteLock sync.Mutex
// 	// Keep track of double writes to client (and double reads from server?)
// }

// func NewRedisProxy(clientConn, serverConn net.Conn) RedisProxy {
// 	return RedisProxy{
// 		clientConn:   clientConn,
// 		serverConn:   serverConn,
// 		clientReader: NewReader(clientConn),
// 		serverReader: NewReader(serverConn),
// 	}
// }

// func (proxy *RedisProxy) Println(msg string) {
// 	fmt.Println(proxy.clientConn.RemoteAddr().String(), "--", proxy.serverConn.RemoteAddr().String(), msg)
// }

// func (proxy *RedisProxy) Close() error {
// 	defer proxy.Println("connections closed")
// 	cerr := proxy.clientConn.Close()
// 	serr := proxy.serverConn.Close()
// 	if cerr != nil {
// 		return cerr
// 	}
// 	if serr != nil {
// 		return serr
// 	}
// 	return nil
// }

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

// func (proxy *RedisProxy) ReadClientObject() ([]byte, error) {
// 	return proxy.clientReader.ReadObject()
// }

// func (proxy *RedisProxy) WriteClientObject(resp []byte) error {
// 	fmt.Printf("%s <- %s %q\n", proxy.clientConn.RemoteAddr().String(), proxy.serverConn.RemoteAddr().String(), resp)
// 	_, err := proxy.clientConn.Write(resp)
// 	return err
// }

// func (proxy *RedisProxy) WriteClientErr(e error) error {
// 	resp := "-PROXYERR " + e.Error() + "\r\n"
// 	fmt.Printf("%s <- %s %q\n", proxy.clientConn.RemoteAddr().String(), proxy.serverConn.RemoteAddr().String(), resp)
// 	_, err := proxy.clientConn.Write([]byte(resp))
// 	return err
// }

// func (proxy *RedisProxy) ReadServerObject() ([]byte, error) {
// 	return proxy.serverReader.ReadObject()
// }

// func (proxy *RedisProxy) WriteServerObject(resp []byte) error {
// 	fmt.Printf("%s -> %s %q\n", proxy.clientConn.RemoteAddr().String(), proxy.serverConn.RemoteAddr().String(), resp)
// 	proxy.serverWriteLock.Lock()
// 	_, err := proxy.serverConn.Write([]byte(resp))
// 	proxy.serverWriteLock.Unlock()
// 	return err
// }

const (
	SIMPLE_STRING = '+'
	BULK_STRING   = '$'
	INTEGER       = ':'
	ARRAY         = '*'
	ERROR         = '-'
)

var (
	ErrInvalidSyntax = errors.New("resp: invalid syntax")

	masterWriteLock sync.Mutex // TODO: Make lock checking conditional on promote activity to allow concurrent writes
)

type RESPReader struct {
	*bufio.Reader
}

func NewReader(reader io.Reader) *RESPReader {
	return &RESPReader{
		Reader: bufio.NewReaderSize(reader, 32*1024), // 32KB is just a guess
	}
}

func (r *RESPReader) ReadObject() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case SIMPLE_STRING, INTEGER, ERROR:
		return line, nil
	case BULK_STRING:
		return r.readBulkString(line)
	case ARRAY:
		return r.readArray(line)
	default:
		return nil, ErrInvalidSyntax
	}
}

// In readLine(), we read up until the first occurrence of \n and
// then check to make sure that it was preceded by a \r before returning the line as a byte slice.
func (r *RESPReader) readLine() (line []byte, err error) {
	line, err = r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	if len(line) > 1 && line[len(line)-2] == '\r' {
		return line, nil
	} else {
		// Line was too short or \n wasn't preceded by \r.
		return nil, ErrInvalidSyntax
	}
}

// In readBulkString() we parse the length specification for the bulk string to know how many
// bytes we need to read. Once we do, we read that count of bytes and the \r\n line terminator
func (r *RESPReader) readBulkString(line []byte) ([]byte, error) {
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return line, nil
	}

	buf := make([]byte, len(line)+count+2)
	copy(buf, line)
	_, err = r.Read(buf[len(line):])
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// To handle arrays, we get the number of array elements, and then call ReadObject()
// recursively, adding the resulting objects to our current RESP buffer
func (r *RESPReader) readArray(line []byte) ([]byte, error) {
	// Get number of array elements.
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}

	// Read `count` number of RESP objects in the array.
	for i := 0; i < count; i++ {
		buf, err := r.ReadObject()
		if err != nil {
			return nil, err
		}
		line = append(line, buf...)
	}

	return line, nil
}

func (r *RESPReader) getCount(line []byte) (int, error) {
	end := bytes.IndexByte(line, '\r')
	return strconv.Atoi(string(line[1:end]))
}
