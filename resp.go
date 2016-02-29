package redix

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"sync"
)

const (
	IntegerPrefix      = ':'
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
)

var (
	CRLF = []byte("\r\n")

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

// ReadObject will attempt to parse the input as a RESP
// object.
func (r *RESPReader) ReadObject() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case SimpleStringPrefix, IntegerPrefix, ErrorPrefix:
		return line, nil
	case BulkStringPrefix:
		return r.readBulkString(line)
	case ArrayPrefix:
		return r.readArray(line)
	default:
		return nil, ErrInvalidSyntax
	}
}

// ParseObject will attempt to parse the input as a RESP
// object and further parse the object into its components
func (r *RESPReader) ParseObject() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case IntegerPrefix:
		return r.parseInteger(line)
	case SimpleStringPrefix, ErrorPrefix:
		return r.parseSimpleString(line)
	case BulkStringPrefix:
		return r.parseBulkString(line)
	case ArrayPrefix:
		return r.parseArray(line)
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
// recursively, adding the resulting objects to our current buffer
func (r *RESPReader) readArray(line []byte) ([]byte, error) {
	// Get number of array elements.
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}

	// Read `count` number of objects in the array.
	for i := 0; i < count; i++ {
		buf, err := r.ReadObject()
		if err != nil {
			return nil, err
		}
		line = append(line, buf...)
	}

	return line, nil
}

func (r *RESPReader) parseInteger(line []byte) ([][]byte, error) {
	val := line[1 : len(line)-2]
	valStr := string(val)
	if _, err := strconv.ParseInt(valStr, 10, 64); err != nil {
		return nil, ErrInvalidSyntax
	}
	// Just clean up negative zero
	if valStr == "-0" {
		return [][]byte{[]byte(`0`)}, nil
	} else {
		return [][]byte{val}, nil
	}
}

func (r *RESPReader) parseSimpleString(line []byte) ([][]byte, error) {
	val := line[1 : len(line)-2]
	for _, c := range val {
		if c == '\r' || c == '\n' {
			return nil, ErrInvalidSyntax
		}
	}
	return [][]byte{val}, nil
}

// In readBulkString() we parse the length specification for the bulk string to know how many
// bytes we need to read. Once we do, we read that count of bytes and the \r\n line terminator
func (r *RESPReader) parseBulkString(line []byte) ([][]byte, error) {
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	// Null Bulk String
	if count == -1 {
		return [][]byte{nil}, nil
	}

	buf := make([]byte, count)
	// Read count bytes into buf
	_, err = r.Read(buf)
	if err != nil {
		return nil, err
	}

	crlf := make([]byte, 2)
	_, err = r.Read(crlf)
	if err != nil {
		return nil, err
	}
	// Make sure the count specifies a valid stopping point
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, ErrInvalidSyntax
	}

	return [][]byte{buf}, nil
}

// To handle arrays, we get the number of array elements, and then call ParseObject()
// recursively, adding the resulting objects to our current buffer
func (r *RESPReader) parseArray(line []byte) ([][]byte, error) {
	// Get number of array elements.
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	// Null Array
	if count == -1 {
		return nil, nil
	}
	// Empty Array
	if count == 0 {
		return [][]byte{}, nil
	}

	var array [][]byte
	// Read `count` number of objects in the array.
	for i := 0; i < count; i++ {
		parsed, err := r.ParseObject()
		if err != nil {
			return nil, err
		}
		array = append(array, parsed...)
	}

	return array, nil
}

// line is gauranteed to begin with a prefix and end with CRLF
func (r *RESPReader) getCount(line []byte) (int, error) {
	end := bytes.IndexByte(line, '\r')
	if count, err := strconv.Atoi(string(line[1:end])); err != nil {
		return 0, ErrInvalidSyntax
	} else {
		return count, nil
	}
}
