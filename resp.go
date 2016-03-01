package redix

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

const (
	IntegerPrefix      = ':'
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
)

var (
	ErrInvalidSyntax = errors.New("resp: invalid syntax")
	CRLF             = []byte{'\r', '\n'}
)

type Integer []byte
type SimpleString []byte
type Error []byte
type BulkString []byte
type Array []Resp

type Resp interface {
	// Prefix() byte
	HumanReadable() string
	Raw() []byte
	String() string
}

// func (Integer) Prefix() byte      { return IntegerPrefix }
// func (SimpleString) Prefix() byte { return SimpleStringPrefix }
// func (Error) Prefix() byte        { return ErrorPrefix }
// func (BulkString) Prefix() byte   { return BulkStringPrefix }
// func (Array) Prefix() byte        { return ArrayPrefix }

func (resp Integer) HumanReadable() string      { return fmt.Sprintf("%q", resp) }
func (resp SimpleString) HumanReadable() string { return fmt.Sprintf("%q", resp) }
func (resp Error) HumanReadable() string        { return fmt.Sprintf("%q", resp) }
func (resp BulkString) HumanReadable() string   { return fmt.Sprintf("%q", resp) }
func (resp Array) HumanReadable() string {
	s := ""
	for i, r := range resp {
		s += fmt.Sprintf("%q", r)
		if i < len(resp)-1 {
			s += " "
		}
	}
	return s
}

func (resp Integer) String() string      { return string(resp) }
func (resp SimpleString) String() string { return string(resp) }
func (resp Error) String() string        { return string(resp) }
func (resp BulkString) String() string   { return string(resp) }
func (resp Array) String() string {
	s := "["
	for i, r := range resp {
		s += r.String()
		if i < len(resp)-1 {
			s += " "
		}
	}
	return s + "]"
}

func (resp Integer) Raw() []byte      { return raw(':', []byte(resp)) }
func (resp SimpleString) Raw() []byte { return raw('+', []byte(resp)) }
func (resp Error) Raw() []byte        { return raw('-', []byte(resp)) }
func (resp BulkString) Raw() []byte   { return raw('$', []byte(fmt.Sprint(len(resp))), []byte(resp)) }
func (resp Array) Raw() []byte {
	r := []byte(fmt.Sprintf("*%d\r\n", len(resp)))
	for _, elem := range resp {
		r = append(r, elem.Raw()...)
	}
	return r
}

func raw(prefix byte, vals ...[]byte) []byte {
	r := []byte{prefix}
	for _, val := range vals {
		r = append(r, val...)
		r = append(r, '\r', '\n')
	}
	return r
}

type RESPReader struct {
	*bufio.Reader
}

func NewReader(reader io.Reader) *RESPReader {
	return &RESPReader{
		Reader: bufio.NewReaderSize(reader, 128*1024),
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
func (r *RESPReader) ParseObject() (Resp, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case IntegerPrefix:
		return r.parseInteger(line)
	case SimpleStringPrefix:
		return r.parseSimpleString(line)
	case ErrorPrefix:
		return r.parseError(line)
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
func (r *RESPReader) readLine() ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrInvalidSyntax
	}
	return line, nil
}

// In readBulkString() we parse the length specification for the bulk string to know how many
// bytes we need to read. Once we do, we read that count of bytes and the \r\n line terminator
func (r *RESPReader) readBulkString(line []byte) ([]byte, error) {
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return nil, nil
	}

	buf := make([]byte, len(line)+count+2)
	copy(buf, line)
	if _, err := io.ReadFull(r, buf[len(line):]); err != nil {
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

func (r *RESPReader) parseInteger(line []byte) (Integer, error) {
	n := line[1 : len(line)-2]
	for _, b := range n {
		if b != '-' && b < '0' || b > '9' {
			return nil, ErrInvalidSyntax
		}
	}
	return Integer(n), nil
}

func (r *RESPReader) parseError(line []byte) (Error, error) {
	e, err := r.parseSimpleString(line)
	if err != nil {
		return nil, err
	}
	return Error(e), nil
}

func (r *RESPReader) parseSimpleString(line []byte) (SimpleString, error) {
	ss := line[1 : len(line)-2]
	for _, c := range ss {
		if c == '\r' || c == '\n' {
			return nil, ErrInvalidSyntax
		}
	}
	return SimpleString(ss), nil
}

// In readBulkString() we parse the length specification for the bulk string to know how many
// bytes we need to read. Once we do, we read that count of bytes and the \r\n line terminator
func (r *RESPReader) parseBulkString(line []byte) (BulkString, error) {
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	// Null Bulk String
	if count == -1 {
		return nil, nil
	}

	buf := make([]byte, count)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	crlf := make([]byte, 2)
	if _, err = io.ReadFull(r, crlf); err != nil {
		return nil, err
	}

	// Make sure the count specifies a valid stopping point
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, ErrInvalidSyntax
	}

	return BulkString(buf), nil
}

// To handle arrays, we get the number of array elements, and then call ParseObject()
// recursively, adding the resulting objects to our current buffer
func (r *RESPReader) parseArray(line []byte) (Array, error) {
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
		return Array{}, nil
	}

	var array Array
	// Read `count` number of objects in the array.
	for i := 0; i < count; i++ {
		resp, err := r.ParseObject()
		if err != nil {
			return nil, err
		}
		array = append(array, resp)
	}

	return array, nil
}

// line is gauranteed to begin with a prefix and end with CRLF
func (r *RESPReader) getCount(line []byte) (int, error) {
	if len(line) == 0 {
		return -1, ErrInvalidSyntax
	}

	if len(line) == 5 && line[1] == '-' && line[2] == '1' {
		// handle $-1 null replies.
		return -1, nil
	}

	// Credit goes to redigo for this logic
	var n int
	for _, b := range line[1 : len(line)-2] {
		n *= 10
		if b < '0' || b > '9' {
			return -1, ErrInvalidSyntax
		}
		n += int(b - '0')
	}

	return n, nil
}
