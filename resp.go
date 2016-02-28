package redix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
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

func (r *RESPReader) getCount(line []byte) (int, error) {
	end := bytes.IndexByte(line, '\r')
	return strconv.Atoi(string(line[1:end]))
}

// ParseRESP accepts a resp object and returns its
// component strings. Null Bulk Strings result in an
// array of length 1 where the first element is a nil byte slice.
// Arrays are the only type that may return a slice of length > 1.
// Null Arrays return nil.
//
// ParseRESP may not be used to parse multiple RESP objects.
func ParseRESP(resp []byte) ([][]byte, error) {
	if len(resp) < 1 {
		return nil, fmt.Errorf("resp: %q contains invalid prefix", resp)
	}
	switch resp[0] {
	case IntegerPrefix:
		integer, err := parseAsInteger(resp)
		if err != nil {
			return nil, err
		}
		return [][]byte{integer}, nil
	case SimpleStringPrefix, ErrorPrefix:
		ss, err := parseAsSimpleString(resp)
		if err != nil {
			return nil, err
		}
		return [][]byte{ss}, nil
	case BulkStringPrefix:
		bs, err := parseAsBulkString(resp)
		if err != nil {
			return nil, err
		}
		return [][]byte{bs}, nil
	case ArrayPrefix:
		return parseArray(resp)
	default:
		return nil, fmt.Errorf("resp: %q contains invalid prefix", resp)
	}
}

// http://redis.io/topics/protocol:
// "the returned integer is guaranteed to be in the range of a signed 64 bit integer"
func parseAsInteger(resp []byte) ([]byte, error) {
	// Must be at least 4 chars ":5\r\n"
	if len(resp) < 4 {
		return nil, fmt.Errorf("resp: %q must contain a value and end with CRLF", resp)
	}
	if resp[len(resp)-2] != '\r' && resp[len(resp)-1] != '\n' {
		return nil, fmt.Errorf("resp: %q must be terminated with CRLF", resp)
	}
	// Allow negation symbol with zero (not defined in spec, we just allow it)
	if string(resp) == ":-0\r\n" {
		return []byte("0"), nil
	}
	integer := resp[1 : len(resp)-2]
	if _, err := strconv.ParseInt(string(integer), 10, 64); err != nil {
		return nil, fmt.Errorf("resp: %q is not a 64 bit integer", resp)
	}
	return integer, nil
}

func parseAsSimpleString(resp []byte) ([]byte, error) {
	// Must be at least 3 chars "+\r\n" or "-\r\n"
	if len(resp) < 3 {
		return nil, fmt.Errorf("resp: %q must start with prefix and end with CRLF", resp)
	}
	if resp[len(resp)-2] != '\r' && resp[len(resp)-1] != '\n' {
		return nil, fmt.Errorf("resp: %q must be terminated with CRLF", resp)
	}
	simpleString := resp[1 : len(resp)-2]
	for _, c := range simpleString {
		if c == '\r' || c == '\n' {
			return nil, fmt.Errorf("resp: %q may not contain CR or LF", resp)
		}
	}
	return simpleString, nil
}

func parseAsBulkString(resp []byte) ([]byte, error) {
	// Handle Null Bulk Strings
	if string(resp) == "$-1\r\n" {
		return nil, nil
	}
	// Must be at least 6 chars "$X\r\n\r\n"
	if len(resp) < 6 {
		return nil, fmt.Errorf("resp: %q must indicate both a length and a value", resp)
	}
	if resp[len(resp)-2] != '\r' && resp[len(resp)-1] != '\n' {
		return nil, fmt.Errorf("resp: %q must be terminated with CRLF", resp)
	}

	i := 1
	// Calculate the length
	var l []byte
	for ; resp[i] != '\r' && resp[i+1] != '\n'; i++ {
		l = append(l, resp[i])
	}
	expectedLength, err := strconv.Atoi(string(l))
	if err != nil {
		return nil, fmt.Errorf("resp: %q must specify an integer length", resp)
	}

	// Calculate the bulk string
	bulkString := resp[i+2 : len(resp)-2]
	if len(bulkString) != expectedLength {
		return nil, fmt.Errorf("resp: %q incorrect length", resp)
	}
	return bulkString, nil
}

func parseArray(resp []byte) ([][]byte, error) {
	// A client library API should return a null object and not an empty Array
	// when Redis replies with a Null Array. This is necessary to distinguish
	// between an empty list and a different condition (for instance the timeout condition of the BLPOP command).
	if string(resp) == "*-1\r\n" {
		return nil, nil
	}
	// Must be at least 4 chars empty array "*0\r\n"
	if len(resp) < 4 {
		return nil, fmt.Errorf("resp: %q must contain a collection and end with CRLF", resp)
	}
	if resp[len(resp)-2] != '\r' && resp[len(resp)-1] != '\n' {
		return nil, fmt.Errorf("resp: %q must be terminated with CRLF", resp)
	}

	i := 1
	// Calculate the count
	var c []byte
	for ; resp[i] != '\r' && resp[i+1] != '\n'; i++ {
		c = append(c, resp[i])
	}
	count, err := strconv.Atoi(string(c))
	if err != nil {
		return nil, fmt.Errorf("resp: %q must specify an integer count", resp)
	}

	// Parse each resp element in the array
	elements := [][]byte{}
	var countToNext int
	for i = i + 2; i < len(resp); i += countToNext {
		countToNext = bytes.IndexAny(resp[i+1:], ":+-$")
		if countToNext == -1 {
			countToNext = len(resp[i+1:])
		}
		countToNext++ // Since we are counting with an offset of +1
		elem, err := ParseRESP(resp[i : i+countToNext])
		if err != nil {
			return nil, err
		}
		elements = append(elements, elem[0])
	}

	if len(elements) != count {
		return nil, fmt.Errorf("resp: %q wrong number of elements", resp)
	}

	return elements, nil
}
