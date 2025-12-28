package core

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"mtredis/internal/constant"
	"strings"
)

const CRLF string = "\r\n"

// +OK\r\n => "OK", 5
func decodeSimpleString(data []byte) (string, int, error) {
	var pos int = 1

	for data[pos] != '\r' {
		pos++
	}

	return string(data[1:pos]), pos + 2, nil
}

// :123\r\n => 123, 5
func decodeInt64(data []byte) (int64, int, error) {
	var res int64 = 0
	var pos int = 1
	var sign int64 = 1

	if data[pos] == '-' {
		sign = -1
		pos++
	}
	if data[pos] == '+' {
		pos++
	}

	for data[pos] != '\r' {
		res = res*10 + int64(data[pos]-'0')
		pos++
	}

	return sign * res, pos + 2, nil
}

func decodeError(data []byte) (string, int, error) {
	return decodeSimpleString(data)
}

// $5\r\nhello\r\n => 5, 4
func findLen(data []byte) (int, int) {
	res, pos, _ := decodeInt64(data)

	return int(res), pos
}

// $5\r\nhello\r\n => "hello", 11
func decodeBulkString(data []byte) (string, int, error) {
	length, pos := findLen(data)

	return string(data[pos : pos+length]), pos + length + 2, nil
}

// *2\r\n*2\r\r:1\r\n:-2\r\n$5\r\nhello\r\n => [[1, -2], "hello"]
func decodeArray(data []byte) (interface{}, int, error) {
	length, pos := findLen(data)
	var res []interface{} = make([]interface{}, length)

	for i := range res {
		elem, delta, err := DecodeOne(data[pos:])
		if err != nil {
			log.Printf("failed to decode data: %v", err)
			return nil, 0, err
		}
		res[i] = elem
		pos += delta
	}

	return res, pos, nil
}

func DecodeOne(data []byte) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("no data")
	}

	switch data[0] {
	case '+':
		return decodeSimpleString(data)
	case '-':
		return decodeError(data)
	case ':':
		return decodeInt64(data)
	case '$':
		return decodeBulkString(data)
	case '*':
		return decodeArray(data)
	}

	return nil, 0, nil
}

// RESP format data => raw data
func Decode(data []byte) (interface{}, error) {
	res, _, err := DecodeOne(data)

	return res, err
}

// // "OK" => +OK\r\n
// func encodeSimpleString(s string) []byte {
// 	return []byte(fmt.Sprintf("+%s\r\n", s))
// }

// "hello" => $5\r\nhello\r\n
func encodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// 123 => :123\r\n
func encodeInt64(i int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

func encodeError(s string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", s))
}

// ["hello", "engineer"] => "*2\r\n$5\r\nhello\r\n$8\r\nengineer\r\n"
func encodeStringArray(sa []string) []byte {
	var buf bytes.Buffer
	for _, s := range sa {
		buf.Write(encodeBulkString(s))
	}
	return []byte(fmt.Sprintf("*%d%s%s", len(sa), CRLF, buf.Bytes()))
}

// [[1, -2], "hello"] => *2\r\n*2\r\r:1\r\n:-2\r\n$5\r\nhello\r\n
func encodeArray(arr []interface{}) []byte {
	var buf bytes.Buffer
	for _, v := range arr {
		buf.Write(EncodeOne(v))
	}
	return []byte(fmt.Sprintf("*%d%s%s", len(arr), CRLF, buf.Bytes()))
}

func EncodeOne(value interface{}) []byte {
	switch v := value.(type) {
	case string:
		// bulk string by default
		return encodeBulkString(v)
	case int64:
		return encodeInt64(v)
	case error:
		return encodeError(v.Error())
	case []string:
		return encodeStringArray(v)
	case []interface{}:
		return encodeArray(v)
	default:
		return constant.RespNil
	}
}

func Encode(value interface{}) []byte {
	if value == nil {
		return constant.RespNil
	}

	data := EncodeOne(value)
	if data == nil {
		return nil
	}

	return data
}

func ParseCmd(data []byte) (*Command, error) {
	value, err := Decode(data)
	if err != nil {
		log.Printf("can not decode resp data: %v", err)
		return nil, err
	}

	array := value.([]interface{})
	tokens := make([]string, len(array))
	for i := range tokens {
		tokens[i] = array[i].(string)
	}

	res := &Command{
		Cmd:  strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	}

	return res, nil
}
