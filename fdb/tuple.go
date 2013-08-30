// FoundationDB Go API
// Copyright (c) 2013 FoundationDB, LLC

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package fdb

import (
	"fmt"
	"encoding/binary"
	"bytes"
)

type Tuple []interface{}

var sizeLimits = []uint64{
	1 << (0 * 8) - 1,
	1 << (1 * 8) - 1,
	1 << (2 * 8) - 1,
	1 << (3 * 8) - 1,
	1 << (4 * 8) - 1,
	1 << (5 * 8) - 1,
	1 << (6 * 8) - 1,
	1 << (7 * 8) - 1,
	1 << (8 * 8) - 1,
}

func encodeBytes(buf *bytes.Buffer, b []byte) {
	buf.WriteByte(0x01)
	buf.Write(bytes.Replace(b, []byte{0x00}, []byte{0x00, 0xff}, -1))
	buf.WriteByte(0x00)
}

func encodeString(buf *bytes.Buffer, s string) {
	buf.WriteByte(0x02)
	buf.Write(bytes.Replace([]byte(s), []byte{0x00}, []byte{0x00, 0xff}, -1))
	buf.WriteByte(0x00)
}

func bisectLeft(u uint64) int {
	var n int
	for sizeLimits[n] < u {
		n += 1
	}
	return n
}

func encodeInt(buf *bytes.Buffer, i int64) {
	if i == 0 {
		buf.WriteByte(0x14)
		return
	}

	var n int
	var ibuf bytes.Buffer

	switch {
	case i > 0:
		n = bisectLeft(uint64(i))
		buf.WriteByte(byte(0x14+n))
		binary.Write(&ibuf, binary.BigEndian, i)
	case i < 0:
		n = bisectLeft(uint64(-i))
		buf.WriteByte(byte(0x14-n))
		binary.Write(&ibuf, binary.BigEndian, int64(sizeLimits[n])+i)
	}

	buf.Write(ibuf.Bytes()[8-n:])
}

func (a *api) Pack(t Tuple) ([]byte, error) {
	buf := new(bytes.Buffer)

	for i, e := range(t) {
		switch e := e.(type) {
		case nil:
			buf.WriteByte(0x00)
		case int64:
			encodeInt(buf, e)
		case string:
			encodeString(buf, e)
		case []byte:
			encodeBytes(buf, e)
		default:
			return []byte{}, fmt.Errorf("Unencodable type at index %d", i)
		}
	}

	return buf.Bytes(), nil
}

func findTerminator(b []byte) int {
	bp := b
	var length int

	for {
		idx := bytes.IndexByte(bp, 0x00)
		length += idx
		if idx + 1 == len(bp) || bp[idx+1] != 0xff {
			break
		}
		length += 2
		bp = bp[idx+2:]
	}

	return length
}

func decodeBytes(b []byte) ([]byte, int) {
	idx := findTerminator(b[1:])
	return bytes.Replace(b[1:idx+1], []byte{0x00, 0xff}, []byte{0x00}, -1), idx + 2
}

func decodeString(b []byte) (string, int) {
	bp, idx := decodeBytes(b)
	return string(bp), idx
}

func decodeInt(b []byte) (int64, int) {
	if b[0] == 0x14 {
		return 0, 1
	}

	var neg bool

	n := int(b[0]) - 20
	if n < 0 {
		n = -n
		neg = true
	}

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret int64

	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	if neg {
		ret -= int64(sizeLimits[n])
	}

	return ret, n+1
}

func (a *api) Unpack(b []byte) (Tuple, error) {
	var t Tuple

	var i int

	for i < len(b) {
		var el interface{}
		var off int

		switch {
		case b[i] == 0x00:
			el = nil
			off = 1
		case b[i] == 0x01:
			el, off = decodeBytes(b[i:])
		case b[i] == 0x02:
			el, off = decodeString(b[i:])
		case 0x0c <= b[i] && b[i] <= 0x1c:
			el, off = decodeInt(b[i:])
		default:
			return nil, fmt.Errorf("Can't decode tuple typecode %02x", b[i])
		}

		t = append(t, el)
		i += off
	}

	return t, nil
}
