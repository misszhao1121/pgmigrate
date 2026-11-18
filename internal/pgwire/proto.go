package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

const (
	protocolVersion   = 196608 // 3.0
	sslRequestCode    = 80877103
	cancelRequestCode = 80877102

	maxMessageSize = 1 << 30 // 1GB, protocol allows large COPY messages
)

// PgError is a parsed ErrorResponse from the server.
type PgError struct {
	Severity string
	Code     string
	Message  string
	Detail   string
	Hint     string
	Position string
	Where    string
	Schema   string
	Table    string
	Column   string
}

func (e *PgError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s (SQLSTATE %s)", e.Severity, e.Message, e.Code)
	if e.Detail != "" {
		fmt.Fprintf(&b, "\n  详细: %s", e.Detail)
	}
	if e.Hint != "" {
		fmt.Fprintf(&b, "\n  提示: %s", e.Hint)
	}
	if e.Where != "" {
		fmt.Fprintf(&b, "\n  位置: %s", e.Where)
	}
	if e.Position != "" {
		fmt.Fprintf(&b, "\n  字符位置: %s", e.Position)
	}
	return b.String()
}

// FieldDesc describes one column of a result set.
type FieldDesc struct {
	Name     string
	TableOID uint32
	Col      int16
	TypeOID  uint32
	Len      int16
	Mod      int32
	Fmt      int16
}

// Row is one result row; Vals[i]==nil means SQL NULL.
type Row struct {
	Vals []*string
}

// Result holds a complete result set of a simple query.
type Result struct {
	Fields []FieldDesc
	Rows   []Row
	Tag    string // last CommandComplete tag
}

// Col returns the string value of column by index; NULL becomes "".
func (r *Result) Col(rowIdx, colIdx int) string {
	if rowIdx >= len(r.Rows) || colIdx >= len(r.Rows[rowIdx].Vals) {
		return ""
	}
	v := r.Rows[rowIdx].Vals[colIdx]
	if v == nil {
		return ""
	}
	return *v
}

// ColPtr returns raw value pointer (nil for NULL).
func (r *Result) ColPtr(rowIdx, colIdx int) *string {
	if rowIdx >= len(r.Rows) || colIdx >= len(r.Rows[rowIdx].Vals) {
		return nil
	}
	return r.Rows[rowIdx].Vals[colIdx]
}

// FieldIndex returns the index of the named column, or -1.
func (r *Result) FieldIndex(name string) int {
	for i, f := range r.Fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// readMsg reads one backend message; returns type byte and payload.
func (c *Conn) readMsg() (byte, []byte, error) {
	t, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(c.r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n < 4 {
		return 0, nil, fmt.Errorf("pgwire: 非法消息长度 %d", n)
	}
	n -= 4
	if n > maxMessageSize {
		return 0, nil, fmt.Errorf("pgwire: 消息过大 %d 字节", n)
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return 0, nil, err
		}
	}
	return t, buf, nil
}

// writeMsg writes one frontend message (with type byte + length header).
func (c *Conn) writeMsg(t byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = t
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)+4))
	if _, err := c.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.w.Write(payload); err != nil {
			return err
		}
	}
	return c.w.Flush()
}

// writeNoFlush writes a message without flushing (for batching).
func (c *Conn) writeNoFlush(t byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = t
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)+4))
	if _, err := c.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		_, err := c.w.Write(payload)
		return err
	}
	return nil
}

func (c *Conn) flush() error { return c.w.Flush() }

// writeStartup writes the StartupMessage (no type byte).
func (c *Conn) writeStartup(params map[string]string) error {
	var b []byte
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, protocolVersion)
	b = append(b, tmp...)
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	// stable order: user, database first
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, 0)
		b = append(b, params[k]...)
		b = append(b, 0)
	}
	b = append(b, 0)
	total := make([]byte, 4)
	binary.BigEndian.PutUint32(total, uint32(len(b)+4))
	if _, err := c.w.Write(total); err != nil {
		return err
	}
	_, err := c.w.Write(b)
	if err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *Conn) writeCancel(pid, key uint32) error {
	var b [16]byte
	binary.BigEndian.PutUint32(b[0:4], 16)
	binary.BigEndian.PutUint32(b[4:8], cancelRequestCode)
	binary.BigEndian.PutUint32(b[8:12], pid)
	binary.BigEndian.PutUint32(b[12:16], key)
	if _, err := c.w.Write(b[:]); err != nil {
		return err
	}
	return c.w.Flush()
}

// ---- payload parsers ----

func readCString(b []byte, off int) (string, int, error) {
	end := -1
	for i := off; i < len(b); i++ {
		if b[i] == 0 {
			end = i
			break
		}
	}
	if end < 0 {
		return "", off, fmt.Errorf("pgwire: 消息中缺少结束符")
	}
	return string(b[off:end]), end + 1, nil
}

func parseErrorResponse(b []byte) *PgError {
	e := &PgError{}
	off := 0
	for off < len(b) {
		code := b[off]
		off++
		if code == 0 {
			break
		}
		v, noff, err := readCString(b, off)
		if err != nil {
			break
		}
		off = noff
		switch code {
		case 'S':
			e.Severity = v
		case 'C':
			e.Code = v
		case 'M':
			e.Message = v
		case 'D':
			e.Detail = v
		case 'H':
			e.Hint = v
		case 'P':
			e.Position = v
		case 'W':
			e.Where = v
		case 's':
			e.Schema = v
		case 't':
			e.Table = v
		case 'c':
			e.Column = v
		}
	}
	return e
}

func parseRowDescription(b []byte) []FieldDesc {
	if len(b) < 2 {
		return nil
	}
	n := int(int16(binary.BigEndian.Uint16(b[0:2])))
	fields := make([]FieldDesc, 0, n)
	off := 2
	for i := 0; i < n && off < len(b); i++ {
		name, noff, err := readCString(b, off)
		if err != nil {
			break
		}
		off = noff
		var f FieldDesc
		f.Name = name
		f.TableOID = binary.BigEndian.Uint32(b[off:])
		f.Col = int16(binary.BigEndian.Uint16(b[off+4:]))
		f.TypeOID = binary.BigEndian.Uint32(b[off+6:])
		f.Len = int16(binary.BigEndian.Uint16(b[off+10:]))
		f.Mod = int32(binary.BigEndian.Uint32(b[off+12:]))
		f.Fmt = int16(binary.BigEndian.Uint16(b[off+16:]))
		off += 18
		fields = append(fields, f)
	}
	return fields
}

func parseDataRow(b []byte) Row {
	if len(b) < 2 {
		return Row{}
	}
	n := int(int16(binary.BigEndian.Uint16(b[0:2])))
	row := Row{Vals: make([]*string, 0, n)}
	off := 2
	for i := 0; i < n && off+4 <= len(b); i++ {
		l := int32(binary.BigEndian.Uint32(b[off:]))
		off += 4
		if l < 0 {
			row.Vals = append(row.Vals, nil) // NULL
			continue
		}
		if off+int(l) > len(b) {
			break
		}
		s := string(b[off : off+int(l)])
		row.Vals = append(row.Vals, &s)
		off += int(l)
	}
	return row
}

func parseCopyResponse(b []byte) (overall int8, colFmts []int8, err error) {
	if len(b) < 3 {
		return 0, nil, fmt.Errorf("pgwire: COPY 响应过短")
	}
	overall = int8(b[0])
	n := int(int16(binary.BigEndian.Uint16(b[1:3])))
	if len(b) < 3+n {
		return 0, nil, fmt.Errorf("pgwire: COPY 响应格式错误")
	}
	colFmts = make([]int8, n)
	for i := 0; i < n; i++ {
		colFmts[i] = int8(b[3+i])
	}
	return overall, colFmts, nil
}
