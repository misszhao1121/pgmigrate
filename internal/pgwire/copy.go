package pgwire

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
)

// CopyTo runs "COPY ... TO STDOUT" and streams result bytes to w.
// Data arrives as CopyData ('d') messages until CopyDone ('c').
func (c *Conn) CopyTo(ctx context.Context, sql string, w io.Writer) error {
	if c.closed {
		return errors.New("pgwire: 连接已关闭")
	}
	if err := c.writeMsg('Q', append([]byte(sql), 0)); err != nil {
		return fmt.Errorf("发送 COPY 查询失败: %w", err)
	}
	sawResponse := false
	tag := ""
	for {
		if err := ctx.Err(); err != nil {
			_ = c.Cancel()
			c.drainToReady()
			return err
		}
		t, payload, err := c.readMsg()
		if err != nil {
			return fmt.Errorf("COPY 输出流中断: %w", err)
		}
		switch t {
		case 'H': // CopyOutResponse
			if _, _, err := parseCopyResponse(payload); err != nil {
				return err
			}
			sawResponse = true
		case 'd': // CopyData
			if !sawResponse {
				return errors.New("pgwire: 在 COPY 响应前收到数据")
			}
			if _, err := w.Write(payload); err != nil {
				// downstream failed; abort server side
				_ = c.writeMsg('f', []byte("下游写入失败: "+err.Error()))
				c.drainToReady()
				return fmt.Errorf("写入下游失败: %w", err)
			}
		case 'c': // CopyDone
		case 'C': // CommandComplete
			if s, _, err := readCString(payload, 0); err == nil {
				tag = s
			}
		case 'E':
			e := parseErrorResponse(payload)
			c.drainToReady()
			return e
		case 'Z': // ReadyForQuery
			_ = tag
			return nil
		case 'N':
		default:
		}
	}
}

// CopyFrom runs "COPY ... FROM STDIN", streaming r into the server.
// Returns the command tag, e.g. "COPY 12345".
func (c *Conn) CopyFrom(ctx context.Context, sql string, r io.Reader) (string, error) {
	if c.closed {
		return "", errors.New("pgwire: 连接已关闭")
	}
	if err := c.writeMsg('Q', append([]byte(sql), 0)); err != nil {
		return "", fmt.Errorf("发送 COPY 查询失败: %w", err)
	}
	// wait for CopyInResponse
	got := false
	for !got {
		if err := ctx.Err(); err != nil {
			_ = c.Cancel()
			c.drainToReady()
			return "", err
		}
		t, payload, err := c.readMsg()
		if err != nil {
			return "", fmt.Errorf("等待 COPY 确认失败: %w", err)
		}
		switch t {
		case 'G':
			if _, _, err := parseCopyResponse(payload); err != nil {
				return "", err
			}
			got = true
		case 'E':
			e := parseErrorResponse(payload)
			c.drainToReady()
			return "", e
		case 'Z':
			return "", errors.New("pgwire: 服务器拒绝 COPY（未收到 CopyInResponse）")
		case 'N':
		}
	}

	buf := make([]byte, 512*1024)
	br := bufio.NewReaderSize(r, 1024*1024)
	for {
		if err := ctx.Err(); err != nil {
			_ = c.writeMsg('f', []byte("上下文取消"))
			c.drainToReady()
			return "", err
		}
		n, rerr := br.Read(buf)
		if n > 0 {
			if err := c.writeNoFlush('d', buf[:n]); err != nil {
				return "", fmt.Errorf("发送 COPY 数据失败: %w", err)
			}
			if err := c.flush(); err != nil {
				return "", fmt.Errorf("发送 COPY 数据失败: %w", err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = c.writeMsg('f', []byte("读取源数据失败: "+rerr.Error()))
			c.drainToReady()
			return "", fmt.Errorf("读取源数据失败: %w", rerr)
		}
	}
	if err := c.writeMsg('c', nil); err != nil { // CopyDone
		return "", fmt.Errorf("发送 CopyDone 失败: %w", err)
	}
	tag := ""
	for {
		if err := ctx.Err(); err != nil {
			_ = c.Cancel()
			c.drainToReady()
			return tag, err
		}
		t, payload, err := c.readMsg()
		if err != nil {
			return tag, fmt.Errorf("等待 COPY 完成失败: %w", err)
		}
		switch t {
		case 'C':
			if s, _, e := readCString(payload, 0); e == nil {
				tag = s
			}
		case 'E':
			e := parseErrorResponse(payload)
			c.drainToReady()
			return tag, e
		case 'Z':
			return tag, nil
		case 'N':
		default:
		}
	}
}
