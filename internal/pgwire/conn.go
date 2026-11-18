package pgwire

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// Conn is a single PostgreSQL connection.
// It is NOT safe for concurrent use of Query/Exec/Copy* on the same Conn;
// the migrate engine uses one connection per concurrent role.
type Conn struct {
	nc     net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	cfg    *Config
	pid    uint32
	secret uint32
	params map[string]string
	closed bool
}

// Connect dials and authenticates.
func Connect(ctx context.Context, cfg *Config) (*Conn, error) {
	if cfg.AppName == "" {
		cfg.AppName = "pgmigrate"
	}
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	host := cfg.Host
	// TCP port notation host:port already handled by cfg.Port; strip brackets for IPv6
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	nc, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, fmt.Sprint(cfg.Port)))
	if err != nil {
		return nil, fmt.Errorf("连接 %s:%d 失败: %w", cfg.Host, cfg.Port, err)
	}

	useTLS := false
	switch cfg.SSLMode {
	case "disable":
	case "prefer", "require", "verify-full":
		ok, err := negotiateSSL(nc)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("SSL 协商失败: %w", err)
		}
		if ok {
			tcfg := &tls.Config{ServerName: host}
			switch cfg.SSLMode {
			case "require":
				tcfg.InsecureSkipVerify = true
			case "verify-full":
				// system roots + verify hostname
			}
			tc := tls.Client(nc, tcfg)
			hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := tc.HandshakeContext(hctx); err != nil {
				nc.Close()
				return nil, fmt.Errorf("TLS 握手失败: %w", err)
			}
			nc = tc
			useTLS = true
		} else if cfg.SSLMode == "require" || cfg.SSLMode == "verify-full" {
			nc.Close()
			return nil, fmt.Errorf("服务器不支持 SSL，但 sslmode=%s", cfg.SSLMode)
		}
	}
	_ = useTLS

	c := &Conn{
		nc:     nc,
		r:      bufio.NewReaderSize(nc, 1<<20),
		w:      bufio.NewWriterSize(nc, 1<<20),
		cfg:    cfg,
		params: map[string]string{},
	}
	if err := c.startupAndAuth(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// negotiateSSL sends SSLRequest; returns true when the server agreed ('S').
func negotiateSSL(nc net.Conn) (bool, error) {
	var req [8]byte
	binary.BigEndian.PutUint32(req[0:4], 8)
	binary.BigEndian.PutUint32(req[4:8], sslRequestCode)
	if _, err := nc.Write(req[:]); err != nil {
		return false, err
	}
	one := make([]byte, 1)
	nc.SetReadDeadline(time.Now().Add(15 * time.Second))
	defer nc.SetReadDeadline(time.Time{})
	if _, err := nc.Read(one); err != nil {
		return false, err
	}
	return one[0] == 'S', nil
}

func (c *Conn) startupAndAuth(ctx context.Context) error {
	params := map[string]string{
		"user":             c.cfg.User,
		"database":         c.cfg.DB,
		"application_name": c.cfg.AppName,
		"client_encoding":  "UTF8",
	}
	if err := c.writeStartup(params); err != nil {
		return fmt.Errorf("发送启动消息失败: %w", err)
	}
	var scram *scramClient
	authDone := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, payload, err := c.readMsg()
		if err != nil {
			return fmt.Errorf("等待认证响应失败: %w", err)
		}
		switch t {
		case 'R': // Authentication
			if len(payload) < 4 {
				return fmt.Errorf("认证消息格式错误")
			}
			typ := binary.BigEndian.Uint32(payload[0:4])
			switch typ {
			case 0: // AuthenticationOk
				authDone = true
			case 3: // cleartext password
				if err := c.writeMsg('p', []byte(c.cfg.Password+"\x00")); err != nil {
					return err
				}
			case 5: // md5
				if len(payload) < 8 {
					return fmt.Errorf("MD5 认证消息格式错误")
				}
				salt := payload[4:8]
				resp := md5Password(c.cfg.Password, c.cfg.User, salt)
				if err := c.writeMsg('p', []byte(resp+"\x00")); err != nil {
					return err
				}
			case 10: // SASL (SCRAM-SHA-256)
				mechs, err := parseAuthMechanisms(payload[4:])
				if err != nil {
					return err
				}
				mech := ""
				for _, m := range mechs {
					if m == "SCRAM-SHA-256" {
						mech = m
						break
					}
				}
				if mech == "" {
					return fmt.Errorf("服务器不支持的 SASL 机制: %v", mechs)
				}
				scram = newScramClient(c.cfg.Password)
				var b []byte
				b = append(b, mech...)
				b = append(b, 0)
				init := scram.initialResponse()
				lenb := make([]byte, 4)
				binary.BigEndian.PutUint32(lenb, uint32(len(init)))
				b = append(b, lenb...)
				b = append(b, init...)
				if err := c.writeMsg('p', b); err != nil {
					return err
				}
			case 11: // SASLContinue
				if scram == nil {
					return fmt.Errorf("收到意外的 SASLContinue")
				}
				final, err := scram.handleServerFirst(payload[4:])
				if err != nil {
					return err
				}
				if err := c.writeMsg('p', []byte(final)); err != nil {
					return err
				}
			case 12: // SASLFinal
				if scram == nil {
					return fmt.Errorf("收到意外的 SASLFinal")
				}
				if err := scram.verifyServerFinal(payload[4:]); err != nil {
					return err
				}
			default:
				return fmt.Errorf("不支持的认证方式 (类型 %d)", typ)
			}
		case 'K': // BackendKeyData
			if len(payload) >= 8 {
				c.pid = binary.BigEndian.Uint32(payload[0:4])
				c.secret = binary.BigEndian.Uint32(payload[4:8])
			}
		case 'S': // ParameterStatus
			key, off, err := readCString(payload, 0)
			if err != nil {
				return err
			}
			val, _, err := readCString(payload, off)
			if err != nil {
				return err
			}
			c.params[key] = val
		case 'Z': // ReadyForQuery
			if !authDone {
				return fmt.Errorf("服务器在认证完成前进入就绪状态")
			}
			return nil
		case 'E':
			e := parseErrorResponse(payload)
			if strings.Contains(e.Message, "password authentication failed") ||
				e.Code == "28P01" || e.Code == "28000" {
				return fmt.Errorf("认证失败: %s", e.Message)
			}
			return fmt.Errorf("连接数据库 %q 失败: %w", c.cfg.DB, e)
		case 'N': // NoticeResponse — ignore during auth
		case 'v': // NegotiateProtocolVersion
		default:
			// skip unknown
		}
	}
}

func parseAuthMechanisms(b []byte) ([]string, error) {
	var mechs []string
	off := 0
	for off < len(b) {
		m, noff, err := readCString(b, off)
		if err != nil || m == "" {
			if err != nil {
				return nil, err
			}
			break
		}
		off = noff
		mechs = append(mechs, m)
	}
	if len(mechs) == 0 {
		return nil, fmt.Errorf("服务器 SASL 机制列表为空")
	}
	return mechs, nil
}

func md5Password(password, user string, salt []byte) string {
	h1 := md5.Sum([]byte(password + user))
	h2 := md5.Sum(append([]byte(hex.EncodeToString(h1[:])), salt...))
	return "md5" + hex.EncodeToString(h2[:])
}

// serverVersion returns the server version string (e.g. "PostgreSQL 11.22").
func (c *Conn) serverVersion() string {
	return c.params["server_version"]
}

// ServerVersion returns the server_version parameter.
func (c *Conn) ServerVersion() string { return c.serverVersion() }

// Query runs SQL via the simple query protocol and returns all rows.
func (c *Conn) Query(sql string) (*Result, error) {
	return c.queryCtx(context.Background(), sql)
}

func (c *Conn) queryCtx(ctx context.Context, sql string) (*Result, error) {
	if c.closed {
		return nil, errors.New("pgwire: 连接已关闭")
	}
	if err := c.writeMsg('Q', append([]byte(sql), 0)); err != nil {
		return nil, fmt.Errorf("发送查询失败: %w", err)
	}
	res := &Result{}
	haveRows := false
	for {
		if err := ctx.Err(); err != nil {
			// connection may be stuck mid-query; try cancel then drain
			_ = c.Cancel()
			c.drainToReady()
			return nil, err
		}
		t, payload, err := c.readMsg()
		if err != nil {
			return nil, fmt.Errorf("读取查询结果失败: %w", err)
		}
		switch t {
		case 'T': // RowDescription — starts a new result set
			res.Fields = parseRowDescription(payload)
			res.Rows = res.Rows[:0]
			haveRows = true
		case 'D':
			res.Rows = append(res.Rows, parseDataRow(payload))
		case 'C': // CommandComplete
			tag, _, err := readCString(payload, 0)
			if err == nil {
				res.Tag = tag
			}
		case 'I': // EmptyQueryResponse
			res.Tag = ""
		case 'E':
			err := parseErrorResponse(payload)
			c.drainToReady()
			return nil, err
		case 'Z':
			return res, nil
		case 'N': // notice — ignore
		case 'S': // parameter status (e.g. after SET)
			key, off, err := readCString(payload, 0)
			if err == nil {
				val, _, err2 := readCString(payload, off)
				if err2 == nil {
					c.params[key] = val
				}
			}
		case 'A': // NotificationResponse — ignore
		default:
			// unexpected message: ignore
		}
		_ = haveRows
	}
}

// drainToReady consumes messages until ReadyForQuery after an error.
func (c *Conn) drainToReady() {
	for i := 0; i < 100000; i++ {
		t, _, err := c.readMsg()
		if err != nil || t == 'Z' {
			return
		}
	}
}

// Exec runs SQL and returns the command tag.
func (c *Conn) Exec(sql string) (string, error) {
	res, err := c.Query(sql)
	if err != nil {
		return "", err
	}
	return res.Tag, nil
}

// Cancel sends a CancelRequest for this connection via a separate connection.
func (c *Conn) Cancel() error {
	if c.pid == 0 {
		return nil
	}
	nc, err := net.DialTimeout("tcp", net.JoinHostPort(c.cfg.Host, fmt.Sprint(c.cfg.Port)), 10*time.Second)
	if err != nil {
		return err
	}
	defer nc.Close()
	cc := &Conn{nc: nc, r: bufio.NewReader(nc), w: bufio.NewWriter(nc)}
	if err := cc.writeCancel(c.pid, c.secret); err != nil {
		return err
	}
	nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, _ = c.readMsg() // best effort
	return nil
}

// Close terminates the connection cleanly.
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.nc == nil {
		return nil
	}
	_ = c.writeMsg('X', nil) // Terminate
	return c.nc.Close()
}

// Parameter returns a session parameter value (e.g. server_version).
func (c *Conn) Parameter(k string) string { return c.params[k] }
