// Package pgwire implements a minimal pure-Go PostgreSQL wire protocol client
// (protocol version 3.0) using only the standard library.
//
// Supported features:
//   - SSL negotiation (sslmode: disable / prefer / require / verify-full)
//   - Authentication: trust, cleartext password, MD5, SCRAM-SHA-256
//   - Simple query protocol (Query / Exec)
//   - COPY TO STDOUT (CopyTo) and COPY FROM STDIN (CopyFrom) streaming
//   - CancelRequest (backend cancellation)
package pgwire

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config holds the parsed connection parameters.
type Config struct {
	Host    string
	Port    int
	User    string
	Password string
	DB      string
	SSLMode string // disable | prefer | require | verify-full
	AppName string
}

func (c *Config) String() string {
	s := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s", c.Host, c.Port, c.User, c.DB, c.SSLMode)
	if c.Password != "" {
		s += " password=***"
	}
	return s
}

// Sanitized returns the connection string without the password (for logs/UI).
func (c *Config) Sanitized() string {
	return fmt.Sprintf("%s:%d/%s (user=%s, sslmode=%s)", c.Host, c.Port, c.DB, c.User, c.SSLMode)
}

// ParseDSN accepts either a URL form (postgres://user:pass@host:port/db?sslmode=disable)
// or the keyword=value form (host=... port=... user=... password=... dbname=... sslmode=...).
func ParseDSN(s string) (*Config, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("连接串为空")
	}
	cfg := &Config{Port: 5432, SSLMode: "prefer"}
	if strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://") {
		u, err := url.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("URL 连接串解析失败: %w", err)
		}
		if u.User != nil {
			cfg.User = u.User.Username()
			cfg.Password, _ = u.User.Password()
		}
		cfg.Host = u.Hostname()
		if p := u.Port(); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil {
				return nil, fmt.Errorf("端口不合法: %s", p)
			}
			cfg.Port = n
		}
		cfg.DB = strings.TrimPrefix(u.Path, "/")
		q := u.Query()
		if v := q.Get("sslmode"); v != "" {
			cfg.SSLMode = v
		}
		if v := q.Get("application_name"); v != "" {
			cfg.AppName = v
		}
	} else {
		kv, err := parseKeywordValue(s)
		if err != nil {
			return nil, err
		}
		get := func(k string) string { return kv[strings.ToLower(k)] }
		if v := get("host"); v != "" {
			cfg.Host = v
		}
		if v := get("port"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("端口不合法: %s", v)
			}
			cfg.Port = n
		}
		cfg.User = get("user")
		cfg.Password = get("password")
		cfg.DB = get("dbname")
		if cfg.DB == "" {
			cfg.DB = get("database")
		}
		if v := get("sslmode"); v != "" {
			cfg.SSLMode = v
		}
		if v := get("application_name"); v != "" {
			cfg.AppName = v
		}
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	switch cfg.SSLMode {
	case "disable", "prefer", "require", "verify-full", "allow":
	default:
		return nil, fmt.Errorf("不支持的 sslmode: %s（支持 disable/prefer/require/verify-full/allow）", cfg.SSLMode)
	}
	if cfg.SSLMode == "allow" { // treat allow like prefer
		cfg.SSLMode = "prefer"
	}
	if cfg.User == "" {
		return nil, fmt.Errorf("连接串缺少 user")
	}
	if cfg.DB == "" {
		cfg.DB = cfg.User
	}
	return cfg, nil
}

// parseKeywordValue parses "host=a port=5432 password='p w'" style strings.
func parseKeywordValue(s string) (map[string]string, error) {
	out := map[string]string{}
	i := 0
	n := len(s)
	for i < n {
		// skip whitespace
		for i < n && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
			i++
		}
		if i >= n {
			break
		}
		// key
		start := i
		for i < n && s[i] != '=' && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' {
			i++
		}
		key := s[start:i]
		if key == "" {
			return nil, fmt.Errorf("连接串格式错误: %q", s)
		}
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i >= n || s[i] != '=' {
			return nil, fmt.Errorf("连接串缺少 '='（键 %q）", key)
		}
		i++ // skip '='
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		var val string
		if i < n && (s[i] == '\'' || s[i] == '"') {
			quote := s[i]
			i++
			var b strings.Builder
			for i < n {
				ch := s[i]
				if ch == '\\' && i+1 < n {
					i++
					if s[i] == '\n' {
						i++
						continue
					}
					b.WriteByte(s[i])
					i++
					continue
				}
				if ch == quote {
					if i+1 < n && s[i+1] == quote {
						b.WriteByte(quote)
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(ch)
				i++
			}
			val = b.String()
		} else {
			start = i
			for i < n && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' {
				i++
			}
			val = s[start:i]
		}
		out[strings.ToLower(key)] = val
	}
	return out, nil
}

// BuildKeyword builds a keyword=value DSN from parts (password is escaped).
func BuildKeyword(host string, port int, user, password, db, sslmode string) string {
	esc := func(v string) string {
		if v == "" {
			return ""
		}
		if strings.ContainsAny(v, " \t'\"\\") {
			return "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), `'`, `\'`) + "'"
		}
		return v
	}
	parts := []string{
		"host=" + esc(host),
		"port=" + strconv.Itoa(port),
		"user=" + esc(user),
	}
	if password != "" {
		parts = append(parts, "password="+esc(password))
	}
	parts = append(parts, "dbname="+esc(db), "sslmode="+esc(sslmode))
	return strings.Join(parts, " ")
}
