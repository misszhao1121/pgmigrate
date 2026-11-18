package pgwire

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func ctxBG() context.Context { return context.Background() }

// ---- minimal in-process fake PostgreSQL server for protocol tests ----

func srvReadMsg(t *testing.T, c net.Conn) (byte, []byte, bool) {
	var hdr [5]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return 0, nil, false // connection closed or aborted: handler exits quietly
	}
	n := binary.BigEndian.Uint32(hdr[1:5]) - 4
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		return 0, nil, false
	}
	return hdr[0], buf, true
}

func srvWriteMsg(c net.Conn, typ byte, payload []byte) {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)+4))
	c.Write(hdr[:])
	c.Write(payload)
}

func srvSendReady(c net.Conn) {
	srvWriteMsg(c, 'Z', []byte("I"))
}

// srvReadStartup reads the StartupMessage which (unlike normal messages) has
// NO type byte: it is just int32 length + int32 protocol + params.
func srvReadStartup(t *testing.T, c net.Conn) []byte {
	t.Helper()
	var lb [4]byte
	if _, err := io.ReadFull(c, lb[:]); err != nil {
		t.Fatalf("fake server read startup length: %v", err)
	}
	n := binary.BigEndian.Uint32(lb[:]) - 4
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("fake server read startup body: %v", err)
	}
	return buf
}

func srvStartupOK(t *testing.T, c net.Conn) {
	first := srvReadStartup(t, c)
	if len(first) >= 4 && binary.BigEndian.Uint32(first[0:4]) == sslRequestCode {
		c.Write([]byte("N")) // no ssl
		first = srvReadStartup(t, c)
	}
	if binary.BigEndian.Uint32(first[0:4]) != protocolVersion {
		t.Fatalf("expected startup protocol %d, got %d", protocolVersion, binary.BigEndian.Uint32(first[0:4]))
	}
	// AuthenticationOk + params + keydata + ready
	srvWriteMsg(c, 'R', []byte{0, 0, 0, 0})
	srvWriteMsg(c, 'S', []byte("server_version\x00PostgreSQL 11.22 (fake)\x00"))
	var kd [8]byte
	srvWriteMsg(c, 'K', kd[:])
	srvSendReady(c)
}

func startFake(t *testing.T, handler func(c net.Conn)) *Config {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	var once sync.Once
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		once.Do(func() { handler(c) })
		c.Close()
	}()
	addr := ln.Addr().String()
	_, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return &Config{Host: "127.0.0.1", Port: port, User: "tester", Password: "secret", DB: "fake", SSLMode: "disable"}
}

func TestConnectAndQuery(t *testing.T) {
	cfg := startFake(t, func(c net.Conn) {
		srvStartupOK(t, c)
		for {
			typ, buf, ok := srvReadMsg(t, c)
			if !ok {
				return
			}
			switch typ {
			case 'Q':
				q := string(buf)
				if !strings.Contains(q, "SELECT 1") {
					t.Errorf("unexpected query: %q", q)
				}
				// RowDescription with one column "one"
				var rd bytes.Buffer
				binary.Write(&rd, binary.BigEndian, int16(1))
				rd.WriteString("one")
				rd.WriteByte(0)
				fields := make([]byte, 18)
				binary.BigEndian.PutUint32(fields[0:4], 0)   // table oid
				binary.BigEndian.PutUint16(fields[4:6], 0)   // col
				binary.BigEndian.PutUint32(fields[6:10], 23) // type oid int4
				binary.BigEndian.PutUint16(fields[10:12], 4)
				binary.BigEndian.PutUint32(fields[12:16], 0xffffffff)
				binary.BigEndian.PutUint16(fields[16:18], 0)
				rd.Write(fields)
				srvWriteMsg(c, 'T', rd.Bytes())
				srvWriteMsg(c, 'D', []byte{0, 1, 0, 0, 0, 1, '1'})
				srvWriteMsg(c, 'C', []byte("SELECT 1\x00"))
				srvSendReady(c)
			case 'X':
				return
			}
		}
	})
	conn, err := Connect(ctxBG(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	if got := conn.ServerVersion(); !strings.Contains(got, "11.22") {
		t.Errorf("server_version = %q", got)
	}
	r, err := conn.Query("SELECT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(r.Rows) != 1 || r.Col(0, 0) != "1" {
		t.Errorf("rows = %v", r.Rows)
	}
}

func TestQueryError(t *testing.T) {
	cfg := startFake(t, func(c net.Conn) {
		// read startup then fail the AUTH phase with an ErrorResponse
		first := srvReadStartup(t, c)
		if len(first) >= 4 && binary.BigEndian.Uint32(first[0:4]) == sslRequestCode {
			c.Write([]byte("N"))
			_ = srvReadStartup(t, c)
		}
		srvWriteMsg(c, 'E', []byte("SFATAL\x00C28000\x00Mpassword authentication failed for user \"tester\"\x00\x00"))
		for {
			typ, _, ok := srvReadMsg(t, c)
			if !ok || typ == 'X' {
				return
			}
		}
	})
	_, err := Connect(ctxBG(), cfg)
	if err == nil || !strings.Contains(err.Error(), "认证失败") {
		t.Fatalf("expected auth failure error, got %v", err)
	}
}

func TestCopyTo(t *testing.T) {
	cfg := startFake(t, func(c net.Conn) {
		srvStartupOK(t, c)
		for {
			typ, buf, ok := srvReadMsg(t, c)
			if !ok {
				return
			}
			if typ != 'Q' {
				continue
			}
			q := string(buf)
			if !strings.Contains(q, "COPY fake TO STDOUT") {
				t.Errorf("unexpected query %q", q)
			}
			// CopyOutResponse: 1 col, text
			srvWriteMsg(c, 'H', []byte{0, 0, 1, 0})
			srvWriteMsg(c, 'd', []byte("a\t1\n"))
			srvWriteMsg(c, 'd', []byte("b\t2\n"))
			srvWriteMsg(c, 'd', []byte("c\t\\N\n"))
			srvWriteMsg(c, 'c', nil)
			srvWriteMsg(c, 'C', []byte("COPY 3\x00"))
			srvSendReady(c)
		}
	})
	conn, err := Connect(ctxBG(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	var out bytes.Buffer
	if err := conn.CopyTo(ctxBG(), "COPY fake TO STDOUT", &out); err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	want := "a\t1\nb\t2\nc\t\\N\n"
	if out.String() != want {
		t.Errorf("CopyTo got %q want %q", out.String(), want)
	}
}

func TestCopyFrom(t *testing.T) {
	var got bytes.Buffer
	var mu sync.Mutex
	cfg := startFake(t, func(c net.Conn) {
		srvStartupOK(t, c)
		for {
			typ, buf, ok := srvReadMsg(t, c)
			if !ok {
				return
			}
			if typ == 'X' {
				return
			}
			if typ != 'Q' {
				continue
			}
			if !strings.Contains(string(buf), "COPY fake FROM STDIN") {
				t.Errorf("unexpected query %q", string(buf))
			}
			srvWriteMsg(c, 'G', []byte{0, 0, 1, 0})
			total := 0
			for {
				mt, payload, ok2 := srvReadMsg(t, c)
				if !ok2 {
					return
				}
				if mt == 'd' {
					mu.Lock()
					got.Write(payload)
					mu.Unlock()
					total++
				} else if mt == 'c' {
					break
				} else {
					t.Fatalf("unexpected message %q during COPY IN", mt)
				}
			}
			srvWriteMsg(c, 'C', []byte(fmt.Sprintf("COPY %d\x00", total)))
			srvSendReady(c)
		}
	})
	conn, err := Connect(ctxBG(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	data := bytes.Repeat([]byte("row\tdata\n"), 100000) // ~900KB, multiple chunks
	tag, err := conn.CopyFrom(ctxBG(), "COPY fake FROM STDIN", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got.Len() != len(data) {
		t.Errorf("server received %d bytes, want %d", got.Len(), len(data))
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Errorf("server received different bytes")
	}
	if tag != fmt.Sprintf("COPY %d", 100000/(900/1)*1) && !strings.HasPrefix(tag, "COPY ") {
		t.Errorf("tag = %q", tag)
	}
}

func TestMD5Auth(t *testing.T) {
	salt := []byte{1, 2, 3, 4}
	cfg := startFake(t, func(c net.Conn) {
		first := srvReadStartup(t, c)
		if len(first) >= 4 && binary.BigEndian.Uint32(first[0:4]) == sslRequestCode {
			c.Write([]byte("N"))
			_ = srvReadStartup(t, c)
		}
		srvWriteMsg(c, 'R', append([]byte{0, 0, 0, 5}, salt...))
		typ, payload, ok := srvReadMsg(t, c)
		if !ok {
			return
		}
		if typ != 'p' {
			t.Fatalf("expected password message, got %c", typ)
		}
		resp := strings.TrimRight(string(payload), "\x00")
		want := md5Password("secret", "tester", salt)
		if resp != want {
			t.Errorf("md5 response = %q want %q", resp, want)
		}
		srvWriteMsg(c, 'R', []byte{0, 0, 0, 0})
		srvSendReady(c)
	})
	conn, err := Connect(ctxBG(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn.Close()
}

func TestSCRAMAuth(t *testing.T) {
	salt := []byte("0123456789abcdef")
	cfg := startFake(t, func(c net.Conn) {
		first := srvReadStartup(t, c)
		if len(first) >= 4 && binary.BigEndian.Uint32(first[0:4]) == sslRequestCode {
			c.Write([]byte("N"))
			_ = srvReadStartup(t, c)
		}
		// AuthenticationSASL with mechanism list
		var b bytes.Buffer
		b.Write([]byte{0, 0, 0, 10})
		b.WriteString("SCRAM-SHA-256")
		b.WriteByte(0)
		b.WriteByte(0)
		srvWriteMsg(c, 'R', b.Bytes())
		// client SASLInitialResponse
		typ, payload, ok := srvReadMsg(t, c)
		if !ok {
			return
		}
		if typ != 'p' {
			t.Fatalf("expected SASLInitialResponse, got %c", typ)
		}
		mech, off, _ := readCString(payload, 0)
		if mech != "SCRAM-SHA-256" {
			t.Fatalf("mech = %q", mech)
		}
		n := int(int32(binary.BigEndian.Uint32(payload[off : off+4])))
		firstMsg := string(payload[off+4 : off+4+n])
		if !strings.HasPrefix(firstMsg, "n,,n=,r=") {
			t.Fatalf("client first message = %q", firstMsg)
		}
		cNonce := strings.TrimPrefix(firstMsg, "n,,n=,r=")
		serverFirst := fmt.Sprintf("r=%ssrv,s=%s,i=4096", cNonce, base64.StdEncoding.EncodeToString(salt))
		srvWriteMsg(c, 'R', append([]byte{0, 0, 0, 11}, []byte(serverFirst)...))
		// client final
		typ, payload, ok2 := srvReadMsg(t, c)
		if !ok2 {
			return
		}
		if typ != 'p' {
			t.Fatalf("expected SASLResponse, got %c", typ)
		}
		final := strings.TrimRight(string(payload), "\x00")
		parts := map[string]string{}
		for _, kv := range strings.Split(final, ",") {
			if len(kv) > 2 && kv[1] == '=' {
				parts[string(kv[0])] = kv[2:]
			}
		}
		if parts["c"] != "biws" {
			t.Errorf("channel binding = %q", parts["c"])
		}
		if parts["r"] != cNonce+"srv" {
			t.Errorf("nonce = %q", parts["r"])
		}
		proof, err := base64.StdEncoding.DecodeString(parts["p"])
		if err != nil {
			t.Fatalf("proof b64: %v", err)
		}
		// verify proof like a real server would
		salted := hi([]byte("secret"), salt, 4096)
		clientKey := hmacSHA256(salted, []byte("Client Key"))
		storedKey := sha256.Sum256(clientKey)
		authMsg := strings.TrimPrefix(firstMsg, "n,,") + "," + serverFirst + "," + "c=biws,r=" + parts["r"]
		clientSig := hmacSHA256(storedKey[:], []byte(authMsg))
		expected := make([]byte, len(clientKey))
		for i := range clientKey {
			expected[i] = clientKey[i] ^ clientSig[i]
		}
		if !hmac.Equal(proof, expected) {
			t.Errorf("client proof mismatch")
		}
		serverKey := hmacSHA256(salted, []byte("Server Key"))
		serverSig := hmacSHA256(serverKey, []byte(authMsg))
		srvWriteMsg(c, 'R', append([]byte{0, 0, 0, 12}, []byte("v="+base64.StdEncoding.EncodeToString(serverSig))...))
		srvWriteMsg(c, 'R', []byte{0, 0, 0, 0}) // AuthenticationOk follows SASLFinal
		srvSendReady(c)
	})
	conn, err := Connect(ctxBG(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn.Close()
}

func TestDSNParse(t *testing.T) {
	c, err := ParseDSN("postgres://adm:P%40ss@198.51.100.5:5433/substation_new?sslmode=require")
	if err != nil {
		t.Fatalf("url dsn: %v", err)
	}
	if c.Host != "198.51.100.5" || c.Port != 5433 || c.User != "adm" || c.Password != "P@ss" || c.DB != "substation_new" || c.SSLMode != "require" {
		t.Errorf("parsed = %+v", c)
	}
	c2, err := ParseDSN("host= 127.0.0.1 port=5432 user=pg password='p w x' dbname=substation_new sslmode=disable")
	if err != nil {
		t.Fatalf("kv dsn: %v", err)
	}
	if c2.Host != "127.0.0.1" || c2.Password != "p w x" || c2.DB != "substation_new" || c2.SSLMode != "disable" {
		t.Errorf("parsed = %+v", c2)
	}
}
