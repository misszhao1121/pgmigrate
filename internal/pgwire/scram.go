package pgwire

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// scramClient implements client-side SCRAM-SHA-256 per RFC 5802 / RFC 7677
// as required by PostgreSQL 10+ default authentication.
type scramClient struct {
	password string

	clientNonce string
	clientFirstBare string
	serverFirst string
	saltedPassword []byte
	authMessage string
}

func newScramClient(password string) *scramClient {
	nonce := make([]byte, 18)
	_, _ = rand.Read(nonce)
	b64 := base64.StdEncoding.EncodeToString(nonce)
	return &scramClient{password: password, clientNonce: b64}
}

// initialResponse returns "n,,n=,r=<nonce>".
func (s *scramClient) initialResponse() string {
	s.clientFirstBare = "n=,r=" + s.clientNonce
	return "n,," + s.clientFirstBare
}

// handleServerFirst parses "r=...,s=...,i=..." and returns the client final message.
func (s *scramClient) handleServerFirst(payload []byte) (string, error) {
	s.serverFirst = string(payload)
	fields := map[string]string{}
	for _, part := range strings.Split(s.serverFirst, ",") {
		if len(part) >= 2 && part[1] == '=' {
			fields[string(part[0])] = part[2:]
		}
	}
	nonce, ok := fields["r"]
	if !ok {
		return "", fmt.Errorf("SCRAM: 服务器首条消息缺少 nonce")
	}
	saltB64, ok := fields["s"]
	if !ok {
		return "", fmt.Errorf("SCRAM: 服务器首条消息缺少 salt")
	}
	iterStr, ok := fields["i"]
	if !ok {
		return "", fmt.Errorf("SCRAM: 服务器首条消息缺少迭代次数")
	}
	if !strings.HasPrefix(nonce, s.clientNonce) || len(nonce) <= len(s.clientNonce) {
		return "", fmt.Errorf("SCRAM: 服务器 nonce 校验失败")
	}
	iter, err := strconv.Atoi(iterStr)
	if err != nil || iter < 1 || iter > 1000000 {
		return "", fmt.Errorf("SCRAM: 迭代次数非法 %q", iterStr)
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return "", fmt.Errorf("SCRAM: salt 解码失败: %w", err)
	}

	// SaltedPassword = Hi(password, salt, i)
	s.saltedPassword = hi([]byte(s.password), salt, iter)

	// client-final-message-without-proof
	channelBindingB64 := "biws" // base64("n,,") — no channel binding
	withoutProof := "c=" + channelBindingB64 + ",r=" + nonce
	s.authMessage = s.clientFirstBare + "," + s.serverFirst + "," + withoutProof

	// ClientProof = ClientKey XOR ClientSignature
	clientKey := hmacSHA256(s.saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(s.authMessage))
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}

// verifyServerFinal checks "v=<serverSignature>".
func (s *scramClient) verifyServerFinal(payload []byte) error {
	if !bytes.HasPrefix(payload, []byte("v=")) {
		return fmt.Errorf("SCRAM: 服务器最终消息格式错误")
	}
	sig, err := base64.StdEncoding.DecodeString(string(payload[2:]))
	if err != nil {
		return fmt.Errorf("SCRAM: 服务器签名解码失败: %w", err)
	}
	serverKey := hmacSHA256(s.saltedPassword, []byte("Server Key"))
	expected := hmacSHA256(serverKey, []byte(s.authMessage))
	if !hmac.Equal(sig, expected) {
		return fmt.Errorf("SCRAM: 服务器签名校验失败")
	}
	return nil
}

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

// hi implements PBKDF2-HMAC-SHA-256 (RFC 2898 "Hi" as used by SCRAM).
func hi(str, salt []byte, iter int) []byte {
	// U1 = HMAC(str, salt || INT_32_BE(1))
	var saltBlock []byte
	saltBlock = append(saltBlock, salt...)
	saltBlock = append(saltBlock, 0, 0, 0, 1)

	u := hmacSHA256(str, saltBlock)
	t := make([]byte, len(u))
	copy(t, u)
	for i := 1; i < iter; i++ {
		u = hmacSHA256(str, u)
		for j := range t {
			t[j] ^= u[j]
		}
	}
	return t
}
