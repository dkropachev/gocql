package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

const testAuthClass = "org.apache.cassandra.auth.PasswordAuthenticator"

// authReply is how the test server answers one AUTH_RESPONSE.
type authReply struct {
	op      frm.Op // OpAuthChallenge, OpAuthSuccess, OpError or anything else
	data    []byte
	errCode protocol.ErrorCode
	errMsg  string
}

// authTestServer answers STARTUP with AUTHENTICATE and every AUTH_RESPONSE
// with reply(token). It records the tokens it receives.
type authTestServer struct {
	*TestServer
	mu     sync.Mutex
	tokens []string
}

func newAuthTestServer(t *testing.T, reply func(token []byte) authReply) *authTestServer {
	t.Helper()
	srv := &authTestServer{TestServer: NewTestServer(t, protocol.ProtoVersion4, context.Background())}
	t.Cleanup(srv.Stop)
	srv.setAuthHook(func(req *protocol.Framer, stream int, resp *protocol.Framer) {
		if req.Header.Op == frm.OpStartup {
			resp.WriteHeader(0, frm.OpAuthenticate, stream)
			resp.WriteString(testAuthClass)
			return
		}
		token := req.ReadBytesCopy()
		srv.mu.Lock()
		srv.tokens = append(srv.tokens, string(token))
		srv.mu.Unlock()

		r := reply(token)
		switch r.op {
		case frm.OpError:
			resp.WriteHeader(0, frm.OpError, stream)
			resp.WriteInt(int32(r.errCode))
			resp.WriteString(r.errMsg)
		case frm.OpAuthChallenge, frm.OpAuthSuccess:
			resp.WriteHeader(0, r.op, stream)
			resp.WriteBytes(r.data)
		default:
			resp.WriteHeader(0, r.op, stream)
		}
	})
	return srv
}

func (srv *authTestServer) received() []string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return append([]string(nil), srv.tokens...)
}

// cluster returns a config for one connection to srv that gives up after a
// single attempt. testCluster has no control connection, so open
// reports a failed handshake only as ErrNoConnectionsStarted; the cause is in
// the pool's log, which lands in the returned logger.
func (srv *authTestServer) cluster(auth transport.Authenticator) (*implCluster.InternalClusterSettings, *logging.Capture) {
	cfg := testCluster(protocol.ProtoVersion4, srv.Address)
	cfg.NumConns = 1
	cfg.Authenticator = auth
	cfg.InitialReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 1}
	cfg.ReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 1}
	logger, out := logging.NewCapture()
	cfg.Logger = logger
	return cfg, out
}

// requireHandshakeError fails t unless open failed and the pool
// logged a cause containing want.
func requireHandshakeError(t *testing.T, cfg *implCluster.InternalClusterSettings, log *logging.Capture, want string) {
	t.Helper()
	s, err := cfg.Open()
	if err == nil {
		s.Close()
		t.Fatal("open succeeded, want an authentication error")
	}
	if !strings.Contains(err.Error(), want) && !log.Contains(want) {
		t.Fatalf("open error = %v, log = %q; want the cause %q", err, log.String(), want)
	}
}

func passwordToken(user, pass string) string {
	return "\x00" + user + "\x00" + pass
}

// authLog records what a stepAuthenticator was called with.
type authLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *authLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *authLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

// stepAuthenticator answers challenge n with token "r<n>" and hands back the
// authenticator for step n+1, so every connection walks the same sequence.
type stepAuthenticator struct {
	log          *authLog
	n            int
	challengeErr error
	successErr   error
}

func (a stepAuthenticator) Challenge(req []byte) ([]byte, transport.Authenticator, error) {
	a.log.add("challenge %d %q", a.n, req)
	if a.challengeErr != nil && a.n > 0 {
		return nil, nil, a.challengeErr
	}
	next := a
	next.n++
	return []byte(fmt.Sprintf("r%d", a.n)), next, nil
}

func (a stepAuthenticator) Success(data []byte) error {
	a.log.add("success %d %q", a.n, data)
	return a.successErr
}

// A SASL-style exchange: two challenges, then a success carrying final data
// that is handed to the authenticator the last challenge returned.
func multiStepReply(token []byte) authReply {
	switch string(token) {
	case "r0":
		return authReply{op: frm.OpAuthChallenge, data: []byte("c1")}
	case "r1":
		return authReply{op: frm.OpAuthSuccess, data: []byte("done")}
	}
	return authReply{op: frm.OpError, errCode: protocol.CodeBadCredentials, errMsg: "unexpected token " + string(token)}
}
