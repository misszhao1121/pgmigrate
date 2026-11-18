package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"pgmigrate/internal/pgwire"
)

func ctxBG() context.Context { return context.Background() }

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// fakePG is an in-process PostgreSQL server speaking enough of the wire
// protocol to drive the real migration engine end-to-end.
type fakePG struct {
	mu             sync.Mutex
	ver            string
	routes         []fakeRoute
	copyOutData    string   // data served for "TO STDOUT"
	srsCopyOutData string   // data served for spatial_ref_sys COPY
	all            []string // every query received (for assertions)
	raw            []string // raw messages for debugging
	copyInBytes    int64
	unknown        []string // unrouted, non-SET queries
	lastCopyTag    string
	connCount      int
}

type fakeRoute struct {
	match string
	cols  []string
	rows  [][]string
}

func (f *fakePG) start(t *testing.T) (host string, port int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.connCount++
			id := f.connCount
			f.mu.Unlock()
			go f.serve(t, c, id)
		}
	}()
	addr := ln.Addr().String()
	_, ps, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(ps)
	return "127.0.0.1", p
}

func (f *fakePG) serve(t *testing.T, c net.Conn, connID int) {
	defer c.Close()
	wmsg := func(typ byte, payload []byte) {
		var hdr [5]byte
		hdr[0] = typ
		hb := uint32(len(payload) + 4)
		hdr[1] = byte(hb >> 24)
		hdr[2] = byte(hb >> 16)
		hdr[3] = byte(hb >> 8)
		hdr[4] = byte(hb)
		c.Write(hdr[:])
		c.Write(payload)
	}
	// startup (no type byte)
	readStartup := func() {
		var lb [4]byte
		io.ReadFull(c, lb[:])
		n := int(uint32(lb[0])<<24|uint32(lb[1])<<16|uint32(lb[2])<<8|uint32(lb[3])) - 4
		buf := make([]byte, n)
		io.ReadFull(c, buf)
		code := uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
		if code == 80877103 { // SSLRequest
			c.Write([]byte("N"))
			io.ReadFull(c, lb[:])
			n = int(uint32(lb[0])<<24|uint32(lb[1])<<16|uint32(lb[2])<<8|uint32(lb[3])) - 4
			buf = make([]byte, n)
			io.ReadFull(c, buf)
		}
		wmsg('R', []byte{0, 0, 0, 0}) // AuthenticationOk
		wmsg('S', []byte("server_version\x00"+f.ver+"\x00"))
		wmsg('K', make([]byte, 8))
		wmsg('Z', []byte("I"))
	}
	readStartup()
	readMsg := func() (byte, []byte, bool) {
		var hdr [5]byte
		if _, err := io.ReadFull(c, hdr[:]); err != nil {
			return 0, nil, false
		}
		n := int(uint32(hdr[1])<<24|uint32(hdr[2])<<16|uint32(hdr[3])<<8|uint32(hdr[4])) - 4
		buf := make([]byte, n)
		if _, err := io.ReadFull(c, buf); err != nil {
			return 0, nil, false
		}
		return hdr[0], buf, true
	}
	for {
		typ, payload, ok := readMsg()
		if !ok || typ == 'X' {
			return
		}
		if typ != 'Q' {
			continue
		}
		f.mu.Lock()
		prev := payload
		if len(prev) > 80 {
			prev = prev[:80]
		}
		f.raw = append(f.raw, fmt.Sprintf("len=%d hex=%x str=%q", len(payload), prev, string(prev)))
		f.mu.Unlock()
		q := strings.TrimRight(string(payload), "\x00")
		switch {
		case strings.Contains(q, " TO STDOUT"):
			wmsg('H', []byte{0, 0, 1, 0})
			f.mu.Lock()
			data := f.copyOutData
			if strings.Contains(q, "spatial_ref_sys") && f.srsCopyOutData != "" {
				data = f.srsCopyOutData
			}
			f.mu.Unlock()
			wmsg('d', []byte(data))
			wmsg('c', nil)
			wmsg('C', []byte(fmt.Sprintf("COPY %d\x00", strings.Count(data, "\n"))))
			wmsg('Z', []byte("I"))
		case strings.Contains(q, " FROM STDIN"):
			wmsg('G', []byte{0, 0, 1, 0})
			nl := 0
			for {
				mt, p2, ok2 := readMsg()
				if !ok2 {
					return
				}
				if mt == 'd' {
					f.mu.Lock()
					f.copyInBytes += int64(len(p2))
					nl += strings.Count(string(p2), "\n")
					f.mu.Unlock()
				} else if mt == 'c' {
					break
				}
			}
			tag := fmt.Sprintf("COPY %d", nl)
			f.mu.Lock()
			f.lastCopyTag = tag
			f.mu.Unlock()
			wmsg('C', []byte(tag+"\x00"))
			wmsg('Z', []byte("I"))
		default:
			f.answerQuery(wmsg, q, connID)
		}
	}
}

func (f *fakePG) answerQuery(wmsg func(byte, []byte), q string, connID int) {
	upper := strings.ToUpper(strings.TrimSpace(q))
	isSet := strings.HasPrefix(upper, "SET ")
	f.mu.Lock()
	f.all = append(f.all, fmt.Sprintf("[c%d] %s", connID, q))
	f.mu.Unlock()
	// longest matching route wins
	var best *fakeRoute
	for i := range f.routes {
		if strings.Contains(q, f.routes[i].match) {
			if best == nil || len(f.routes[i].match) > len(best.match) {
				best = &f.routes[i]
			}
		}
	}
	if best == nil && !isSet && !isCommand(q) {
		f.mu.Lock()
		f.unknown = append(f.unknown, q)
		f.mu.Unlock()
	}
	if best == nil {
		tag := "OK"
		if strings.HasPrefix(upper, "INSERT") {
			tag = "INSERT 0 2"
		}
		wmsg('C', []byte(tag+"\x00"))
		wmsg('Z', []byte("I"))
		return
	}
	// RowDescription
	var rd []byte
	rd = append(rd, byte(len(best.cols)>>8), byte(len(best.cols)))
	for _, name := range best.cols {
		rd = append(rd, name...)
		rd = append(rd, 0)
		rd = append(rd, make([]byte, 18)...)
	}
	wmsg('T', rd)
	for _, row := range best.rows {
		var dr []byte
		dr = append(dr, byte(len(row)>>8), byte(len(row)))
		for _, v := range row {
			l := uint32(len(v))
			dr = append(dr, byte(l>>24), byte(l>>16), byte(l>>8), byte(l))
			dr = append(dr, v...)
		}
		wmsg('D', dr)
	}
	wmsg('C', []byte(fmt.Sprintf("SELECT %d\x00", len(best.rows))))
	wmsg('Z', []byte("I"))
}

func isCommand(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	for _, p := range []string{"CREATE", "ALTER", "DROP", "COMMENT", "GRANT", "REVOKE", "ANALYZE", "REFRESH", "COPY", "BEGIN", "COMMIT", "INSERT", "UPDATE", "DELETE"} {
		if strings.HasPrefix(u, p) {
			return true
		}
	}
	return false
}

func fakeSourceRoutes() []fakeRoute {
	return []fakeRoute{
		{match: "FROM pg_database d WHERE d.datname = current_database()", cols: []string{"datname", "enc", "encid", "collate", "ctype", "size"},
			rows: [][]string{{"my_database", "UTF8", "6", "C", "C", "12345"}}},
		{match: "FROM pg_extension e", cols: []string{"extname", "extversion", "nspname", "extrelocatable"},
			rows: [][]string{{"plpgsql", "1.0", "pg_catalog", "f"}, {"pg_trgm", "1.4", "public", "t"}}},
		{match: "pg_get_userbyid(n.nspowner)", cols: []string{"nspname", "owner"},
			rows: [][]string{{"public", "postgres"}}},
		{match: "t.typtype = 'e'", cols: []string{"nspname", "typname", "labels"},
			rows: [][]string{{"public", "mood", "sad\x01ok\x01happy"}}},
		{match: "t.typtype = 'd'", cols: []string{"nspname", "typname", "base", "notnull", "dflt", "checks", "coll"}, rows: nil},
		{match: "t.typtype = 'c'", cols: []string{"nspname", "typname", "cols"}, rows: nil},
		{match: "format_type(r.rngsubtype", cols: []string{"nspname", "typname", "subtype"}, rows: nil},
		{match: "pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace", cols: []string{"nspname", "collname", "provider", "lcc", "lct"}, rows: nil},
		{match: "p.prokind <> 'a'", cols: []string{"oid", "nspname", "proname", "owner", "args", "prokind", "def"}, rows: nil},
		{match: "pg_aggregate a JOIN pg_proc p", cols: []string{"nspname", "proname", "args", "transfn", "finalfn", "stype", "initcond"}, rows: nil},
		{match: "FROM pg_sequence s", cols: []string{"nspname", "relname", "owner", "seqtype", "start", "incr", "max", "min", "cache", "cycle"},
			rows: [][]string{{"public", "biz_id_seq", "postgres", "bigint", "1", "1", "9223372036854775807", "1", "1", "false"}}},
		{match: "c.relhasoids", cols: []string{"oid", "nspname", "relname", "relkind", "relispartition", "relpersistence", "relrowsecurity", "relforcerowsecurity", "relhasoids", "owner", "partkey", "partbound", "reloptions", "est_rows", "tblsize"},
			rows: [][]string{{"16384", "public", "biz", "r", "f", "p", "f", "f", "f", "postgres", "", "", "", "42", "8192"}}},
		{match: "AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid", cols: []string{"oid", "nspname", "relname", "relkind", "relispartition", "relpersistence", "relrowsecurity", "relforcerowsecurity", "relhasoids", "owner", "partkey", "partbound", "reloptions", "est_rows", "tblsize"},
			rows: [][]string{{"16400", "public", "spatial_ref_sys", "r", "f", "p", "f", "f", "f", "postgres", "", "", "", "1000", "16384"}}},
		{match: "a.attnum > 0 AND NOT a.attisdropped AND tc.relkind IN ('r','p')", cols: []string{"attrelid", "attnum", "attname", "typ", "attnotnull", "attidentity", "dflt", "coll"},
			rows: [][]string{
				{"16384", "1", "id", "integer", "t", "", "", "default"},
				{"16384", "2", "name", "character varying(64)", "f", "", "", "default"},
				{"16384", "3", "mood", "public.mood", "f", "", "", "default"},
				{"16400", "1", "srid", "integer", "t", "", "", "default"},
				{"16400", "2", "auth_name", "character varying(256)", "f", "", "", "default"},
				{"16400", "3", "auth_srid", "integer", "f", "", "", "default"},
				{"16400", "4", "srtext", "character varying(2048)", "f", "", "", "default"},
				{"16400", "5", "proj4text", "character varying(2048)", "f", "", "", "default"},
			}},
		{match: "SELECT i.inhparent, i.inhrelid FROM pg_inherits i", cols: []string{"inhparent", "inhrelid"}, rows: nil},
		{match: "c.relkind = 'v'", cols: []string{"nspname", "relname", "owner", "def", "reloptions"}, rows: nil},
		{match: "c.relkind = 'm'", cols: []string{"nspname", "relname", "owner", "def", "reloptions"}, rows: nil},
		{match: "pg_get_indexdef(i.indexrelid)", cols: []string{"insch", "iname", "def", "tsch", "tname", "tkind", "indexrelid"},
			rows: [][]string{{"public", "biz_name_idx", "CREATE INDEX biz_name_idx ON public.biz USING btree (name)", "public", "biz", "r", "16385"}}},
		{match: "SELECT conindid FROM pg_constraint", cols: []string{"conindid"}, rows: nil},
		{match: "pi.relkind = 'I' AND ci.relkind = 'i'", cols: []string{"inhparent", "inhrelid"}, rows: nil},
		{match: "pg_get_constraintdef(con.oid, true)", cols: []string{"nspname", "relname", "conname", "contype", "def", "relkind"},
			rows: [][]string{{"public", "biz", "biz_pkey", "p", "PRIMARY KEY (id)", "r"}}},
		{match: "NOT t.tgisinternal", cols: []string{"nspname", "def"}, rows: nil},
		{match: "FROM pg_policies", cols: []string{"schemaname", "tablename", "policyname", "permissive", "roles", "cmd", "qual", "with_check"}, rows: nil},
		{match: "format('COMMENT ON TABLE %I.%I IS %L'", cols: []string{"comment"},
			rows: [][]string{{`COMMENT ON TABLE public.biz IS '业务表'`}}},
		{match: "COMMENT ON COLUMN", cols: []string{"comment"}, rows: nil},
		{match: "COMMENT ON INDEX", cols: []string{"comment"}, rows: nil},
		{match: "COMMENT ON CONSTRAINT", cols: []string{"comment"}, rows: nil},
		{match: "COMMENT ON FUNCTION", cols: []string{"comment"}, rows: nil},
		{match: "COMMENT ON TYPE", cols: []string{"comment"}, rows: nil},
		{match: "COMMENT ON SCHEMA", cols: []string{"comment"}, rows: nil},
		{match: "unnest(setconfig)", cols: []string{"setconfig"}, rows: nil},
		{match: "d.deptype = 'a'", cols: []string{"seqsch", "seqname", "tabsch", "tabname", "colname"},
			rows: [][]string{{"public", "biz_id_seq", "public", "biz", "id"}}},
		{match: "relkind = 'f'", cols: []string{"count"}, rows: [][]string{{"0"}}},
		{match: "pg_largeobject", cols: []string{"exists"}, rows: [][]string{{"f"}}},
		{match: "FROM pg_partitioned_table pt", cols: []string{"partrelid", "partstrat", "attname"}, rows: nil},
		{match: "SELECT last_value, is_called FROM", cols: []string{"last_value", "is_called"},
			rows: [][]string{{"5", "t"}}},
		{match: "last_value||','||is_called", cols: []string{"v"}, rows: [][]string{{"5,true"}}},
		{match: "SELECT count(*)", cols: []string{"count"}, rows: [][]string{{"2"}}},
		{match: "con.contype = 'p'", cols: []string{"nspname", "relname", "attname"},
			rows: [][]string{{"public", "biz", "id"}}},
		{match: "t.typname LIKE 'timestamp%'", cols: []string{"nspname", "relname", "attname"}, rows: nil},
		{match: "SELECT max(", cols: []string{"max"}, rows: [][]string{{"2"}}},
		{match: "GROUP BY loid ORDER BY loid", cols: []string{"loid", "bytes"},
			rows: [][]string{{"100001", "6"}}},
		{match: "SELECT pageno, data FROM pg_catalog.pg_largeobject", cols: []string{"pageno", "data"},
			rows: [][]string{{"0", "\\x48454144"}, {"1", "\\x54524149"}, {"2", "\\x4c454e44"}}},
		{match: "pg_get_userbyid(lomowner)", cols: []string{"loid", "owner"},
			rows: [][]string{{"100001", "postgres"}}},
		{match: "COALESCE(sum(length(data)), 0) FROM pg_catalog.pg_largeobject", cols: []string{"count", "sz"},
			rows: [][]string{{"3", "6"}}},
	}
}
func fakeTargetRoutes() []fakeRoute {
	return []fakeRoute{
		{match: "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname =", cols: []string{"exists"},
			rows: [][]string{{"f"}}},
		{match: "SELECT loid FROM pg_catalog.pg_largeobject_metadata", cols: []string{"loid"}, rows: nil},
		{match: "FROM pg_catalog.pg_roles", cols: []string{"rolname"},
			rows: [][]string{{"postgres"}, {"u"}}},
		{match: "pg_available_extensions", cols: []string{"name", "default_version"},
			rows: [][]string{{"pg_trgm", "1.6"}}},
		{match: "c.relkind IN ('r','p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE", cols: []string{"nspname", "relname"},
			rows: [][]string{{"public", "biz"}, {"public", "spatial_ref_sys"}}},
		{match: "SELECT setval(", cols: []string{"setval"}, rows: [][]string{{"5"}}},
		{match: "last_value||','||is_called", cols: []string{"v"}, rows: [][]string{{"5,true"}}},
		{match: "COALESCE(sum(length(data)), 0) FROM pg_catalog.pg_largeobject", cols: []string{"count", "sz"},
			rows: [][]string{{"3", "6"}}},
		{match: "SELECT pg_catalog.lo_create(", cols: []string{"lo_create"}, rows: [][]string{{"100001"}}},
		{match: "SELECT pg_catalog.lo_put(", cols: []string{"lo_put"}, rows: [][]string{{"0"}}},
		{match: "SELECT count(*)", cols: []string{"count"}, rows: [][]string{{"2"}}},
	}
}

func runEngineTest(t *testing.T, opts *Options, src *fakePG, tgt *fakePG) (*Status, error) {
	sh, sp := src.start(t)
	th, tp := tgt.start(t)
	opts.Source = ConnInfo{Host: sh, Port: sp, User: "u", Password: "p", DB: "my_database", SSLMode: "disable"}
	opts.Target = ConnInfo{Host: th, Port: tp, User: "u", Password: "p", DB: "postgres", SSLMode: "disable"}
	if opts.TargetDB == "" {
		opts.TargetDB = "my_database"
	}
	opts.normalize()
	eng := New(*opts, nil)
	err := eng.Run()
	return eng.Status(), err
}

func TestEngineEndToEnd(t *testing.T) {
	bizData := "1\t张三\tsad\n2\t李四\tok\n"
	srsData := "4326\tEPSG:4326\t4326\tGEOGCS[\"WGS 84\"]\t+proj=longlat\n100001\tCUSTOM\t100001\tCUSTOM SRID\t+proj=local\n"
	src := &fakePG{ver: "PostgreSQL 11.22", routes: fakeSourceRoutes(), copyOutData: bizData, srsCopyOutData: srsData}
	tgt := &fakePG{ver: "PostgreSQL 18.0", routes: fakeTargetRoutes()}
	wmPath := filepath.Join(t.TempDir(), "wm.json")
	opts := &Options{
		Workers: 2, CopyData: true, Verify: true, CreateTarget: true,
		RefreshMatviews: true, Analyze: false, WatermarkFile: wmPath,
	}
	st, err := runEngineTest(t, opts, src, tgt)
	if err != nil {
		t.Fatalf("engine run: %v", err)
	}
	if st.State != "done" {
		t.Fatalf("state = %s, error = %s, warnings = %v", st.State, st.Error, st.Warnings)
	}
	if st.DoneTables != 2 || st.FailedTables != 0 {
		t.Errorf("tables done=%d failed=%d", st.DoneTables, st.FailedTables)
	}
	if st.RowsCopied != 4 {
		tgt.mu.Lock()
		lastTag := tgt.lastCopyTag
		tgt.mu.Unlock()
		t.Errorf("rows copied = %d, want 4; tables=%+v lastCopyTag=%q", st.RowsCopied, st.Tables, lastTag)
	}
	if st.Verification == nil || !st.Verification.OK {
		t.Errorf("verification missing or not OK: %+v", st.Verification)
	}
	// unknown queries on both sides must be empty (all catalog queries mapped)
	if len(src.unknown) > 0 {
		sort.Strings(src.unknown)
		t.Errorf("source got %d unrouted queries: %v", len(src.unknown), src.unknown)
	}
	tgtUnknown := []string{}
	tgt.mu.Lock()
	tgtUnknown = append(tgtUnknown, tgt.unknown...)
	tgtAll := append([]string(nil), tgt.all...)
	tgt.mu.Unlock()
	if len(tgtUnknown) > 0 {
		sort.Strings(tgtUnknown)
		t.Errorf("target got %d unrouted queries: %v", len(tgtUnknown), tgtUnknown)
	}
	for i, qq := range tgtAll {
		if qq == "[c2] public" {
			tgt.mu.Lock()
			rawDump := strings.Join(tgt.raw[max(0, i-2):i+3], "\n  ")
			tgt.mu.Unlock()
			t.Errorf("bare 'public' at all-index %d; raw messages around it:\n  %s", i, rawDump)
		}
	}
	// check key DDL reached the target
	tgt.mu.Lock()
	ddl := strings.Join(tgt.all, "\n")
	tgt.mu.Unlock()
	for _, want := range []string{
		`CREATE DATABASE "my_database"`,
		`CREATE EXTENSION IF NOT EXISTS "pg_trgm" VERSION '1.4'`,
		`CREATE TYPE "public"."mood" AS ENUM ('sad', 'ok', 'happy')`,
		`CREATE SEQUENCE "public"."biz_id_seq" AS bigint`,
		`CREATE TABLE "public"."biz"`,
		`"id" integer NOT NULL`,
		`"name" character varying(64)`,
		`"mood" public.mood`,
		`ADD CONSTRAINT "biz_pkey" PRIMARY KEY (id)`,
		`CREATE INDEX biz_name_idx ON public.biz USING btree (name)`,
		`ALTER SEQUENCE "public"."biz_id_seq" OWNED BY "public"."biz"."id"`,
		`COMMENT ON TABLE public.biz IS '业务表'`,
		`setval('public.biz_id_seq'::regclass, 5, true)`,
		`CREATE TEMP TABLE pgmigrate_stg AS SELECT * FROM "public"."spatial_ref_sys" LIMIT 0`,
		`INSERT INTO "public"."spatial_ref_sys" SELECT * FROM pgmigrate_stg ON CONFLICT DO NOTHING`,
		`lo_create(100001)`,
		`lo_put(100001, 0, decode('48454144545241494c454e44','hex'))`,
		`ALTER LARGE OBJECT 100001 OWNER TO "postgres"`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("target DDL missing %q\nDDL:\n%s", want, ddl)
		}
	}
	tgt.mu.Lock()
	bytesIn := tgt.copyInBytes
	tgt.mu.Unlock()
	wantBytes := int64(len(src.copyOutData) + len(src.srsCopyOutData))
	if bytesIn != wantBytes {
		t.Errorf("target received %d bytes, want %d", bytesIn, wantBytes)
	}

	// ---- watermark file recorded by the full run ----
	wmRaw, err := os.ReadFile(wmPath)
	if err != nil {
		t.Fatalf("watermark file missing: %v", err)
	}
	var wm map[string]Watermark
	if err := json.Unmarshal(wmRaw, &wm); err != nil {
		t.Fatalf("watermark parse: %v", err)
	}
	w := wm["public.biz"]
	if w.Mode != IncrPK || w.Column != "id" || w.Value != "2" {
		t.Errorf("watermark = %+v, want pk/id/2", w)
	}

	// ---- incremental run: only rows above the watermark are merged ----
	incOpts := &Options{
		Source: opts.Source, Target: opts.Target,
		Workers: 1, CopyData: true, Verify: true, Incremental: true,
		WatermarkFile: wmPath, TargetDB: "my_database",
	}
	eng2 := New(*incOpts, nil)
	if err := eng2.Run(); err != nil {
		t.Fatalf("incremental run: %v", err)
	}
	st2 := eng2.Status()
	if st2.State != "done" {
		t.Fatalf("incremental state = %s, error = %s, warnings = %v", st2.State, st2.Error, st2.Warnings)
	}
	if st2.RowsCopied != 2 {
		t.Errorf("incremental rows = %d, want 2 (INSERT tag from fake)", st2.RowsCopied)
	}
	foundIncr := false
	for _, l := range eng2.Logs(0) {
		if strings.Contains(l.Msg, "增量同步 public.biz") {
			foundIncr = true
		}
	}
	if !foundIncr {
		t.Errorf("expected incremental sync log for public.biz")
	}
}

func TestExtCheck(t *testing.T) {
	src := &fakePG{ver: "PostgreSQL 11.22", routes: fakeSourceRoutes()}
	tgt := &fakePG{ver: "PostgreSQL 18.0", routes: fakeTargetRoutes()}
	sh, sp := src.start(t)
	th, tp := tgt.start(t)
	srcCfg := &pgwire.Config{Host: sh, Port: sp, User: "u", Password: "p", DB: "my_database", SSLMode: "disable"}
	tgtCfg := &pgwire.Config{Host: th, Port: tp, User: "u", Password: "p", DB: "postgres", SSLMode: "disable"}
	items, err := CheckExtensionCompatibility(ctxBG(), srcCfg, tgtCfg)
	if err != nil {
		t.Fatalf("extcheck: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1 (plpgsql skipped)", len(items))
	}
	it := items[0]
	if it.Name != "pg_trgm" || it.Status != "version-diff" || it.TargetVersion != "1.6" {
		t.Errorf("item = %+v", it)
	}
	if !it.Available {
		t.Errorf("pg_trgm should be available on target")
	}
}

func TestEngineRefusesNonEmptyTarget(t *testing.T) {
	src := &fakePG{ver: "PostgreSQL 11.22", routes: fakeSourceRoutes(), copyOutData: "1\ta\tsad\n"}
	tgt := &fakePG{ver: "PostgreSQL 18.0", routes: fakeTargetRoutes()}
	// make target db exist
	for i := range tgt.routes {
		if tgt.routes[i].match == "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname =" {
			tgt.routes[i].rows = [][]string{{"t"}}
		}
	}
	opts := &Options{Workers: 1, CopyData: true, Verify: false, CreateTarget: true, DropTarget: false}
	_, err := runEngineTest(t, opts, src, tgt)
	if err == nil || !strings.Contains(err.Error(), "非空") {
		t.Fatalf("expected non-empty target rejection, got %v", err)
	}
}

var _ = pgwire.Connect // keep import
