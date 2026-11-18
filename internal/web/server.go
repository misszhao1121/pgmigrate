// Package web serves the embedded migration UI and JSON API.
package web

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"pgmigrate/internal/migrate"
	"pgmigrate/internal/pgwire"
)

//go:embed static
var staticFS embed.FS

// Server holds the single migration job.
type Server struct {
	mu       sync.Mutex
	engine   *migrate.Engine
	defaults map[string]interface{}
}

// New creates a web server state.
func New() *Server { return &Server{} }

// SetDefaults pre-fills the UI from a config file.
func (s *Server) SetDefaults(m map[string]interface{}) { s.defaults = m }

// Handler builds the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/defaults", s.handleDefaults)
	mux.HandleFunc("/api/test", s.handleTest)
	mux.HandleFunc("/api/extcheck", s.handleExtCheck)
	mux.HandleFunc("/api/incrplan", s.handleIncrPlan)
	mux.HandleFunc("/api/tablelist", s.handleTableList)
	mux.HandleFunc("/api/start", s.handleStart)
	mux.HandleFunc("/api/cancel", s.handleCancel)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// decodeBody decodes a JSON request body, tolerating a UTF-8 BOM.
func decodeBody(r *http.Request, v interface{}) error {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
	b = bytes.TrimSpace(b)
	return json.Unmarshal(b, v)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "UI 资源缺失", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func (s *Server) handleDefaults(w http.ResponseWriter, r *http.Request) {	s.mu.Lock()
	defer s.mu.Unlock()
	if s.defaults == nil {
		writeJSON(w, 200, map[string]interface{}{})
		return
	}
	writeJSON(w, 200, s.defaults)
}

func (s *Server) currentEngine() *migrate.Engine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.engine
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	eng := s.currentEngine()
	var since uint64
	if v := r.URL.Query().Get("since"); v != "" {
		fmt.Sscanf(v, "%d", &since)
	}
	if eng == nil {
		writeJSON(w, 200, map[string]interface{}{"status": map[string]string{"state": "idle", "phase": "idle"}, "logs": []migrate.Log{}})
		return
	}
	st := eng.Status()
	logs := eng.Logs(since)
	writeJSON(w, 200, map[string]interface{}{"status": st, "logs": logs})
}

type testReq struct {
	Role     string `json:"role"` // source | target
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	SSLMode  string `json:"sslmode"`
	TargetDB string `json:"targetdb"`
}

func (s *Server) handleTest(w http.ResponseWriter, r *http.Request) {
	var req testReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, 400, "请求体解析失败: "+err.Error())
		return
	}
	if req.SSLMode == "" {
		req.SSLMode = "prefer"
	}
	cfg := &pgwire.Config{
		Host: req.Host, Port: req.Port, User: req.User, Password: req.Password,
		DB: req.DB, SSLMode: req.SSLMode, AppName: "pgmigrate-test",
	}
	ctx := r.Context()
	c, err := pgwire.Connect(ctx, cfg)
	if err != nil {
		writeJSON(w, 200, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	defer c.Close()
	out := map[string]interface{}{
		"ok":      true,
		"version": c.ServerVersion(),
		"user":    c.Parameter("session_user"),
	}
	if rr, err := c.Query(`SELECT pg_encoding_to_char(encoding), pg_size_pretty(pg_database_size(oid)), current_database() FROM pg_database WHERE datname = current_database()`); err == nil && len(rr.Rows) > 0 {
		out["encoding"] = rr.Col(0, 0)
		out["size"] = rr.Col(0, 1)
		out["db"] = rr.Col(0, 2)
	}
	if req.Role == "target" && req.TargetDB != "" {
		rr, err := c.Query(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)", migrate.QuoteLit(req.TargetDB)))
		if err == nil && len(rr.Rows) > 0 {
			out["dbexists"] = rr.Col(0, 0) == "t"
		}
	}
	writeJSON(w, 200, out)
}

// handleExtCheck compares source extensions against the target server's
// availability (important for offline container deployments).
func (s *Server) handleExtCheck(w http.ResponseWriter, r *http.Request) {
	var req startReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, 400, "请求体解析失败: "+err.Error())
		return
	}
	if req.Source.Host == "" || req.Source.User == "" || req.Source.DB == "" {
		writeErr(w, 400, "请先填写源库连接信息")
		return
	}
	if req.Target.Host == "" || req.Target.User == "" {
		writeErr(w, 400, "请先填写目标库连接信息")
	}
	srcSSL := req.Source.SSLMode
	tgtSSL := req.Target.SSLMode
	if srcSSL == "" {
		srcSSL = "prefer"
	}
	if tgtSSL == "" {
		tgtSSL = "prefer"
	}
	srcCfg := &pgwire.Config{
		Host: req.Source.Host, Port: req.Source.Port, User: req.Source.User,
		Password: req.Source.Password, DB: req.Source.DB, SSLMode: srcSSL, AppName: "pgmigrate-extcheck",
	}
	tgtDB := req.Target.DB
	if tgtDB == "" {
		tgtDB = "postgres"
	}
	tgtCfg := &pgwire.Config{
		Host: req.Target.Host, Port: req.Target.Port, User: req.Target.User,
		Password: req.Target.Password, DB: tgtDB, SSLMode: tgtSSL, AppName: "pgmigrate-extcheck",
	}
	items, err := migrate.CheckExtensionCompatibility(r.Context(), srcCfg, tgtCfg)
	if err != nil {
		writeJSON(w, 200, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	missing := 0
	vdiff := 0
	for _, it := range items {
		if it.Status == "missing" {
			missing++
		} else if it.Status == "version-diff" {
			vdiff++
		}
	}
	writeJSON(w, 200, map[string]interface{}{
		"ok": true, "items": items, "total": len(items), "missing": missing, "version_diff": vdiff,
	})
}

// handleIncrPlan probes the source database and returns the per-table
// incremental (watermark) plan for the UI.
func (s *Server) handleIncrPlan(w http.ResponseWriter, r *http.Request) {
	var req startReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, 400, "请求体解析失败: "+err.Error())
		return
	}
	if req.Source.Host == "" || req.Source.User == "" || req.Source.DB == "" {
		writeErr(w, 400, "请先填写源库连接信息")
		return
	}
	ssl := req.Source.SSLMode
	if ssl == "" {
		ssl = "prefer"
	}
	cfg := &pgwire.Config{
		Host: req.Source.Host, Port: req.Source.Port, User: req.Source.User,
		Password: req.Source.Password, DB: req.Source.DB, SSLMode: ssl, AppName: "pgmigrate-incrplan",
	}
	c, err := pgwire.Connect(r.Context(), cfg)
	if err != nil {
		writeJSON(w, 200, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	defer c.Close()
	opts := &migrate.Options{
		Source: migrate.ConnInfo{Host: req.Source.Host, Port: req.Source.Port, User: req.Source.User, DB: req.Source.DB, SSLMode: ssl},
	}
	cat, err := migrate.CollectCatalog(c, opts)
	if err != nil {
		writeJSON(w, 200, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	plans := migrate.BuildIncrPlans(c, cat, migrate.ParseIncrOverrides(req.IncrKeys))
	usable, none, pk, ts := 0, 0, 0, 0
	for _, p := range plans {
		switch p.Mode {
		case migrate.IncrPK:
			pk++
		case migrate.IncrTS:
			ts++
		default:
			none++
		}
	}
	usable = pk + ts
	writeJSON(w, 200, map[string]interface{}{
		"ok": true, "plans": plans, "total": len(plans), "usable": usable,
		"pk": pk, "ts": ts, "none": none,
	})
}

// handleTableList returns source tables for the UI picker: name, kind
// (table/partition parent/partition), estimated rows and size.
func (s *Server) handleTableList(w http.ResponseWriter, r *http.Request) {
	var req startReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, 400, "请求体解析失败: "+err.Error())
		return
	}
	if req.Source.Host == "" || req.Source.User == "" || req.Source.DB == "" {
		writeErr(w, 400, "请先填写源库连接信息")
		return
	}
	ssl := req.Source.SSLMode
	if ssl == "" {
		ssl = "prefer"
	}
	cfg := &pgwire.Config{
		Host: req.Source.Host, Port: req.Source.Port, User: req.Source.User,
		Password: req.Source.Password, DB: req.Source.DB, SSLMode: ssl, AppName: "pgmigrate-tablelist",
	}
	c, err := pgwire.Connect(r.Context(), cfg)
	if err != nil {
		writeJSON(w, 200, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	defer c.Close()
	type row struct {
		Schema string `json:"schema"`
		Name   string `json:"name"`
		Kind   string `json:"kind"` // table | parent | partition
		Est    int64  `json:"estrows"`
		Size   int64  `json:"sizebytes"`
	}
	rr, err := c.Query(`SELECT n.nspname, c.relname, c.relkind, c.reltuples::bigint, COALESCE(pg_table_size(c.oid), 0)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'
		AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
		ORDER BY n.nspname, c.relname`)
	if err != nil {
		writeJSON(w, 200, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	rows := []row{}
	col := func(rr *pgwire.Result, i, j int) string { return rr.Col(i, j) }
	num := func(rr *pgwire.Result, i, j int) int64 {
		v, _ := strconv.ParseInt(strings.TrimSpace(rr.Col(i, j)), 10, 64)
		return v
	}
	for i := range rr.Rows {
		k := "table"
		if col(rr, i, 2) == "p" {
			k = "parent"
		}
		rows = append(rows, row{
			Schema: col(rr, i, 0), Name: col(rr, i, 1), Kind: k,
			Est: num(rr, i, 3), Size: num(rr, i, 4),
		})
	}
	// mark partitions (children) via pg_inherits to relkind 'p' parents
	childMap := map[string]bool{}
	if ri, err := c.Query(`SELECT pn.nspname, pc.relname, cn.nspname, cc.relname
		FROM pg_inherits i
		JOIN pg_class pc ON pc.oid = i.inhparent JOIN pg_namespace pn ON pn.oid = pc.relnamespace
		JOIN pg_class cc ON cc.oid = i.inhrelid JOIN pg_namespace cn ON cn.oid = cc.relnamespace
		WHERE pc.relkind = 'p'`); err == nil {
		for i := range ri.Rows {
			childMap[col(ri, i, 2)+"."+col(ri, i, 3)] = true
		}
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, t := range rows {
		kind := t.Kind
		if kind == "table" && childMap[t.Schema+"."+t.Name] {
			kind = "partition"
		}
		out = append(out, map[string]interface{}{
			"schema": t.Schema, "name": t.Name, "kind": kind,
			"estrows": t.Est, "sizebytes": t.Size,
		})
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "tables": out, "total": len(out)})
}

type startReq struct {
	Source struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		DB       string `json:"db"`
		SSLMode  string `json:"sslmode"`
	} `json:"source"`
	Target struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		DB       string `json:"db"`
		SSLMode  string `json:"sslmode"`
	} `json:"target"`
	TargetDB        string   `json:"targetdb"`
	Workers         int      `json:"workers"`
	CopyData        bool     `json:"copydata"`
	Verify          bool     `json:"verify"`
	DropTarget      bool     `json:"droptarget"`
	CreateTarget    bool     `json:"createtarget"`
	RefreshMatviews bool     `json:"refreshmatviews"`
	Analyze         bool     `json:"analyze"`
	MigrateOwners   bool     `json:"migrateowners"`
	MigrateACLs     bool     `json:"migrateacls"`
	MigrateRoles    bool     `json:"migrateroles"`
	Incremental     bool     `json:"incremental"`
	IncrKeys        []string `json:"incrkeys"`
	WatermarkFile   string   `json:"watermarkfile"`
	IncludeTables   []string `json:"includetables"`
	ExcludeTables   []string `json:"excludetables"`
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.engine != nil && s.engine.Status().State == "running" {
		writeErr(w, 409, "已有迁移任务在运行中")
		return
	}
	var req startReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, 400, "请求体解析失败: "+err.Error())
		return
	}
	if req.Source.Host == "" || req.Source.User == "" || req.Source.DB == "" {
		writeErr(w, 400, "源库连接信息不完整")
		return
	}
	if req.Target.Host == "" || req.Target.User == "" {
		writeErr(w, 400, "目标库连接信息不完整")
		return
	}
	if req.TargetDB == "" {
		req.TargetDB = req.Source.DB
	}
	if req.Target.DB == "" {
		req.Target.DB = "postgres"
	}
	if req.Source.SSLMode == "" {
		req.Source.SSLMode = "prefer"
	}
	if req.Target.SSLMode == "" {
		req.Target.SSLMode = "prefer"
	}
	opts := migrate.Options{
		Source: migrate.ConnInfo{
			Host: req.Source.Host, Port: req.Source.Port, User: req.Source.User,
			Password: req.Source.Password, DB: req.Source.DB, SSLMode: req.Source.SSLMode,
		},
		Target: migrate.ConnInfo{
			Host: req.Target.Host, Port: req.Target.Port, User: req.Target.User,
			Password: req.Target.Password, DB: req.Target.DB, SSLMode: req.Target.SSLMode,
		},
		TargetDB:        req.TargetDB,
		CreateTarget:    req.CreateTarget,
		DropTarget:      req.DropTarget,
		Workers:         req.Workers,
		CopyData:        req.CopyData,
		Verify:          req.Verify,
		RefreshMatviews: req.RefreshMatviews,
		Analyze:         req.Analyze,
		MigrateOwners:   req.MigrateOwners,
		MigrateACLs:     req.MigrateACLs,
		MigrateRoles:    req.MigrateRoles,
		Incremental:     req.Incremental,
		IncrKeys:        req.IncrKeys,
		WatermarkFile:   req.WatermarkFile,
		IncludeTables:   req.IncludeTables,
		ExcludeTables:   req.ExcludeTables,
	}
	eng := migrate.New(opts, func(level, msg string) {
		log.Printf("[%s] %s", level, msg)
	})
	s.engine = eng
	// remember the last successfully submitted configuration so that a tool
	// restart can pre-fill the form again (pgmigrate-last.json)
	if b, err := json.Marshal(req); err == nil {
		if err := os.WriteFile("pgmigrate-last.json", b, 0600); err == nil {
			log.Printf("本次连接配置已保存到 pgmigrate-last.json（下次启动自动预填）")
		}
	}
	go func() {
		if err := eng.Run(); err != nil {
			log.Printf("[engine] %v", err)
		}
	}()
	writeJSON(w, 200, map[string]string{"started": "ok", "targetdb": req.TargetDB})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	eng := s.currentEngine()
	if eng == nil {
		writeErr(w, 409, "没有正在运行的任务")
		return
	}
	eng.Cancel()
	writeJSON(w, 200, map[string]string{"canceled": "ok"})
}
