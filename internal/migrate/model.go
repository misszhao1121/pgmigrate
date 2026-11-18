package migrate

import (
	"fmt"
	"sync"
	"time"

	"pgmigrate/internal/pgwire"
)

// ConnInfo is the web/CLI friendly connection description.
type ConnInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	SSLMode  string `json:"sslmode"`
}

func (c ConnInfo) toCfg() *pgwire.Config {
	return &pgwire.Config{
		Host: c.Host, Port: c.Port, User: c.User, Password: c.Password,
		DB: c.DB, SSLMode: c.SSLMode, AppName: "pgmigrate",
	}
}

func (c ConnInfo) dsn() string {
	return pgwire.BuildKeyword(c.Host, c.Port, c.User, c.Password, c.DB, c.SSLMode)
}

// Options configures a migration run.
type Options struct {
	Source ConnInfo `json:"source"`
	Target ConnInfo `json:"target"`

	TargetDB        string `json:"targetdb"`        // database created on target (default: source db name)
	CreateTarget    bool   `json:"createtarget"`    // create target database when missing
	DropTarget      bool   `json:"droptarget"`      // drop existing target database first (destructive)
	Workers         int    `json:"workers"`         // parallel copy workers
	CopyData        bool   `json:"copydata"`        // copy table data
	Verify          bool   `json:"verify"`          // verify row counts & sequences after load
	RefreshMatviews bool   `json:"refreshmatviews"` // refresh materialized views
	Analyze         bool   `json:"analyze"`         // run ANALYZE after load
	MigrateOwners   bool   `json:"migrateowners"`   // ALTER ... OWNER TO
	MigrateACLs     bool   `json:"migrateacls"`     // GRANT statements
	MigrateRoles    bool   `json:"migrateroles"`    // create roles referenced by owners/acls
	Incremental     bool     `json:"incremental"`    // watermark incremental sync mode
	IncrKeys        []string `json:"incrkeys"`       // "schema.table=column[:pk|ts]" overrides
	WatermarkFile   string   `json:"watermarkfile"`  // watermark state file path
	IncludeTables   []string `json:"includetables"` // patterns like "public.t*" (empty = all)
	ExcludeTables   []string `json:"excludetables"` // patterns like "public.tmp*"
}

func (o *Options) normalize() {
	if o.Workers < 1 {
		o.Workers = 1
	}
	if o.Workers > 64 {
		o.Workers = 64
	}
	if o.TargetDB == "" {
		o.TargetDB = o.Source.DB
	}
	if o.WatermarkFile == "" {
		o.WatermarkFile = "pgmigrate-watermark.json"
	}
}

// Log is one engine log entry.
type Log struct {
	Seq   uint64    `json:"seq"`
	Ts    time.Time `json:"ts"`
	Level string    `json:"level"` // info warn error ok
	Msg   string    `json:"msg"`
}

// TableStat tracks per-table data copy progress.
type TableStat struct {
	Schema    string  `json:"schema"`
	Name      string  `json:"name"`
	EstRows   int64   `json:"estrows"`
	Rows      int64   `json:"rows"`
	SizeBytes int64   `json:"sizebytes"`
	State     string  `json:"state"` // pending | copying | done | failed | skipped
	Err       string  `json:"err,omitempty"`
	Seconds   float64 `json:"seconds,omitempty"`
}

func (t TableStat) Key() string { return t.Schema + "." + t.Name }

// SeqVerify / TableVerify / MVVerify form the verification report.
type TableVerify struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Src    int64  `json:"src"`
	Dst    int64  `json:"dst"`
	OK     bool   `json:"ok"`
}

type SeqVerify struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Src    string `json:"src"`
	Dst    string `json:"dst"`
	OK     bool   `json:"ok"`
}

type MVVerify struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Exists bool   `json:"exists"`
}

type VerifyReport struct {
	OK       bool          `json:"ok"`
	Tables   []TableVerify `json:"tables,omitempty"`
	Seqs     []SeqVerify   `json:"seqs,omitempty"`
	Matviews []MVVerify    `json:"matviews,omitempty"`
	Errors   []string      `json:"errors,omitempty"`
}

// Status is the whole job state published to the web UI / CLI.
type Status struct {
	mu sync.Mutex

	State       string     `json:"state"` // idle|running|done|failed|canceled
	Phase       string     `json:"phase"`
	PhaseDetail string     `json:"phase_detail"`
	Error       string     `json:"error,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`

	Workers      int    `json:"workers"`
	TotalTables  int    `json:"total_tables"`
	DoneTables   int    `json:"done_tables"`
	FailedTables int    `json:"failed_tables"`
	Current      string `json:"current"` // tables being copied now
	RowsCopied   uint64 `json:"rows_copied"`
	BytesCopied  uint64 `json:"bytes_copied"`
	EstRows      uint64 `json:"est_rows"`

	Tables        []TableStat   `json:"tables,omitempty"`
	Warnings      []string      `json:"warnings,omitempty"`
	PhaseFailures []Failure     `json:"phase_failures,omitempty"`
	Verification  *VerifyReport `json:"verification,omitempty"`

	logSeq uint64
	logs   []Log
}

func (s *Status) addLog(level, msg string) Log {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logSeq++
	l := Log{Seq: s.logSeq, Ts: time.Now(), Level: level, Msg: msg}
	s.logs = append(s.logs, l)
	if len(s.logs) > 4000 {
		s.logs = s.logs[len(s.logs)-2000:]
	}
	return l
}

func (s *Status) logsSince(seq uint64) []Log {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Log{}
	for _, l := range s.logs {
		if l.Seq > seq {
			out = append(out, l)
		}
	}
	return out
}

// Snapshot returns a deep-enough copy for JSON encoding (constructed field by
// field to avoid copying the mutex).
func (s *Status) Snapshot() *Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := &Status{
		State:         s.State,
		Phase:         s.Phase,
		PhaseDetail:   s.PhaseDetail,
		Error:         s.Error,
		StartedAt:     s.StartedAt,
		FinishedAt:    s.FinishedAt,
		Workers:       s.Workers,
		TotalTables:   s.TotalTables,
		DoneTables:    s.DoneTables,
		FailedTables:  s.FailedTables,
		Current:       s.Current,
		RowsCopied:    s.RowsCopied,
		BytesCopied:   s.BytesCopied,
		EstRows:       s.EstRows,
		Tables:        append([]TableStat(nil), s.Tables...),
		Warnings:      append([]string(nil), s.Warnings...),
		PhaseFailures: append([]Failure(nil), s.PhaseFailures...),
		Verification:  s.Verification,
		logSeq:        s.logSeq,
	}
	return cp
}

// Phase constants.
const (
	PhaseIdle      = "idle"
	PhasePrecheck  = "precheck"   // 连接与预检查
	PhaseCreateDB  = "createdb"   // 创建目标库
	PhaseExtension = "extensions" // 扩展
	PhaseSchema    = "schemas"    // 模式
	PhaseTypes     = "types"      // 类型
	PhaseFuncs     = "functions"  // 函数
	PhaseSeqs      = "sequences"  // 序列
	PhaseTables    = "tables"     // 表结构
	PhaseViews     = "views"      // 视图
	PhaseMatviews  = "matviews"   // 物化视图
	PhaseData      = "data"       // 数据拷贝
	PhasePostData  = "postdata"   // 索引/约束/触发器等
	PhaseVerify    = "verify"     // 校验
	PhaseDone      = "done"
)

func stateDesc(p string) string {
	switch p {
	case PhaseIdle:
		return "待机"
	case PhasePrecheck:
		return "连接源库与目标库并预检查"
	case PhaseCreateDB:
		return "创建目标数据库"
	case PhaseExtension:
		return "迁移扩展"
	case PhaseSchema:
		return "迁移模式(schema)"
	case PhaseTypes:
		return "迁移类型(枚举/域/复合)"
	case PhaseFuncs:
		return "迁移函数与存储过程"
	case PhaseSeqs:
		return "迁移序列"
	case PhaseTables:
		return "迁移表结构"
	case PhaseViews:
		return "迁移视图"
	case PhaseMatviews:
		return "迁移物化视图"
	case PhaseData:
		return "拷贝表数据"
	case PhasePostData:
		return "迁移索引/约束/触发器/注释等"
	case PhaseVerify:
		return "校验数据一致性"
	case PhaseDone:
		return "完成"
	}
	return p
}

func errf(format string, a ...interface{}) error { return fmt.Errorf(format, a...) }
