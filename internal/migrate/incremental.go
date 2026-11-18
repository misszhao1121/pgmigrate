package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"pgmigrate/internal/pgwire"
)

// Incremental ("watermark") sync: only rows newer than the last recorded
// watermark are copied and upserted into the target. Works without touching
// the source server configuration (no wal_level change, no restart).
//
//   - pk mode:  single-column auto-increment primary key; captures INSERTs only
//   - ts mode:  an "update time" style timestamp column; captures INSERTs and
//     UPDATEs (idempotent upsert)
//   - none:     table cannot be incrementally synced (no PK / composite PK);
//     full re-run needed. DELETEs are never captured by watermark sync —
//     run a final full pass (or verify) before cutover.

const (
	IncrPK   = "pk"
	IncrTS   = "ts"
	IncrNone = "none"
)

// IncrPlan is the per-table incremental sync plan.
type IncrPlan struct {
	Schema string   `json:"schema"`
	Table  string   `json:"table"`
	Mode   string   `json:"mode"`           // pk | ts | none
	Column string   `json:"column"`         // watermark column
	PKCols []string `json:"pkcols"`         // target PK columns (for upsert)
	Note   string   `json:"note"`           // explanation / caveat
	EstRows int64   `json:"estrows"`
}

func (p IncrPlan) Key() string { return p.Schema + "." + p.Table }

// Watermark is the recorded sync position of one table.
type Watermark struct {
	Mode      string `json:"mode"`
	Column    string `json:"column"`
	Value     string `json:"value"` // empty = from the beginning
	UpdatedAt string `json:"updated_at"`
}

type watermarkFile map[string]Watermark

func loadWatermarks(path string) watermarkFile {
	wm := watermarkFile{}
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &wm)
	}
	return wm
}

func saveWatermarks(path string, wm watermarkFile) error {
	b, err := json.MarshalIndent(wm, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0600)
}

var (
	reAutoIncr  = regexp.MustCompile(`(?i)nextval`)
	reUpdateCol = regexp.MustCompile(`(?i)(updat|modif|last_modif|last_updat|gx|modif)\w*(time|date|_at|sj|_sj)`)
	reAnyTime   = regexp.MustCompile(`(?i)(time|date|sj)$`)
)

// pkMap queries the primary-key columns of every user table on the source.
func pkMap(src *pgwire.Conn) (map[string][]string, error) {
	r, err := src.Query(`SELECT n.nspname, c.relname, a.attname
		FROM pg_constraint con
		JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = u.attnum
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE con.contype = 'p' AND ` + sysFilter + `
		ORDER BY n.nspname, c.relname, u.ord`)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for i := range r.Rows {
		key := str(r, i, 0) + "." + str(r, i, 1)
		out[key] = append(out[key], str(r, i, 2))
	}
	return out, nil
}

// tsCols queries candidate timestamp/date columns of every user table.
func tsCols(src *pgwire.Conn) (map[string][]string, error) {
	r, err := src.Query(`SELECT n.nspname, c.relname, a.attname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind = 'r' AND ` + sysFilter + `
		  AND (t.typname LIKE 'timestamp%' OR t.typname = 'date')
		ORDER BY n.nspname, c.relname, a.attnum`)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for i := range r.Rows {
		key := str(r, i, 0) + "." + str(r, i, 1)
		out[key] = append(out[key], str(r, i, 2))
	}
	return out, nil
}

// ParseIncrOverrides parses "schema.table=column[:pk|ts]" entries.
func ParseIncrOverrides(entries []string) map[string]string {
	out := map[string]string{}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		i := strings.Index(e, "=")
		if i <= 0 {
			continue
		}
		out[strings.TrimSpace(e[:i])] = strings.TrimSpace(e[i+1:])
	}
	return out
}

// BuildIncrPlans probes the catalog and produces one plan per data table.
// overrides map "schema.table" -> "column[:mode]" and take precedence.
func BuildIncrPlans(src *pgwire.Conn, cat *Catalog, overrides map[string]string) []IncrPlan {
	pks, _ := pkMap(src)
	tss, _ := tsCols(src)

	plans := []IncrPlan{}
	for _, t := range cat.Tables {
		if t.Kind != 'r' || t.ExtOwned {
			continue
		}
		p := IncrPlan{Schema: t.Schema, Table: t.Name, EstRows: t.EstRows, PKCols: pks[t.Key()]}
		key := t.Key()
		if ov, ok := overrides[key]; ok {
			col := ov
			mode := ""
			if j := strings.Index(ov, ":"); j >= 0 {
				col = ov[:j]
				mode = strings.ToLower(ov[j+1:])
			}
			if mode == "" {
				mode = IncrPK
				for _, c := range pks[key] {
					if c == col {
						mode = IncrPK
					}
				}
				for _, c := range tss[key] {
					if c == col {
						mode = IncrTS
					}
				}
			}
			p.Mode = mode
			p.Column = col
			p.Note = "人工指定的增量列"
			plans = append(plans, p)
			continue
		}

		switch {
		case len(p.PKCols) == 0:
			p.Mode = IncrNone
			p.Note = "无主键，无法安全增量（可选全量重跑，或人工指定单调列）"
		case len(p.PKCols) > 1:
			p.Mode = IncrNone
			p.Note = "复合主键，暂不支持水位增量（可人工指定单调列覆盖）"
		default:
			pk := p.PKCols[0]
			// find the best "update time" style column
			best := ""
			for _, c := range tss[key] {
				if reUpdateCol.MatchString(c) {
					best = c
					break
				}
			}
			autoPK := false
			for _, c := range t.Columns {
				if c.Name != pk {
					continue
				}
				if c.Identity != "" || reAutoIncr.MatchString(c.Default) {
					autoPK = true
				}
			}
			if best != "" {
				p.Mode = IncrTS
				p.Column = best
				p.Note = "按更新时间列增量（捕获新增和修改）；若该列无索引，增量查询为全表扫描"
			} else if autoPK {
				p.Mode = IncrPK
				p.Column = pk
				p.Note = "自增主键水位（只捕获新增行，不捕获 UPDATE/DELETE）"
			} else {
				p.Mode = IncrPK
				p.Column = pk
				p.Note = "普通主键，假设其单调递增（若业务主键非单调请人工改用时间列）"
			}
		}
		plans = append(plans, p)
	}
	sort.SliceStable(plans, func(a, b int) bool {
		if plans[a].Schema != plans[b].Schema {
			return plans[a].Schema < plans[b].Schema
		}
		return plans[a].Table < plans[b].Table
	})
	return plans
}

// recordWatermarks computes max(watermark column) for each plan on the SOURCE
// and persists them, so the next incremental run knows where to resume.
func recordWatermarks(ctx context.Context, src *pgwire.Conn, plans []IncrPlan, path string, logf func(level, format string, a ...interface{})) {
	wm := loadWatermarks(path)
	now := time.Now().Format("2006-01-02T15:04:05")
	count := 0
	for _, p := range plans {
		if p.Mode == IncrNone || p.Column == "" {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		r, err := src.Query(fmt.Sprintf("SELECT max(%s)::text FROM %s", QuoteIdent(p.Column), QuoteIdent(p.Schema)+"."+QuoteIdent(p.Table)))
		if err != nil {
			logf("warn", "记录水位失败 %s.%s：%s", p.Schema, p.Table, oneLine(err.Error()))
			continue
		}
		wm[p.Key()] = Watermark{Mode: p.Mode, Column: p.Column, Value: r.Col(0, 0), UpdatedAt: now}
		count++
	}
	if err := saveWatermarks(path, wm); err != nil {
		logf("warn", "保存水位文件失败：%s", err.Error())
		return
	}
	logf("ok", "已记录 %d 张表的水位到 %s（下次可勾选增量模式只同步新数据）", count, path)
}

// buildWhere renders the incremental WHERE clause for a table.
//   - pk mode:  WHERE col > value      (strictly monotonic)
//   - ts mode:  WHERE col >= value     (idempotent re-sync of the same second)
//   - empty value: no WHERE (full pass, upsert is idempotent)
func buildWhere(p IncrPlan, wm Watermark) (string, error) {
	if wm.Value == "" || p.Column == "" || p.Mode == IncrNone {
		return "", nil
	}
	qc := QuoteIdent(p.Column)
	switch p.Mode {
	case IncrPK:
		if _, err := strconv.ParseInt(wm.Value, 10, 64); err != nil {
			return " WHERE " + qc + " > " + QuoteLit(wm.Value), nil
		}
		return " WHERE " + qc + " > " + wm.Value, nil
	case IncrTS:
		return " WHERE " + qc + " >= " + QuoteLit(wm.Value), nil
	}
	return "", fmt.Errorf("未知增量模式 %s", p.Mode)
}

// upsertSQL builds the merge statement from the staging temp table.
func upsertSQL(t *TableInfo, pkCols []string) string {
	q := t.QName()
	sql := "INSERT INTO " + q + " SELECT * FROM pgmigrate_stg"
	if len(pkCols) == 0 {
		return sql + " ON CONFLICT DO NOTHING"
	}
	cols := make([]string, 0, len(pkCols))
	for _, c := range pkCols {
		cols = append(cols, QuoteIdent(c))
	}
	sets := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c.Name == "" {
			continue
		}
		qc := QuoteIdent(c.Name)
		sets = append(sets, qc+"=EXCLUDED."+qc)
	}
	if len(sets) == 0 {
		return sql + " ON CONFLICT (" + strings.Join(cols, ",") + ") DO NOTHING"
	}
	return sql + " ON CONFLICT (" + strings.Join(cols, ",") + ") DO UPDATE SET " + strings.Join(sets, ", ")
}

// mergeCopy streams rows matching `where` from source into the target via a
// staging temp table and a single upsert. Returns the number of rows merged.
func (e *Engine) mergeCopy(ctx context.Context, worker int, srcConn, dstConn *pgwire.Conn, t *TableInfo, where string, pkCols []string) (int64, error) {
	const tmp = "pgmigrate_stg"
	if _, err := dstConn.Exec("DROP TABLE IF EXISTS " + tmp); err != nil {
		return 0, fmt.Errorf("清理临时表失败: %w", err)
	}
	if _, err := dstConn.Exec(fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0", tmp, t.QName())); err != nil {
		return 0, fmt.Errorf("创建临时表失败: %w", err)
	}
	pr, pw := io.Pipe()
	cw := &countingWriter{w: pw, engine: e}
	errCh := make(chan error, 1)
	srcSQL := "COPY " + t.QName() + " TO STDOUT"
	if where != "" {
		srcSQL = "COPY (SELECT * FROM " + t.QName() + where + ") TO STDOUT"
	}
	go func() {
		err := srcConn.CopyTo(ctx, srcSQL, cw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		errCh <- err
	}()
	_, err := dstConn.CopyFrom(ctx, "COPY "+tmp+" FROM STDIN", pr)
	srcErr := <-errCh
	if err == nil && srcErr != nil {
		err = srcErr
	}
	if err != nil {
		_, _ = dstConn.Exec("DROP TABLE IF EXISTS " + tmp)
		return 0, err
	}
	itag, ierr := dstConn.Exec(upsertSQL(t, pkCols))
	_, _ = dstConn.Exec("DROP TABLE IF EXISTS " + tmp)
	if ierr != nil {
		return 0, fmt.Errorf("合并(upsert)失败: %w", ierr)
	}
	rows := int64(0)
	if strings.HasPrefix(itag, "INSERT") {
		f := strings.Fields(itag)
		if len(f) >= 3 {
			if n, perr := strconv.ParseInt(f[len(f)-1], 10, 64); perr == nil {
				rows = n
			}
		}
	}
	_ = worker
	return rows, nil
}
