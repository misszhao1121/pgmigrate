package migrate

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"pgmigrate/internal/pgwire"
)

// Stmt is one DDL statement to execute on the target.
type Stmt struct {
	SQL      string `json:"sql"`
	Desc     string `json:"desc"`
	Kind     string `json:"kind"` // table/index/constraint/...
	NoCollate bool  `json:"-"`    // retry with COLLATE clauses stripped
}

// Failure is a statement that ultimately could not be applied.
type Failure struct {
	Desc string `json:"desc"`
	SQL  string `json:"sql"`
	Err  string `json:"err"`
}

var collateRe = regexp.MustCompile(`(?i)\s*COLLATE\s+("[^"]+"|[A-Za-z_][A-Za-z_0-9$]*)(\s*\.\s*("[^"]+"|[A-Za-z_][A-Za-z_0-9$]*))?`)

func stripCollate(sql string) string {
	return collateRe.ReplaceAllString(sql, "")
}

// runWithRetry executes statements; on failure a statement is retried in later
// passes (handles dependency ordering); COLLATE clauses are stripped once for
// cross-platform locale mismatches. Returns the statements that still failed,
// each carrying the LAST error actually produced by the server.
func runWithRetry(ctx context.Context, conn *pgwire.Conn, stmts []Stmt, logf func(level, msg string)) []Failure {
	remaining := make([]Stmt, len(stmts))
	copy(remaining, stmts)
	lastErr := map[string]string{}
	key := func(st Stmt) string { return st.Desc + "\x00" + st.SQL }
	var finalFailures []Failure
	const maxPasses = 6
	for pass := 0; pass < maxPasses && len(remaining) > 0; pass++ {
		var stillFailing []Stmt
		progress := false
		for _, st := range remaining {
			if err := ctx.Err(); err != nil {
				finalFailures = append(finalFailures, Failure{Desc: st.Desc, SQL: st.SQL, Err: "已取消"})
				for _, rest := range stillFailing {
					finalFailures = append(finalFailures, Failure{Desc: rest.Desc, SQL: rest.SQL, Err: "已取消"})
				}
				return finalFailures
			}
			sql := st.SQL
			if st.NoCollate {
				sql = stripCollate(sql)
			}
			if _, err := conn.Exec(sql); err != nil {
				// try stripping collate once on collation-related errors
				if !st.NoCollate && isCollationError(err.Error()) {
					st.NoCollate = true
					if _, err2 := conn.Exec(stripCollate(st.SQL)); err2 == nil {
						progress = true
						continue
					} else {
						err = err2
					}
				}
				stillFailing = append(stillFailing, st)
				lastErr[key(st)] = err.Error()
			} else {
				progress = true
			}
		}
		remaining = stillFailing
		if !progress {
			break
		}
	}
	for _, st := range remaining {
		errText := lastErr[key(st)]
		if errText == "" {
			errText = "依赖缺失或顺序问题"
		}
		finalFailures = append(finalFailures, Failure{Desc: st.Desc, SQL: st.SQL, Err: errText})
	}
	return finalFailures
}

func isCollationError(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "collation") || strings.Contains(m, "排序规则") || strings.Contains(m, "locale") ||
		strings.Contains(m, "lc_collate") || strings.Contains(m, "lc_ctype")
}

// ---- builders ----

func schemaStmts(schemas []SchemaInfo) []Stmt {
	out := []Stmt{}
	for _, s := range schemas {
		out = append(out, Stmt{
			SQL:  fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", QuoteIdent(s.Name)),
			Desc: "模式 " + s.Name, Kind: "schema",
		})
	}
	return out
}

func enumStmts(enums []EnumInfo) []Stmt {
	out := []Stmt{}
	for _, e := range enums {
		lbls := make([]string, 0, len(e.Labels))
		for _, l := range e.Labels {
			lbls = append(lbls, QuoteLit(l))
		}
		out = append(out, Stmt{
			SQL:  fmt.Sprintf("CREATE TYPE %s.%s AS ENUM (%s)", QuoteIdent(e.Schema), QuoteIdent(e.Name), strings.Join(lbls, ", ")),
			Desc: "枚举 " + e.Schema + "." + e.Name, Kind: "enum",
		})
	}
	return out
}

func domainStmts(domains []DomainInfo, collLookup map[string]string) []Stmt {
	out := []Stmt{}
	for _, d := range domains {
		s := fmt.Sprintf("CREATE DOMAIN %s.%s AS %s", QuoteIdent(d.Schema), QuoteIdent(d.Name), d.Base)
		if c := collQual(d.Coll, collLookup); c != "" {
			s += " COLLATE " + c
		}
		if d.Default != "" {
			s += " DEFAULT " + d.Default
		}
		if d.NotNull {
			s += " NOT NULL"
		}
		for _, ck := range d.Checks {
			s += " CHECK " + ck
		}
		out = append(out, Stmt{SQL: s, Desc: "域 " + d.Schema + "." + d.Name, Kind: "domain"})
	}
	return out
}

func compositeStmts(comps []CompositeInfo) []Stmt {
	out := []Stmt{}
	for _, c := range comps {
		out = append(out, Stmt{
			SQL:  fmt.Sprintf("CREATE TYPE %s.%s AS (%s)", QuoteIdent(c.Schema), QuoteIdent(c.Name), strings.Join(c.Cols, ", ")),
			Desc: "复合类型 " + c.Schema + "." + c.Name, Kind: "composite",
		})
	}
	return out
}

func rangeStmts(ranges []RangeInfo) []Stmt {
	out := []Stmt{}
	for _, r := range ranges {
		out = append(out, Stmt{
			SQL:  fmt.Sprintf("CREATE TYPE %s.%s AS RANGE (SUBTYPE = %s)", QuoteIdent(r.Schema), QuoteIdent(r.Name), r.Subtype),
			Desc: "范围类型 " + r.Schema + "." + r.Name, Kind: "range",
		})
	}
	return out
}

func collationStmts(colls []CollationInfo, srcEncID int32) []Stmt {
	out := []Stmt{}
	for _, c := range colls {
		var opts []string
		switch c.Provider {
		case "i":
			opts = append(opts, "PROVIDER = icu", "LOCALE = "+QuoteLit(c.LCCollate))
		default:
			if c.LCCollate != "" {
				opts = append(opts, "LC_COLLATE = "+QuoteLit(c.LCCollate))
			}
			if c.LCCTYPE != "" {
				opts = append(opts, "LC_CTYPE = "+QuoteLit(c.LCCTYPE))
			}
		}
		if len(opts) == 0 {
			continue
		}
		out = append(out, Stmt{
			SQL:  fmt.Sprintf("CREATE COLLATION %s.%s (%s)", QuoteIdent(c.Schema), QuoteIdent(c.Name), strings.Join(opts, ", ")),
			Desc: "排序规则 " + c.Schema + "." + c.Name, Kind: "collation",
		})
	}
	return out
}

func funcStmts(funcs []FuncInfo, aggs []AggInfo) []Stmt {
	out := []Stmt{}
	for _, f := range funcs {
		if f.Def == "" {
			continue
		}
		out = append(out, Stmt{SQL: f.Def, Desc: "函数 " + f.Schema + "." + f.Name, Kind: "function"})
	}
	for _, a := range aggs {
		var opts []string
		opts = append(opts, "SFUNC = "+a.TransFn, "STYPE = "+a.SType)
		if a.FinalFn != "" && a.FinalFn != "-" {
			opts = append(opts, "FINALFUNC = "+a.FinalFn)
		}
		if a.InitCond != "" {
			opts = append(opts, "INITCOND = "+QuoteLit(a.InitCond))
		}
		sql := fmt.Sprintf("CREATE AGGREGATE %s.%s(%s) (%s)", QuoteIdent(a.Schema), QuoteIdent(a.Name), a.Args, strings.Join(opts, ", "))
		out = append(out, Stmt{SQL: sql, Desc: "聚合 " + a.Schema + "." + a.Name, Kind: "aggregate"})
	}
	return out
}

func seqStmts(seqs []SeqInfo) []Stmt {
	out := []Stmt{}
	for _, s := range seqs {
		cyc := "NO CYCLE"
		if s.Cycle {
			cyc = "CYCLE"
		}
		sql := fmt.Sprintf("CREATE SEQUENCE %s.%s AS %s START WITH %d INCREMENT BY %d MINVALUE %s MAXVALUE %s CACHE %d %s",
			QuoteIdent(s.Schema), QuoteIdent(s.Name), s.Typ, s.Start, s.Incr, s.MinV, s.MaxV, s.Cache, cyc)
		out = append(out, Stmt{SQL: sql, Desc: "序列 " + s.Schema + "." + s.Name, Kind: "sequence"})
	}
	return out
}

// collQual builds a COLLATE target name for a collation (qualified when user-defined).
func collQual(coll string, collLookup map[string]string) string {
	if coll == "" || coll == "default" {
		return ""
	}
	if sch, ok := collLookup[coll]; ok && sch != "pg_catalog" {
		return QuoteIdent(sch) + "." + QuoteIdent(coll)
	}
	return QuoteIdent(coll)
}

func collLookupMap(colls []CollationInfo) map[string]string {
	m := map[string]string{}
	for _, c := range colls {
		if _, exists := m[c.Name]; !exists {
			m[c.Name] = c.Schema
		}
	}
	return m
}

// orderTables topologically sorts tables: parents before children (both
// partitioned and old-style inheritance).
func orderTables(tables []*TableInfo) []*TableInfo {
	byOid := map[uint32]*TableInfo{}
	for _, t := range tables {
		byOid[t.OID] = t
	}
	visited := map[uint32]bool{}
	inProgress := map[uint32]bool{}
	var out []*TableInfo
	var visit func(t *TableInfo)
	visit = func(t *TableInfo) {
		if visited[t.OID] || inProgress[t.OID] {
			return
		}
		inProgress[t.OID] = true
		if p, ok := byOid[t.ParentOID]; ok {
			visit(p)
		}
		inProgress[t.OID] = false
		visited[t.OID] = true
		out = append(out, t)
	}
	for _, t := range tables {
		visit(t)
	}
	return out
}

func tableColDefs(t *TableInfo, collLookup map[string]string) []string {
	defs := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		s := QuoteIdent(c.Name) + " " + c.Typ
		if cq := collQual(c.Coll, collLookup); cq != "" {
			s += " COLLATE " + cq
		}
		if c.Identity == "a" {
			s += " GENERATED ALWAYS AS IDENTITY"
		} else if c.Identity == "d" {
			s += " GENERATED BY DEFAULT AS IDENTITY"
		}
		if c.Default != "" {
			s += " DEFAULT " + c.Default
		}
		if c.NotNull {
			s += " NOT NULL"
		}
		defs = append(defs, s)
	}
	return defs
}

func relOptsSQL(opts []string) string {
	if len(opts) == 0 {
		return ""
	}
	return " WITH (" + strings.Join(opts, ", ") + ")"
}

func createTableStmt(t *TableInfo, byOid map[uint32]*TableInfo, collLookup map[string]string) string {
	var b strings.Builder
	if t.Persistence == 'u' {
		b.WriteString("CREATE UNLOGGED TABLE ")
	} else {
		b.WriteString("CREATE TABLE ")
	}
	b.WriteString(t.QName())

	switch {
	case t.Kind == 'p' && t.PartOf == 0: // partitioned root
		b.WriteString(" (\n  " + strings.Join(tableColDefs(t, collLookup), ",\n  ") + "\n)")
		wrotePartKey := false
		if len(t.PartKeyCols) > 0 {
			// structurally rebuilt from pg_partitioned_table (robust across versions)
			var strat string
			switch strings.ToLower(strings.TrimSpace(t.PartStrategy)) {
			case "h":
				strat = "HASH"
			case "l":
				strat = "LIST"
			default:
				strat = "RANGE"
			}
			cols := make([]string, 0, len(t.PartKeyCols))
			for _, c := range t.PartKeyCols {
				cols = append(cols, QuoteIdent(c))
			}
			b.WriteString("\nPARTITION BY " + strat + " (" + strings.Join(cols, ", ") + ")")
			wrotePartKey = true
		}
		if !wrotePartKey && t.PartKey != "" {
			b.WriteString("\n" + t.PartKey) // fallback: pg_get_partkeydef (expression keys)
		}
		b.WriteString(relOptsSQL(t.RelOpts))
	case t.PartOf != 0: // partition leaf or sub-partitioned partition
		p := byOid[t.PartOf]
		pq := ""
		if p != nil {
			pq = p.QName()
		} else {
			pq = QuoteIdent(t.Schema) + "." + QuoteIdent(t.Name) + "_parent"
		}
		b.WriteString(" PARTITION OF " + pq)
		if pb := t.PartBound; pb != "" {
			b.WriteString(" " + pb)
		}
	default: // ordinary table
		b.WriteString(" (\n  " + strings.Join(tableColDefs(t, collLookup), ",\n  ") + "\n)")
		if len(t.ParentNames) > 0 {
			b.WriteString(" INHERITS (" + strings.Join(t.ParentNames, ", ") + ")")
		}
		b.WriteString(relOptsSQL(t.RelOpts))
	}
	return b.String()
}

func tableStmts(cat *Catalog) []Stmt {
	byOid := map[uint32]*TableInfo{}
	for _, t := range cat.Tables {
		byOid[t.OID] = t
	}
	collLookup := collLookupMap(cat.Collations)
	ordered := orderTables(cat.Tables)
	out := []Stmt{}
	for _, t := range ordered {
		out = append(out, Stmt{
			SQL:  createTableStmt(t, byOid, collLookup),
			Desc: "表 " + t.Schema + "." + t.Name, Kind: "table",
		})
	}
	return out
}

func viewStmts(views []ViewInfo) []Stmt {
	out := []Stmt{}
	for _, v := range views {
		sql := fmt.Sprintf("CREATE VIEW %s.%s AS %s", QuoteIdent(v.Schema), QuoteIdent(v.Name), v.Def)
		st := Stmt{SQL: sql, Desc: "视图 " + v.Schema + "." + v.Name, Kind: "view"}
		out = append(out, st)
		if len(v.RelOpts) > 0 {
			out = append(out, Stmt{
				SQL:  fmt.Sprintf("ALTER VIEW %s.%s SET (%s)", QuoteIdent(v.Schema), QuoteIdent(v.Name), strings.Join(v.RelOpts, ", ")),
				Desc: "视图参数 " + v.Schema + "." + v.Name, Kind: "viewopt",
			})
		}
	}
	return out
}

func matviewStmts(views []ViewInfo) []Stmt {
	out := []Stmt{}
	for _, v := range views {
		sql := fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s AS %s WITH NO DATA", QuoteIdent(v.Schema), QuoteIdent(v.Name), v.Def)
		out = append(out, Stmt{SQL: sql, Desc: "物化视图 " + v.Schema + "." + v.Name, Kind: "matview"})
	}
	return out
}

func rlsStmts(tables []*TableInfo) []Stmt {
	out := []Stmt{}
	for _, t := range tables {
		if t.RLS {
			out = append(out, Stmt{
				SQL:  fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", t.QName()),
				Desc: "行级安全 " + t.Schema + "." + t.Name, Kind: "rls",
			})
		}
		if t.ForceRLS {
			out = append(out, Stmt{
				SQL:  fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", t.QName()),
				Desc: "强制行级安全 " + t.Schema + "." + t.Name, Kind: "rls",
			})
		}
	}
	return out
}

// hasOldStyleChildren tells whether any non-partition table inherits from t.
func hasOldStyleChildren(t *TableInfo, cat *Catalog) bool {
	for _, ch := range cat.Inherits[t.OID] {
		if ch == t.OID {
			continue
		}
		return true
	}
	return false
}

func constraintStmts(cat *Catalog) (pre []Stmt, fks []Stmt) {
	tblByKey := map[string]*TableInfo{}
	for _, t := range cat.Tables {
		tblByKey[t.Schema+"."+t.Name] = t
	}
	mk := func(con ConstraintInfo, onlyOK bool) Stmt {
		t := tblByKey[con.Schema+"."+con.Table]
		q := QuoteIdent(con.Schema) + "." + QuoteIdent(con.Table)
		useOnly := false
		if t != nil && con.TabKind != 'p' {
			if hasOldStyleChildren(t, cat) {
				useOnly = true
			}
		}
		_ = onlyOK
		kindName := map[byte]string{'p': "主键", 'u': "唯一", 'c': "检查", 'f': "外键"}[con.Type]
		if kindName == "" {
			kindName = "约束"
		}
		only := ""
		if useOnly {
			only = "ONLY "
		}
		return Stmt{
			SQL:  fmt.Sprintf("ALTER TABLE %s%s ADD CONSTRAINT %s %s", only, q, QuoteIdent(con.Name), con.Def),
			Desc: fmt.Sprintf("%s %s.%s.%s", kindName, con.Schema, con.Table, con.Name),
			Kind: "constraint",
		}
	}
	for _, con := range cat.Constraints {
		if con.Type == 'f' {
			continue
		}
		pre = append(pre, mk(con, true))
	}
	for _, con := range cat.Constraints {
		if con.Type == 'f' {
			fks = append(fks, mk(con, false))
		}
	}
	return pre, fks
}

func injectOnly(def string, tabSch, tabName string) string {
	qq := QuoteIdent(tabSch) + "." + QuoteIdent(tabName)
	if strings.Contains(def, " ON ONLY "+qq+" ") || strings.Contains(def, " ON ONLY "+qq+"(") {
		return def
	}
	for _, v := range []string{qq, QuoteIdent(tabName)} {
		if strings.Contains(def, " ON "+v+" ") {
			return strings.Replace(def, " ON "+v+" ", " ON ONLY "+v+" ", 1)
		}
		if strings.Contains(def, " ON "+v+"(") {
			return strings.Replace(def, " ON "+v+"(", " ON ONLY "+v+"(", 1)
		}
	}
	return def
}

func indexStmts(cat *Catalog) []Stmt {
	idxByOid := map[uint32]*IndexInfo{}
	for i := range cat.Indexes {
		idxByOid[cat.Indexes[i].OIDRef] = &cat.Indexes[i]
	}
	var parents, creates, attaches []Stmt
	for i := range cat.Indexes {
		ix := &cat.Indexes[i]
		if ix.Attach {
			// leaf index on a partition: create it under its own name first
			// (pg_dump order: parent ON ONLY -> leaf indexes -> ATTACH), then
			// attach it to the partitioned parent index.
			creates = append(creates, Stmt{SQL: ix.Def, Desc: "分区子索引 " + ix.Schema + "." + ix.Name, Kind: "index"})
			p := idxByOid[ix.ParentIx]
			pq := ""
			if p != nil {
				pq = QuoteIdent(p.Schema) + "." + QuoteIdent(p.Name)
			}
			if pq == "" {
				continue
			}
			attaches = append(attaches, Stmt{
				SQL:  fmt.Sprintf("ALTER INDEX %s ATTACH PARTITION %s.%s", pq, QuoteIdent(ix.Schema), QuoteIdent(ix.Name)),
				Desc: "挂接索引 " + ix.Schema + "." + ix.Name, Kind: "indexattach",
			})
			continue
		}
		def := ix.Def
		if ix.TabKind == 'p' {
			def = injectOnly(def, ix.TabSch, ix.TabName)
			parents = append(parents, Stmt{SQL: def, Desc: "分区索引 " + ix.Schema + "." + ix.Name, Kind: "index"})
		} else {
			creates = append(creates, Stmt{SQL: def, Desc: "索引 " + ix.Schema + "." + ix.Name, Kind: "index"})
		}
	}
	out := append(parents, creates...)
	out = append(out, attaches...)
	return out
}

func triggerStmts(triggers []TriggerInfo) []Stmt {
	out := []Stmt{}
	for _, tr := range triggers {
		out = append(out, Stmt{SQL: tr.Def, Desc: "触发器 " + tr.Schema, Kind: "trigger"})
	}
	return out
}

func policyStmts(policies []PolicyInfo, tables []*TableInfo) []Stmt {
	out := []Stmt{}
	tblByKey := map[string]bool{}
	for _, t := range tables {
		tblByKey[t.Schema+"."+t.Name] = true
	}
	for _, p := range policies {
		if !tblByKey[p.Schema+"."+p.Table] {
			continue
		}
		q := QuoteIdent(p.Schema) + "." + QuoteIdent(p.Table)
		perm := "PERMISSIVE"
		if !p.Permissive {
			perm = "RESTRICTIVE"
		}
		cmd := strings.ToUpper(strings.TrimSpace(p.Cmd))
		if cmd == "" {
			cmd = "ALL"
		}
		var b strings.Builder
		fmt.Fprintf(&b, "CREATE POLICY %s ON %s AS %s FOR %s", QuoteIdent(p.Name), q, perm, cmd)
		if len(p.Roles) > 0 {
			roles := make([]string, 0, len(p.Roles))
			for _, r := range p.Roles {
				roles = append(roles, QuoteIdent(strings.TrimSpace(r)))
			}
			fmt.Fprintf(&b, " TO %s", strings.Join(roles, ", "))
		}
		if p.Qual != "" {
			fmt.Fprintf(&b, " USING (%s)", p.Qual)
		}
		if p.WithCheck != "" {
			fmt.Fprintf(&b, " WITH CHECK (%s)", p.WithCheck)
		}
		out = append(out, Stmt{SQL: b.String(), Desc: "策略 " + p.Schema + "." + p.Table + "." + p.Name, Kind: "policy"})
	}
	return out
}

func seqOwnStmts(owns []SeqOwn) []Stmt {
	out := []Stmt{}
	for _, so := range owns {
		sql := fmt.Sprintf("ALTER SEQUENCE %s.%s OWNED BY %s.%s.%s",
			QuoteIdent(so.SeqSch), QuoteIdent(so.SeqName),
			QuoteIdent(so.TabSch), QuoteIdent(so.TabName), QuoteIdent(so.ColName))
		out = append(out, Stmt{SQL: sql, Desc: "序列归属 " + so.SeqSch + "." + so.SeqName, Kind: "seqown"})
	}
	return out
}

func commentStmts(comments []string) []Stmt {
	out := []Stmt{}
	for _, c := range comments {
		out = append(out, Stmt{SQL: c, Desc: "注释", Kind: "comment"})
	}
	return out
}

func roleStmts(roles []RoleInfo, existing map[string]bool) []Stmt {
	out := []Stmt{}
	for _, r := range roles {
		if r.Name == "postgres" || existing[r.Name] {
			continue
		}
		login := "NOLOGIN"
		if r.Login {
			login = "LOGIN"
		}
		su := "NOSUPERUSER"
		if r.Super {
			su = "SUPERUSER"
		}
		cdb := "NOCREATEDB"
		if r.CreateDB {
			cdb = "CREATEDB"
		}
		crl := "NOCREATEROLE"
		if r.CreateRole {
			crl = "CREATEROLE"
		}
		inh := "NOINHERIT"
		if r.Inherit {
			inh = "INHERIT"
		}
		rep := "NOREPLICATION"
		if r.Replication {
			rep = "REPLICATION"
		}
		brls := "NOBYPASSRLS"
		if r.BypassRLS {
			brls = "BYPASSRLS"
		}
		sql := fmt.Sprintf("CREATE ROLE %s WITH %s %s %s %s %s %s %s CONNECTION LIMIT %d",
			QuoteIdent(r.Name), login, su, cdb, crl, inh, rep, brls, r.ConnLimit)
		if r.Password != "" {
			sql += " PASSWORD " + QuoteLit(r.Password)
		}
		if r.ValidUntil != "" {
			sql += " VALID UNTIL " + QuoteLit(r.ValidUntil)
		}
		out = append(out, Stmt{SQL: sql, Desc: "角色 " + r.Name, Kind: "role"})
	}
	return out
}

func ownerStmts(cat *Catalog, skipRoles map[string]bool) []Stmt {
	out := []Stmt{}
	add := func(sql, desc string) {
		out = append(out, Stmt{SQL: sql, Desc: desc, Kind: "owner"})
	}
	roleOK := func(role string) bool { return role != "" && !skipRoles[role] }
	for _, t := range cat.Tables {
		if roleOK(t.Owner) {
			add(fmt.Sprintf("ALTER TABLE %s OWNER TO %s", t.QName(), QuoteIdent(t.Owner)), "属主 表 "+t.Schema+"."+t.Name)
		}
	}
	for _, s := range cat.Schemas {
		if roleOK(s.Owner) {
			add(fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", QuoteIdent(s.Name), QuoteIdent(s.Owner)), "属主 模式 "+s.Name)
		}
	}
	for _, v := range cat.Views {
		if roleOK(v.Owner) {
			kind := "VIEW"
			if v.IsMat {
				kind = "MATERIALIZED VIEW"
			}
			add(fmt.Sprintf("ALTER %s %s.%s OWNER TO %s", kind,
				QuoteIdent(v.Schema), QuoteIdent(v.Name), QuoteIdent(v.Owner)),
				"属主 视图 "+v.Schema+"."+v.Name)
		}
	}
	for _, f := range cat.Funcs {
		if roleOK(f.Owner) {
			add(fmt.Sprintf("ALTER FUNCTION %s.%s(%s) OWNER TO %s",
				QuoteIdent(f.Schema), QuoteIdent(f.Name), f.Args, QuoteIdent(f.Owner)), "属主 函数 "+f.Schema+"."+f.Name)
		}
	}
	for _, s := range cat.Seqs {
		if roleOK(s.Owner) {
			add(fmt.Sprintf("ALTER SEQUENCE %s.%s OWNER TO %s",
				QuoteIdent(s.Schema), QuoteIdent(s.Name), QuoteIdent(s.Owner)), "属主 序列 "+s.Schema+"."+s.Name)
		}
	}
	return out
}

func aclStmts(grants []ACLGrant) []Stmt {
	out := []Stmt{}
	for _, g := range grants {
		privs := make([]string, 0, len(g.Privs))
		seen := map[string]bool{}
		for _, p := range g.Privs {
			p = strings.TrimSpace(strings.ToUpper(p))
			if p != "" && !seen[p] {
				seen[p] = true
				privs = append(privs, p)
			}
		}
		if len(privs) == 0 {
			continue
		}
		go_ := ""
		if g.GrantOpt {
			go_ = " WITH GRANT OPTION"
		}
		sql := fmt.Sprintf("GRANT %s ON %s TO %s%s",
			strings.Join(privs, ", "), g.Object, QuoteIdent(g.Role), go_)
		out = append(out, Stmt{SQL: sql, Desc: "授权 " + g.Object + " → " + g.Role, Kind: "acl"})
	}
	return out
}

// dbSettingStmts builds ALTER DATABASE statements from source settings.
func dbSettingStmts(db string, settings []string) []Stmt {
	out := []Stmt{}
	for _, s := range settings {
		i := strings.Index(s, "=")
		if i <= 0 {
			continue
		}
		k := strings.TrimSpace(s[:i])
		v := strings.TrimSpace(s[i+1:])
		if k == "" {
			continue
		}
		out = append(out, Stmt{
			SQL:  fmt.Sprintf("ALTER DATABASE %s SET %s = %s", QuoteIdent(db), QuoteIdent(k), QuoteLit(v)),
			Desc: "库参数 " + k, Kind: "dbsetting",
		})
	}
	return out
}

// createDatabaseSQL builds a CREATE DATABASE statement.
func createDatabaseSQL(name, encoding, collate, ctype string) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "CREATE DATABASE %s TEMPLATE template0", QuoteIdent(name))
	if encoding != "" {
		fmt.Fprintf(b, " ENCODING %s", QuoteLit(encoding))
	}
	if collate != "" && ctype != "" {
		fmt.Fprintf(b, " LC_COLLATE %s LC_CTYPE %s", QuoteLit(collate), QuoteLit(ctype))
	}
	return b.String()
}
