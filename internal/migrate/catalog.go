package migrate

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"pgmigrate/internal/pgwire"
)

// ---- catalog objects ----

type ExtInfo struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Schema      string `json:"schema"`
	Relocatable bool   `json:"relocatable"`
}

type SchemaInfo struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

type EnumInfo struct {
	Schema string   `json:"schema"`
	Name   string   `json:"name"`
	Labels []string `json:"labels"`
}

type DomainInfo struct {
	Schema  string   `json:"schema"`
	Name    string   `json:"name"`
	Base    string   `json:"base"`
	NotNull bool     `json:"notnull"`
	Default string   `json:"default"`
	Coll    string   `json:"coll"`
	Checks  []string `json:"checks"`
}

type CompositeInfo struct {
	Schema string   `json:"schema"`
	Name   string   `json:"name"`
	Cols   []string `json:"cols"` // "name type"
}

type RangeInfo struct {
	Schema  string `json:"schema"`
	Name    string `json:"name"`
	Subtype string `json:"subtype"`
}

type CollationInfo struct {
	Schema    string `json:"schema"`
	Name      string `json:"name"`
	Provider  string `json:"provider"` // d=default c=libc i=icu
	LCCollate string `json:"lccollate"`
	LCCTYPE   string `json:"lcctype"`
}

type FuncInfo struct {
	OID    uint32 `json:"-"`
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	Args   string `json:"args"`
	Kind   byte   `json:"-"`
	Def    string `json:"def"` // full CREATE statement
}

type AggInfo struct {
	Schema   string `json:"schema"`
	Name     string `json:"name"`
	Args     string `json:"args"`
	TransFn  string `json:"transfn"`
	FinalFn  string `json:"finalfn"`
	SType    string `json:"stype"`
	InitCond string `json:"initcond"`
}

type SeqInfo struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	Typ    string `json:"typ"`
	Start  int64  `json:"start"`
	Incr   int64  `json:"incr"`
	MaxV   string `json:"max"`
	MinV   string `json:"min"`
	Cache  int64  `json:"cache"`
	Cycle  bool   `json:"cycle"`
}

// SeqOwn is a sequence "OWNED BY" relation.
type SeqOwn struct {
	SeqSch  string `json:"seqsch"`
	SeqName string `json:"seqname"`
	TabSch  string `json:"tabsch"`
	TabName string `json:"tabname"`
	ColName string `json:"colname"`
}

type ColInfo struct {
	Num      int16  `json:"num"`
	Name     string `json:"name"`
	Typ      string `json:"typ"`
	Default  string `json:"default"`
	Coll     string `json:"coll"`
	NotNull  bool   `json:"notnull"`
	Identity string `json:"identity"` // '' | a | d
}

type TableInfo struct {
	OID          uint32    `json:"-"`
	Schema       string    `json:"schema"`
	Name         string    `json:"name"`
	Kind         byte      `json:"kind"` // r | p
	IsPartition  bool      `json:"ispartition"`
	Persistence  byte      `json:"persistence"` // p | u
	RLS          bool      `json:"rls"`
	ForceRLS     bool      `json:"forcerls"`
	HasOIDs      bool      `json:"hasoids"`
	Owner        string    `json:"owner"`
	PartKey      string    `json:"partkey"`
	PartBound    string    `json:"partbound"`
	RelOpts      []string  `json:"relopts"`
	EstRows      int64     `json:"estrows"`
	SizeBytes    int64     `json:"sizebytes"`
	Columns      []ColInfo `json:"columns"`
	ParentOID    uint32    `json:"-"`
	ParentIsPart bool      `json:"-"`
	ParentNames  []string  `json:"-"` // old-style INHERITS parents (qualified)
	PartOf       uint32    `json:"-"` // partition parent oid
	ExtOwned     bool      `json:"-"` // created by an extension (e.g. PostGIS)
	MergeSafe    bool      `json:"-"` // data merge whitelisted (spatial_ref_sys)
	PartStrategy string    `json:"-"` // r=range h=hash l=list (partitioned parents)
	PartKeyCols  []string  `json:"-"` // partition key column names (column keys only)
}

func (t *TableInfo) QName() string { return QuoteIdent(t.Schema) + "." + QuoteIdent(t.Name) }

// Key returns the "schema.table" identifier.
func (t *TableInfo) Key() string { return t.Schema + "." + t.Name }

type ViewInfo struct {
	Schema   string   `json:"schema"`
	Name     string   `json:"name"`
	Owner    string   `json:"owner"`
	Def      string   `json:"def"`
	RelOpts  []string `json:"relopts"`
	IsMat    bool     `json:"ismat"`
	EstRows  int64    `json:"-"`
}

type IndexInfo struct {
	OIDRef   uint32 `json:"-"`
	Schema   string `json:"schema"`
	Name     string `json:"name"`
	Def      string `json:"def"` // full CREATE INDEX statement (from source)
	TabSch   string `json:"tabsch"`
	TabName  string `json:"tabname"`
	TabKind  byte   `json:"tabkind"`
	ParentIx uint32 `json:"-"` // index-inherits parent (partitioned index)
	Attach   bool   `json:"-"` // child of a partitioned index -> ATTACH instead of CREATE
}

type ConstraintInfo struct {
	Schema  string `json:"schema"`
	Table   string `json:"table"`
	Name    string `json:"name"`
	Type    byte   `json:"type"` // p u c f
	Def     string `json:"def"`
	TabKind byte   `json:"tabkind"`
}

type TriggerInfo struct {
	Schema string `json:"schema"`
	Def    string `json:"def"`
}

type PolicyInfo struct {
	Schema     string   `json:"schema"`
	Table      string   `json:"table"`
	Name       string   `json:"name"`
	Permissive bool     `json:"permissive"`
	Roles      []string `json:"roles"`
	Cmd        string   `json:"cmd"`
	Qual       string   `json:"qual"`
	WithCheck  string   `json:"withcheck"`
}

type ACLGrant struct {
	Object   string   `json:"object"` // e.g. TABLE public.t1 / FUNCTION f(a) / SCHEMA s
	Role     string   `json:"role"`
	Privs    []string `json:"privs"`
	GrantOpt bool     `json:"grantopt"`
}

type DbInfo struct {
	Name     string `json:"name"`
	Encoding string `json:"encoding"`
	EncID    int32  `json:"encid"`
	Collate  string `json:"collate"`
	Ctype    string `json:"ctype"`
	Size     int64  `json:"size"`
}

type Catalog struct {
	DB          DbInfo
	Exts        []ExtInfo
	Schemas     []SchemaInfo
	Collations  []CollationInfo
	Enums       []EnumInfo
	Domains     []DomainInfo
	Composites  []CompositeInfo
	Ranges      []RangeInfo
	Funcs       []FuncInfo
	Aggs        []AggInfo
	Seqs        []SeqInfo
	Tables      []*TableInfo
	ExtTables   []*TableInfo // extension-owned tables (data merge for whitelisted ones)
	Views       []ViewInfo
	Matviews    []ViewInfo
	Indexes     []IndexInfo
	Constraints []ConstraintInfo
	Triggers    []TriggerInfo
	Policies    []PolicyInfo
	Comments    []string // ready-to-run COMMENT statements
	ACLs        []ACLGrant
	DBSettings  []string // "key=value"
	SeqOwns     []SeqOwn
	Inherits    map[uint32][]uint32 // parent oid -> child oids (tables)
	IndexInh    map[uint32][]uint32 // partitioned index oid -> child index oids
	Roles       []RoleInfo
	HasForeignTables int
	HasLargeObjects  bool
}

type RoleInfo struct {
	Name        string `json:"name"`
	Super       bool   `json:"super"`
	CreateDB    bool   `json:"createdb"`
	CreateRole  bool   `json:"createrole"`
	Inherit     bool   `json:"inherit"`
	Login       bool   `json:"login"`
	Replication bool   `json:"replication"`
	BypassRLS   bool   `json:"bypassrls"`
	ConnLimit   int32  `json:"connlimit"`
	Password    string `json:"-"`
	ValidUntil  string `json:"validuntil"`
}

// ---- helpers ----

// QuoteIdent quotes an SQL identifier.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` 
}

// QuoteLit quotes an SQL string literal.
func QuoteLit(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

const sysFilter = "n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%'"

func notExtDep(objid, classid string) string {
	return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = %s AND d.objid = %s AND d.deptype = 'e')", classid, objid)
}

// extDepOnly is the inverse of notExtDep: matches objects OWNED by an extension.
func extDepOnly(objid, classid string) string {
	return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = %s AND d.objid = %s AND d.deptype = 'e')", classid, objid)
}

func str(r *pgwire.Result, i, j int) string { return r.Col(i, j) }
func isNull(r *pgwire.Result, i, j int) bool { return r.ColPtr(i, j) == nil }
func boolean(r *pgwire.Result, i, j int) bool { return str(r, i, j) == "t" || str(r, i, j) == "true" }
func int64v(r *pgwire.Result, i, j int) int64 {
	s := strings.TrimSpace(str(r, i, j))
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// queryFallback tries each SQL in order until one succeeds (for version-dependent columns).
func queryFallback(c *pgwire.Conn, sqls ...string) (*pgwire.Result, error) {
	var lastErr error
	for _, s := range sqls {
		r, err := c.Query(s)
		if err == nil {
			return r, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// matchPattern matches "schema.table" patterns; supports * and ?.
func matchPattern(pattern, schema, table string) bool {
	return matchOne(pattern, schema+"."+table)
}

func matchOne(pattern, s string) bool {
	if pattern == "" {
		return false
	}
	if ok, err := path.Match(pattern, s); err == nil {
		if ok {
			return true
		}
	}
	// case-insensitive retry
	ok2, _ := path.Match(strings.ToLower(pattern), strings.ToLower(s))
	return ok2
}

func (o *Options) tableAllowed(schema, table string) bool {
	if len(o.IncludeTables) > 0 {
		ok := false
		for _, p := range o.IncludeTables {
			if matchPattern(p, schema, table) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	for _, p := range o.ExcludeTables {
		if matchPattern(p, schema, table) {
			return false
		}
	}
	return true
}

// ---- collection ----

func CollectCatalog(c *pgwire.Conn, o *Options) (*Catalog, error) {
	cat := &Catalog{Inherits: map[uint32][]uint32{}, IndexInh: map[uint32][]uint32{}}

	// db info
	r, err := c.Query(`SELECT d.datname, pg_encoding_to_char(d.encoding), d.encoding, d.datcollate, d.datctype,
		pg_database_size(d.oid) FROM pg_database d WHERE d.datname = current_database()`)
	if err != nil {
		return nil, fmt.Errorf("读取源库信息失败: %w", err)
	}
	if len(r.Rows) > 0 {
		cat.DB = DbInfo{
			Name: str(r, 0, 0), Encoding: str(r, 0, 1),
			EncID: int32(int64v(r, 0, 2)), Collate: str(r, 0, 3), Ctype: str(r, 0, 4), Size: int64v(r, 0, 5),
		}
	}

	// extensions
	r, err = c.Query(`SELECT e.extname, e.extversion, n.nspname, e.extrelocatable FROM pg_extension e
		JOIN pg_namespace n ON n.oid = e.extnamespace ORDER BY e.extname`)
	if err == nil {
		for i := range r.Rows {
			cat.Exts = append(cat.Exts, ExtInfo{Name: str(r, i, 0), Version: str(r, i, 1), Schema: str(r, i, 2), Relocatable: boolean(r, i, 3)})
		}
	} else {
		return nil, fmt.Errorf("读取扩展列表失败: %w", err)
	}

	// schemas
	r, err = c.Query(`SELECT n.nspname, pg_get_userbyid(n.nspowner) FROM pg_namespace n
		WHERE ` + sysFilter + ` AND ` + notExtDep("n.oid", "'pg_namespace'::regclass") + ` ORDER BY n.nspname`)
	if err != nil {
		return nil, fmt.Errorf("读取模式列表失败: %w", err)
	}
	for i := range r.Rows {
		cat.Schemas = append(cat.Schemas, SchemaInfo{Name: str(r, i, 0), Owner: str(r, i, 1)})
	}

	// enums
	r, err = c.Query(`SELECT n.nspname, t.typname,
		(SELECT string_agg(e.enumlabel, E'\x01' ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid = t.oid)
		FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype = 'e' AND ` + sysFilter + ` AND ` + notExtDep("t.oid", "'pg_type'::regclass") + `
		ORDER BY n.nspname, t.typname`)
	if err != nil {
		return nil, fmt.Errorf("读取枚举类型失败: %w", err)
	}
	for i := range r.Rows {
		lbls := []string{}
		if s := str(r, i, 2); s != "" {
			lbls = strings.Split(s, "\x01")
		}
		cat.Enums = append(cat.Enums, EnumInfo{Schema: str(r, i, 0), Name: str(r, i, 1), Labels: lbls})
	}

	// domains
	r, err = c.Query(`SELECT n.nspname, t.typname, format_type(t.typbasetype, t.typtypmod), t.typnotnull,
		COALESCE(pg_get_expr(t.typdefaultbin, 0), ''),
		COALESCE((SELECT string_agg(pg_get_constraintdef(x.oid, true), E'\x01') FROM pg_constraint x WHERE x.contypid = t.oid), ''),
		COALESCE((SELECT c2.collname FROM pg_collation c2 WHERE c2.oid = t.typcollation), '')
		FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype = 'd' AND ` + sysFilter + ` AND ` + notExtDep("t.oid", "'pg_type'::regclass") + `
		ORDER BY n.nspname, t.typname`)
	if err != nil {
		return nil, fmt.Errorf("读取域类型失败: %w", err)
	}
	for i := range r.Rows {
		var checks []string
		if s := str(r, i, 5); s != "" {
			checks = strings.Split(s, "\x01")
		}
		cat.Domains = append(cat.Domains, DomainInfo{
			Schema: str(r, i, 0), Name: str(r, i, 1), Base: str(r, i, 2), NotNull: boolean(r, i, 3),
			Default: str(r, i, 4), Checks: checks, Coll: str(r, i, 6),
		})
	}

	// composites (standalone only, not table row types)
	r, err = c.Query(`SELECT n.nspname, t.typname,
		(SELECT string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
		 FROM pg_attribute a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped)
		FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class tc ON tc.oid = t.typrelid AND tc.relkind = 'c'
		WHERE t.typtype = 'c' AND ` + sysFilter + ` AND ` + notExtDep("t.oid", "'pg_type'::regclass") + `
		ORDER BY n.nspname, t.typname`)
	if err != nil {
		return nil, fmt.Errorf("读取复合类型失败: %w", err)
	}
	for i := range r.Rows {
		var cols []string
		if s := str(r, i, 2); s != "" {
			cols = strings.Split(s, ", ")
		}
		cat.Composites = append(cat.Composites, CompositeInfo{Schema: str(r, i, 0), Name: str(r, i, 1), Cols: cols})
	}

	// ranges
	r, err = c.Query(`SELECT n.nspname, t.typname, format_type(r.rngsubtype, NULL)
		FROM pg_range r JOIN pg_type t ON t.oid = r.rngtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE ` + sysFilter + ` AND ` + notExtDep("t.oid", "'pg_type'::regclass") + `
		ORDER BY n.nspname, t.typname`)
	if err != nil {
		return nil, fmt.Errorf("读取范围类型失败: %w", err)
	}
	for i := range r.Rows {
		cat.Ranges = append(cat.Ranges, RangeInfo{Schema: str(r, i, 0), Name: str(r, i, 1), Subtype: str(r, i, 2)})
	}

	// collations
	r, err = c.Query(`SELECT n.nspname, c.collname, c.collprovider, COALESCE(c.collcollate, ''), COALESCE(c.collctype, '')
		FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace
		WHERE ` + sysFilter + ` AND ` + notExtDep("c.oid", "'pg_collation'::regclass") + `
		ORDER BY n.nspname, c.collname`)
	if err != nil {
		return nil, fmt.Errorf("读取排序规则失败: %w", err)
	}
	for i := range r.Rows {
		cat.Collations = append(cat.Collations, CollationInfo{
			Schema: str(r, i, 0), Name: str(r, i, 1), Provider: str(r, i, 2),
			LCCollate: str(r, i, 3), LCCTYPE: str(r, i, 4),
		})
	}

	// functions (non-aggregate)
	fnFilter := sysFilter + ` AND ` + notExtDep("p.oid", "'pg_proc'::regclass")
	r, err = c.Query(`SELECT p.oid, n.nspname, p.proname, pg_get_userbyid(p.proowner),
		pg_get_function_identity_arguments(p.oid), p.prokind, pg_get_functiondef(p.oid)
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE p.prokind <> 'a' AND ` + fnFilter + `
		ORDER BY n.nspname, p.proname`)
	if err != nil {
		return nil, fmt.Errorf("读取函数失败: %w", err)
	}
	for i := range r.Rows {
		cat.Funcs = append(cat.Funcs, FuncInfo{
			OID: uint32(int64v(r, i, 0)), Schema: str(r, i, 1), Name: str(r, i, 2), Owner: str(r, i, 3),
			Args: str(r, i, 4), Kind: str(r, i, 5)[0], Def: str(r, i, 6),
		})
	}

	// aggregates
	r, err = c.Query(`SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid),
		a.aggtransfn::regprocedure::text,
		CASE WHEN a.aggfinalfn = 0 THEN '' ELSE a.aggfinalfn::regprocedure::text END,
		format_type(a.aggtranstype, NULL), COALESCE(a.agginitval, '')
		FROM pg_aggregate a JOIN pg_proc p ON p.oid = a.aggfnoid
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE ` + fnFilter + `
		ORDER BY n.nspname, p.proname`)
	if err != nil {
		return nil, fmt.Errorf("读取聚合函数失败: %w", err)
	}
	for i := range r.Rows {
		cat.Aggs = append(cat.Aggs, AggInfo{
			Schema: str(r, i, 0), Name: str(r, i, 1), Args: str(r, i, 2),
			TransFn: str(r, i, 3), FinalFn: str(r, i, 4), SType: str(r, i, 5), InitCond: str(r, i, 6),
		})
	}

	// sequences (identity-owned sequences are excluded: the target creates them
	// automatically with the GENERATED AS IDENTITY column, and they can neither
	// be pre-created by name nor re-owned)
	r, err = c.Query(`SELECT n.nspname, c.relname, pg_get_userbyid(c.relowner), format_type(s.seqtypid, NULL), s.seqstart, s.seqincrement,
		s.seqmax::text, s.seqmin::text, s.seqcache, s.seqcycle
		FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE ` + sysFilter + ` AND ` + notExtDep("c.oid", "'pg_class'::regclass") + `
		AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dd WHERE dd.objid = c.oid
			AND dd.classid = 'pg_class'::regclass AND dd.refclassid = 'pg_class'::regclass AND dd.deptype = 'i')
		ORDER BY n.nspname, c.relname`)
	if err != nil {
		return nil, fmt.Errorf("读取序列失败: %w", err)
	}
	for i := range r.Rows {
		cat.Seqs = append(cat.Seqs, SeqInfo{
			Schema: str(r, i, 0), Name: str(r, i, 1), Owner: str(r, i, 2), Typ: str(r, i, 3),
			Start: int64v(r, i, 4), Incr: int64v(r, i, 5), MaxV: str(r, i, 6), MinV: str(r, i, 7),
			Cache: int64v(r, i, 8), Cycle: boolean(r, i, 9),
		})
	}

	// tables (extension-owned tables are collected separately, see ExtTables)
	tblSQL := `SELECT c.oid, n.nspname, c.relname, c.relkind, c.relispartition, c.relpersistence,
		c.relrowsecurity, c.relforcerowsecurity, ` + "%HASOIDS%" + `
		pg_get_userbyid(c.relowner),
		COALESCE(pg_get_partkeydef(c.oid), ''),
		COALESCE(pg_get_expr(c.relpartbound, c.oid), ''),
		COALESCE(array_to_string(c.reloptions, ','), ''),
		c.reltuples::bigint,
		COALESCE(pg_table_size(c.oid), 0)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p') AND ` + sysFilter + ` AND %EXTDEP% 
		ORDER BY n.nspname, c.relname`
	parseTableRows := func(r *pgwire.Result) []*TableInfo {
		hasOidsCol := -1
		for j, f := range r.Fields {
			if f.Name == "relhasoids" {
				hasOidsCol = j
			}
		}
		out := make([]*TableInfo, 0, len(r.Rows))
		for i := range r.Rows {
			t := &TableInfo{
				OID:         uint32(int64v(r, i, 0)),
				Schema:      str(r, i, 1), Name: str(r, i, 2), Kind: str(r, i, 3)[0],
				IsPartition: boolean(r, i, 4), Persistence: str(r, i, 5)[0],
				RLS: boolean(r, i, 6), ForceRLS: boolean(r, i, 7),
			}
			col := 9
			if hasOidsCol >= 0 {
				t.HasOIDs = boolean(r, i, 8)
			} else {
				col = 8
			}
			t.Owner = str(r, i, col)
			t.PartKey = str(r, i, col+1)
			t.PartBound = str(r, i, col+2)
			if s := str(r, i, col+3); s != "" {
				t.RelOpts = strings.Split(s, ",")
			}
			t.EstRows = int64v(r, i, col+4)
			t.SizeBytes = int64v(r, i, col+5)
			out = append(out, t)
		}
		return out
	}
	r, err = queryFallback(c,
		strings.Replace(strings.Replace(tblSQL, "%HASOIDS%", "c.relhasoids,", 1), "%EXTDEP%", notExtDep("c.oid", "'pg_class'::regclass"), 1),
		strings.Replace(strings.Replace(tblSQL, "%HASOIDS%", "", 1), "%EXTDEP%", notExtDep("c.oid", "'pg_class'::regclass"), 1))
	if err != nil {
		return nil, fmt.Errorf("读取表列表失败: %w", err)
	}
	for _, t := range parseTableRows(r) {
		// NOTE: scope filtering happens later in applyTableScope, which needs
		// the FULL table set (partition closure spans unselected tables).
		cat.Tables = append(cat.Tables, t)
	}

	// extension-owned tables (e.g. PostGIS public.spatial_ref_sys): the target
	// gets them from CREATE EXTENSION; only their DATA is merged (spatial_ref_sys
	// only, matching the official postgis_restore.pl merge semantics).
	r, err = queryFallback(c,
		strings.Replace(strings.Replace(tblSQL, "%HASOIDS%", "c.relhasoids,", 1), "%EXTDEP%", extDepOnly("c.oid", "'pg_class'::regclass"), 1),
		strings.Replace(strings.Replace(tblSQL, "%HASOIDS%", "", 1), "%EXTDEP%", extDepOnly("c.oid", "'pg_class'::regclass"), 1))
	if err == nil {
		for _, t := range parseTableRows(r) {
			t.ExtOwned = true
			t.MergeSafe = t.Name == "spatial_ref_sys" // whitelist: safe to merge like postgis_restore.pl
			cat.ExtTables = append(cat.ExtTables, t)
		}
	}
	byOid := map[uint32]*TableInfo{}
	for _, t := range cat.Tables {
		byOid[t.OID] = t
	}

	// partitioned-parent keys: structured rebuild (strategy + column names)
	r, err = c.Query(`SELECT pt.partrelid, pt.partstrat, a.attname
		FROM pg_partitioned_table pt
		JOIN LATERAL unnest(pt.partattrs) WITH ORDINALITY AS u(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = pt.partrelid AND a.attnum = u.attnum
		WHERE u.attnum > 0
		ORDER BY pt.partrelid, u.ord`)
	if err == nil {
		for i := range r.Rows {
			oid := uint32(int64v(r, i, 0))
			if t, ok := byOid[oid]; ok {
				t.PartStrategy = str(r, i, 1)
				t.PartKeyCols = append(t.PartKeyCols, str(r, i, 2))
			}
		}
	}

	// columns
	r, err = c.Query(`SELECT a.attrelid, a.attnum, a.attname, format_type(a.atttypid, a.atttypmod),
		a.attnotnull, a.attidentity::text,
		COALESCE(pg_get_expr(d.adbin, d.adrelid), ''),
		COALESCE((SELECT c2.collname FROM pg_collation c2 WHERE c2.oid = a.attcollation), '')
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		JOIN pg_class tc ON tc.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = tc.relnamespace
		WHERE a.attnum > 0 AND NOT a.attisdropped AND tc.relkind IN ('r','p') AND ` + sysFilter + `
		ORDER BY a.attrelid, a.attnum`)
	if err != nil {
		return nil, fmt.Errorf("读取列信息失败: %w", err)
	}
	for i := range r.Rows {
		oid := uint32(int64v(r, i, 0))
		if t, ok := byOid[oid]; ok {
			t.Columns = append(t.Columns, ColInfo{
				Num: int16(int64v(r, i, 1)), Name: str(r, i, 2), Typ: str(r, i, 3),
				NotNull: boolean(r, i, 4), Identity: str(r, i, 5), Default: str(r, i, 6), Coll: str(r, i, 7),
			})
		}
	}

	// inheritance edges (tables)
	r, err = c.Query(`SELECT i.inhparent, i.inhrelid FROM pg_inherits i`)
	if err != nil {
		return nil, fmt.Errorf("读取继承关系失败: %w", err)
	}
	childParent := map[uint32]uint32{}
	for i := range r.Rows {
		p := uint32(int64v(r, i, 0))
		ch := uint32(int64v(r, i, 1))
		cat.Inherits[p] = append(cat.Inherits[p], ch)
		childParent[ch] = p
	}
	for _, t := range cat.Tables {
		if p, ok := childParent[t.OID]; ok {
			t.ParentOID = p
			if pt, ok2 := byOid[p]; ok2 && pt.Kind == 'p' {
				t.PartOf = p
				t.ParentIsPart = true
			} else {
				// old-style inheritance parent (or partition parent not in our set)
				if pt != nil {
					t.ParentNames = append(t.ParentNames, pt.QName())
				}
			}
		}
	}

	// views
	r, err = c.Query(`SELECT n.nspname, c.relname, pg_get_userbyid(c.relowner), pg_get_viewdef(c.oid, true),
		COALESCE(array_to_string(c.reloptions, ','), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'v' AND ` + sysFilter + ` AND ` + notExtDep("c.oid", "'pg_class'::regclass") + `
		ORDER BY n.nspname, c.relname`)
	if err != nil {
		return nil, fmt.Errorf("读取视图失败: %w", err)
	}
	for i := range r.Rows {
		v := ViewInfo{Schema: str(r, i, 0), Name: str(r, i, 1), Owner: str(r, i, 2), Def: str(r, i, 3)}
		if s := str(r, i, 4); s != "" {
			v.RelOpts = strings.Split(s, ",")
		}
		cat.Views = append(cat.Views, v)
	}

	// materialized views
	r, err = c.Query(`SELECT n.nspname, c.relname, pg_get_userbyid(c.relowner), pg_get_viewdef(c.oid, true),
		COALESCE(array_to_string(c.reloptions, ','), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'm' AND ` + sysFilter + ` AND ` + notExtDep("c.oid", "'pg_class'::regclass") + `
		ORDER BY n.nspname, c.relname`)
	if err != nil {
		return nil, fmt.Errorf("读取物化视图失败: %w", err)
	}
	for i := range r.Rows {
		v := ViewInfo{Schema: str(r, i, 0), Name: str(r, i, 1), Owner: str(r, i, 2), Def: str(r, i, 3), IsMat: true}
		if s := str(r, i, 4); s != "" {
			v.RelOpts = strings.Split(s, ",")
		}
		cat.Matviews = append(cat.Matviews, v)
	}

	// indexes (skip constraint-backed; skip invalid)
	r, err = c.Query(`SELECT tin.nspname, ti.relname, pg_get_indexdef(i.indexrelid),
		n.nspname, t.relname, t.relkind, i.indexrelid
		FROM pg_index i
		JOIN pg_class ti ON ti.oid = i.indexrelid
		JOIN pg_namespace tin ON tin.oid = ti.relnamespace
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE i.indisvalid AND i.indisready AND t.relkind IN ('r','p','m')
		AND ` + sysFilter + ` AND ` + notExtDep("ti.oid", "'pg_class'::regclass") + `
		AND NOT EXISTS (SELECT 1 FROM pg_constraint cc WHERE cc.conindid = i.indexrelid)`)
	if err != nil {
		return nil, fmt.Errorf("读取索引失败: %w", err)
	}
	conIdxOids := map[uint32]bool{}
	// constraint-backed indexes are excluded above; remember them for child detection
	conIdxRows, _ := c.Query(`SELECT conindid FROM pg_constraint WHERE conindid <> 0`)
	if conIdxRows != nil {
		for i := range conIdxRows.Rows {
			conIdxOids[uint32(int64v(conIdxRows, i, 0))] = true
		}
	}
	for i := range r.Rows {
		cat.Indexes = append(cat.Indexes, IndexInfo{
			Schema: str(r, i, 0), Name: str(r, i, 1), Def: str(r, i, 2),
			TabSch: str(r, i, 3), TabName: str(r, i, 4), TabKind: str(r, i, 5)[0],
			OIDRef: uint32(int64v(r, i, 6)),
		})
	}
	// index inheritance (partitioned index parent -> child index)
	r, err = c.Query(`SELECT i.inhparent, i.inhrelid FROM pg_inherits i
		JOIN pg_class pi ON pi.oid = i.inhparent JOIN pg_class ci ON ci.oid = i.inhrelid
		WHERE pi.relkind = 'I' AND ci.relkind = 'i'`)
	if err != nil {
		return nil, fmt.Errorf("读取分区索引关系失败: %w", err)
	}
	idxChild := map[uint32]uint32{}
	for i := range r.Rows {
		p := uint32(int64v(r, i, 0))
		ch := uint32(int64v(r, i, 1))
		cat.IndexInh[p] = append(cat.IndexInh[p], ch)
		idxChild[ch] = p
	}
	// mark attach children and drop constraint-children from creation list
	finalIdx := cat.Indexes[:0]
	for _, ix := range cat.Indexes {
		oid := ix.OIDRef
		if p, ok := idxChild[oid]; ok {
			// child of a partitioned index: attach (unless the whole chain is constraint-backed)
			if !ancestorsHaveConstraint(idxChild, conIdxOids, oid) {
				ix.Attach = true
				ix.ParentIx = p
				finalIdx = append(finalIdx, ix)
			}
			continue
		}
		finalIdx = append(finalIdx, ix)
	}
	cat.Indexes = finalIdx

	// constraints
	r, err = queryFallback(c,
		`SELECT n.nspname, c.relname, con.conname, con.contype, pg_get_constraintdef(con.oid, true), c.relkind
		FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE con.conparentid = 0 AND ` + sysFilter,
		`SELECT n.nspname, c.relname, con.conname, con.contype, pg_get_constraintdef(con.oid, true), c.relkind
		FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE ` + sysFilter)
	if err != nil {
		return nil, fmt.Errorf("读取约束失败: %w", err)
	}
	for i := range r.Rows {
		cat.Constraints = append(cat.Constraints, ConstraintInfo{
			Schema: str(r, i, 0), Table: str(r, i, 1), Name: str(r, i, 2),
			Type: str(r, i, 3)[0], Def: str(r, i, 4), TabKind: str(r, i, 5)[0],
		})
	}

	// triggers
	r, err = c.Query(`SELECT n.nspname, pg_get_triggerdef(t.oid, true)
		FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT t.tgisinternal AND ` + sysFilter)
	if err != nil {
		return nil, fmt.Errorf("读取触发器失败: %w", err)
	}
	for i := range r.Rows {
		cat.Triggers = append(cat.Triggers, TriggerInfo{Schema: str(r, i, 0), Def: str(r, i, 1)})
	}

	// policies
	r, err = c.Query(`SELECT schemaname, tablename, policyname, permissive, array_to_string(roles, E'\x01'), cmd,
		COALESCE(qual, ''), COALESCE(with_check, '') FROM pg_policies ORDER BY schemaname, tablename, policyname`)
	if err == nil {
		for i := range r.Rows {
			roles := []string{}
			if s := str(r, i, 4); s != "" {
				roles = strings.Split(s, "\x01")
			}
			cat.Policies = append(cat.Policies, PolicyInfo{
				Schema: str(r, i, 0), Table: str(r, i, 1), Name: str(r, i, 2),
				Permissive: boolean(r, i, 3), Roles: roles, Cmd: str(r, i, 5),
				Qual: str(r, i, 6), WithCheck: str(r, i, 7),
			})
		}
	} // pg_policies missing => ignore

	// comments
	cat.Comments = collectComments(c)

	// db-level settings
	cat.DBSettings = collectDBSettings(c)

	// sequence OWNED BY relations (deptype 'a' only: identity sequences are
	// managed by the identity column itself)
	r, err = c.Query(`SELECT sn.nspname, sc.relname, tn.nspname, tc.relname, a.attname
		FROM pg_depend d
		JOIN pg_class sc ON sc.oid = d.objid JOIN pg_namespace sn ON sn.oid = sc.relnamespace
		JOIN pg_class tc ON tc.oid = d.refobjid JOIN pg_namespace tn ON tn.oid = tc.relnamespace
		JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = d.refobjsubid
		WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
		AND d.deptype = 'a' AND sc.relkind = 'S'`)
	if err == nil {
		for i := range r.Rows {
			cat.SeqOwns = append(cat.SeqOwns, SeqOwn{
				SeqSch: str(r, i, 0), SeqName: str(r, i, 1),
				TabSch: str(r, i, 2), TabName: str(r, i, 3), ColName: str(r, i, 4),
			})
		}
	}

	if o.MigrateRoles {
		cat.Roles = collectRoles(c)
	}
	if o.MigrateACLs {
		cat.ACLs = collectACLs(c)
	}

	// foreign tables (informational)
	r, err = c.Query(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'f' AND ` + sysFilter)
	if err == nil && len(r.Rows) > 0 {
		cat.HasForeignTables = int(int64v(r, 0, 0))
	}
	// large objects
	r, err = c.Query(`SELECT EXISTS(SELECT 1 FROM pg_largeobject LIMIT 1)`)
	if err == nil && len(r.Rows) > 0 {
		cat.HasLargeObjects = boolean(r, 0, 0)
	}

	sort.SliceStable(cat.Tables, func(a, b int) bool {
		if cat.Tables[a].Schema != cat.Tables[b].Schema {
			return cat.Tables[a].Schema < cat.Tables[b].Schema
		}
		return cat.Tables[a].Name < cat.Tables[b].Name
	})
	applyTableScope(cat, o)
	return cat, nil
}

// applyTableScope narrows the catalog down to the selected table set
// (Options.IncludeTables). When any tables are explicitly included, everything
// not related to them is dropped so that indexes/constraints/triggers/policies
// of unselected tables never generate failing DDL. Partition families are kept
// together: selecting a parent brings all its partitions, selecting a partition
// brings its parent chain (and sibling partitions stay out).
// When nothing is included, only the exclude patterns apply.
func applyTableScope(cat *Catalog, o *Options) {
	// build keep-map over the FULL table set
	keep := map[string]bool{}
	keepOID := map[uint32]bool{}
	for _, t := range cat.Tables {
		keep[t.Key()] = o.tableAllowed(t.Schema, t.Name)
		if keep[t.Key()] {
			keepOID[t.OID] = true
		}
	}
	if len(o.IncludeTables) > 0 {
		// partition closure over the full graph: child -> parent -> all children
		changed := true
		for changed {
			changed = false
			for _, t := range cat.Tables {
				k := t.Key()
				if keep[k] {
					continue
				}
				parentKept := t.PartOf != 0 && keepOID[t.PartOf]
				parentOldStyle := t.ParentOID != 0 && keepOID[t.ParentOID]
				childKept := false
				if t.Kind == 'p' {
					for _, c := range cat.Tables {
						if c.PartOf == t.OID && keep[c.Key()] {
							childKept = true
							break
						}
					}
				}
				if parentKept || parentOldStyle || childKept {
					keep[k] = true
					keepOID[t.OID] = true
					changed = true
				}
			}
		}
	} else {
		return // no includes: keep tableAllowed results (excludes only)
	}

	// tables
	keptTables := make([]*TableInfo, 0, len(cat.Tables))
	for _, t := range cat.Tables {
		if keep[t.Key()] {
			keptTables = append(keptTables, t)
		}
	}
	cat.Tables = keptTables
	keptExt := make([]*TableInfo, 0, len(cat.ExtTables))
	for _, t := range cat.ExtTables {
		if keep[t.Key()] {
			keptExt = append(keptExt, t)
		}
	}
	cat.ExtTables = keptExt

	// indexes / constraints / policies / triggers filter by owning table
	cat.Indexes = filterIndexes(cat.Indexes, keep)
	cat.Constraints = filterSlice(cat.Constraints, func(x ConstraintInfo) bool { return keep[x.Schema+"."+x.Table] })
	cat.Policies = filterSlice(cat.Policies, func(x PolicyInfo) bool { return keep[x.Schema+"."+x.Table] })
	cat.Triggers = filterTriggers(cat.Triggers, keep)

	// sequence OWNED BY: keep only when the owning table is selected
	cat.SeqOwns = filterSlice(cat.SeqOwns, func(x SeqOwn) bool { return keep[x.TabSch+"."+x.TabName] })
	// sequences owned by dropped tables lose their ALTER OWNER statement
	if len(o.IncludeTables) > 0 {
		owned := map[string]bool{}
		for _, so := range cat.SeqOwns {
			owned[so.SeqSch+"."+so.SeqName] = true
		}
		cat.Seqs = filterSlice(cat.Seqs, func(x SeqInfo) bool {
			return owned[x.Schema+"."+x.Name] || o.tableAllowed(x.Schema, x.Name)
		})
	}

	// comments mentioning dropped tables are filtered line-wise
	cat.Comments = filterComments(cat.Comments, keep)
	// views/matviews are NOT dropped (they may reference selected tables), but
	// keep views whose whole definition is unrelated? simplest: keep all views
	// (they were created successfully independent of table scope before).
}

func filterSlice[T any](items []T, pred func(T) bool) []T {
	out := make([]T, 0, len(items))
	for _, x := range items {
		if pred(x) {
			out = append(out, x)
		}
	}
	return out
}

func filterIndexes(indexes []IndexInfo, keep map[string]bool) []IndexInfo {
	out := make([]IndexInfo, 0, len(indexes))
	for _, ix := range indexes {
		if keep[ix.TabSch+"."+ix.TabName] {
			out = append(out, ix)
		}
	}
	return out
}

func filterTriggers(triggers []TriggerInfo, keep map[string]bool) []TriggerInfo {
	out := make([]TriggerInfo, 0, len(triggers))
	for _, tr := range triggers {
		// pg_get_triggerdef text contains "ON schema.table" — match loosely
		for k := range keep {
			qk := QuoteIdent(strings.SplitN(k, ".", 2)[0]) + "." + QuoteIdent(strings.SplitN(k, ".", 2)[1])
			if strings.Contains(tr.Def, " ON "+qk+" ") || strings.Contains(tr.Def, " ON ONLY "+qk+" ") ||
				strings.Contains(tr.Def, " ON "+qk+"(") {
				out = append(out, tr)
				break
			}
		}
	}
	return out
}

func filterComments(comments []string, keep map[string]bool) []string {
	out := make([]string, 0, len(comments))
	for _, c := range comments {
		match := false
		for k := range keep {
			sch, tab, _ := strings.Cut(k, ".")
			if strings.Contains(c, " ON TABLE "+QuoteIdent(sch)+"."+QuoteIdent(tab)+" ") ||
				strings.Contains(c, " ON COLUMN "+QuoteIdent(sch)+"."+QuoteIdent(tab)+".") ||
				strings.Contains(c, " ON CONSTRAINT ") && strings.Contains(c, " ON "+QuoteIdent(sch)+"."+QuoteIdent(tab)+" ") {
				match = true
				break
			}
		}
		// comments not tied to a table (types/functions/schemas) are kept
		if match || !tableBoundComment(c) {
			out = append(out, c)
		}
	}
	return out
}

func tableBoundComment(c string) bool {
	return strings.Contains(c, " ON TABLE ") || strings.Contains(c, " ON COLUMN ")
}

func ancestorsHaveConstraint(idxChild map[uint32]uint32, conIdx map[uint32]bool, oid uint32) bool {
	seen := map[uint32]bool{}
	for {
		p, ok := idxChild[oid]
		if !ok || seen[oid] {
			return false
		}
		seen[oid] = true
		if conIdx[p] {
			return true
		}
		oid = p
	}
}

// legacyLibRe matches $libdir references to OLD PostGIS major versions. When
// the target gets PostGIS 3.x from the image, these legacy C functions cannot
// be created (the .so is not in the container) and are fully superseded by the
// new extension's ST_* implementations — they must be skipped, not migrated.
var legacyLibRe = regexp.MustCompile(`\$libdir/(postgis-2|rtpostgis-2|topology-2|postgis_sfcgal-2)`)

// dropLegacyPostgisObjects removes functions/aggregates whose definition binds
// to a PostGIS 2.x shared library. Detected after extension install failures:
// if postgis could not be installed there is no geometry type anyway, and when
// it IS installed (3.x) these legacy stubs are obsolete.
func dropLegacyPostgisObjects(cat *Catalog) (funcs, aggs int) {
	keepF := make([]FuncInfo, 0, len(cat.Funcs))
	for _, f := range cat.Funcs {
		if legacyLibRe.MatchString(f.Def) {
			continue
		}
		keepF = append(keepF, f)
	}
	funcs = len(cat.Funcs) - len(keepF)
	cat.Funcs = keepF

	keepA := make([]AggInfo, 0, len(cat.Aggs))
	for _, a := range cat.Aggs {
		// aggregates reference their transition/final functions; legacy PostGIS
		// aggregates (e.g. st_accum) all live under the postgis-2 lib via their fns
		if legacyLibRe.MatchString(a.TransFn) || legacyLibRe.MatchString(a.FinalFn) {
			continue
		}
		keepA = append(keepA, a)
	}
	aggs = len(cat.Aggs) - len(keepA)
	cat.Aggs = keepA
	return funcs, aggs
}

func collectComments(c *pgwire.Conn) []string {
	out := []string{}
	q := func(sql string) {
		if r, err := c.Query(sql); err == nil {
			for i := range r.Rows {
				out = append(out, str(r, i, 0))
			}
		}
	}
	q(`SELECT format('COMMENT ON TABLE %I.%I IS %L', n.nspname, c.relname, obj_description(c.oid, 'pg_class'))
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p','v','m','S') AND obj_description(c.oid, 'pg_class') IS NOT NULL
		AND ` + sysFilter + ` AND ` + notExtDep("c.oid", "'pg_class'::regclass"))
	q(`SELECT format('COMMENT ON COLUMN %I.%I.%I IS %L', n.nspname, c.relname, a.attname, col_description(c.oid, a.attnum))
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r','p','v','m')
		AND col_description(c.oid, a.attnum) IS NOT NULL AND ` + sysFilter + ` AND ` + notExtDep("c.oid", "'pg_class'::regclass"))
	q(`SELECT format('COMMENT ON INDEX %I.%I IS %L', n.nspname, ti.relname, obj_description(ti.oid, 'pg_class'))
		FROM pg_index i JOIN pg_class ti ON ti.oid = i.indexrelid
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = ti.relnamespace
		WHERE obj_description(ti.oid, 'pg_class') IS NOT NULL AND ` + sysFilter + ` AND ` + notExtDep("ti.oid", "'pg_class'::regclass"))
	q(`SELECT format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L', con.conname, n.nspname, c.relname, obj_description(con.oid, 'pg_constraint'))
		FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE obj_description(con.oid, 'pg_constraint') IS NOT NULL AND con.conparentid = 0 AND ` + sysFilter)
	q(`SELECT format('COMMENT ON FUNCTION %I.%I(%s) IS %L', n.nspname, p.proname,
		pg_get_function_identity_arguments(p.oid), obj_description(p.oid, 'pg_proc'))
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE obj_description(p.oid, 'pg_proc') IS NOT NULL AND ` + sysFilter + ` AND ` + notExtDep("p.oid", "'pg_proc'::regclass"))
	q(`SELECT format('COMMENT ON TYPE %I.%I IS %L', n.nspname, t.typname, obj_description(t.oid, 'pg_type'))
		FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype IN ('e','d','c','r') AND obj_description(t.oid, 'pg_type') IS NOT NULL
		AND ` + sysFilter + ` AND ` + notExtDep("t.oid", "'pg_type'::regclass"))
	q(`SELECT format('COMMENT ON SCHEMA %I IS %L', n.nspname, shobj_description(n.oid, 'pg_namespace'))
		FROM pg_namespace n
		WHERE shobj_description(n.oid, 'pg_namespace') IS NOT NULL AND ` + sysFilter + ` AND ` + notExtDep("n.oid", "'pg_namespace'::regclass"))
	return out
}

func collectDBSettings(c *pgwire.Conn) []string {
	out := []string{}
	r, err := c.Query(`SELECT unnest(setconfig) FROM pg_db_role_setting
		WHERE setrole = 0 AND setdatabase = (SELECT oid FROM pg_database WHERE datname = current_database())`)
	if err == nil {
		for i := range r.Rows {
			if s := str(r, i, 0); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

func collectRoles(c *pgwire.Conn) []RoleInfo {
	out := []RoleInfo{}
	r, err := c.Query(`SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolcanlogin,
		rolreplication, rolconnlimit, rolbypassrls, COALESCE(rolvaliduntil::text, '')
		FROM pg_roles WHERE rolname NOT LIKE 'pg\_%'`)
	if err != nil {
		return nil
	}
	for i := range r.Rows {
		out = append(out, RoleInfo{
			Name: str(r, i, 0), Super: boolean(r, i, 1), CreateDB: boolean(r, i, 2),
			CreateRole: boolean(r, i, 3), Inherit: boolean(r, i, 4), Login: boolean(r, i, 5),
			Replication: boolean(r, i, 6), ConnLimit: int32(int64v(r, i, 7)),
			BypassRLS: boolean(r, i, 8), ValidUntil: str(r, i, 9),
		})
	}
	// passwords need pg_authid (superuser only)
	if rp, err := c.Query(`SELECT rolname, COALESCE(rolpassword, '') FROM pg_authid WHERE rolname NOT LIKE 'pg\_%'`); err == nil {
		for i := range rp.Rows {
			for j := range out {
				if out[j].Name == str(rp, i, 0) {
					out[j].Password = str(rp, i, 1)
				}
			}
		}
	}
	return out
}

func collectACLs(c *pgwire.Conn) []ACLGrant {
	out := []ACLGrant{}
	type key struct{ obj, role string; grantOpt bool }
	type val struct{ privs []string }
	agg := map[key]*val{}
	order := []key{}

	add := func(obj, role string, grantOpt bool, priv string) {
		k := key{obj, role, grantOpt}
		v, ok := agg[k]
		if !ok {
			v = &val{}
			agg[k] = v
			order = append(order, k)
		}
		v.privs = append(v.privs, priv)
	}

	q := func(sql string, objFmt func(r *pgwire.Result, i int) string) {
		r, err := c.Query(sql)
		if err != nil {
			return
		}
		for i := range r.Rows {
			role := str(r, i, 0)
			priv := str(r, i, 1)
			grantOpt := boolean(r, i, 2)
			add(objFmt(r, i), role, grantOpt, priv)
		}
	}

	// tables & sequences & views & matviews
	q(`SELECT CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(g.grantee) END, g.privilege_type, g.is_grantable,
		n.nspname, c.relname, c.relkind
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, aclexplode(c.relacl) g
		WHERE c.relacl IS NOT NULL AND `+sysFilter+` AND `+notExtDep("c.oid", "'pg_class'::regclass"),
		func(r *pgwire.Result, i int) string {
			kind := str(r, i, 5)
			obj := "TABLE " + QuoteIdent(str(r, i, 3)) + "." + QuoteIdent(str(r, i, 4))
			if kind == "S" {
				obj = "SEQUENCE " + QuoteIdent(str(r, i, 3)) + "." + QuoteIdent(str(r, i, 4))
			}
			return obj
		})
	// functions
	q(`SELECT CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(g.grantee) END, g.privilege_type, g.is_grantable,
		n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace, aclexplode(p.proacl) g
		WHERE p.proacl IS NOT NULL AND `+sysFilter+` AND `+notExtDep("p.oid", "'pg_proc'::regclass"),
		func(r *pgwire.Result, i int) string {
			return "FUNCTION " + QuoteIdent(str(r, i, 3)) + "." + QuoteIdent(str(r, i, 4)) + "(" + str(r, i, 5) + ")"
		})
	// schemas
	q(`SELECT CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(g.grantee) END, g.privilege_type, g.is_grantable, n.nspname
		FROM pg_namespace n, aclexplode(n.nspacl) g
		WHERE n.nspacl IS NOT NULL AND `+sysFilter+` AND `+notExtDep("n.oid", "'pg_namespace'::regclass"),
		func(r *pgwire.Result, i int) string {
			return "SCHEMA " + QuoteIdent(str(r, i, 3))
		})

	for _, k := range order {
		v := agg[k]
		out = append(out, ACLGrant{Object: k.obj, Role: k.role, Privs: v.privs, GrantOpt: k.grantOpt})
	}
	return out
}
