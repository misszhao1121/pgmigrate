package migrate

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pgmigrate/internal/pgwire"
)

// Engine runs one migration job.
type Engine struct {
	opts   Options
	st     *Status
	ctx    context.Context
	cancel context.CancelFunc
	onLog  func(level, msg string)
	once   sync.Once
}

// New creates an engine for the given options.
func New(opts Options, onLog func(level, msg string)) *Engine {
	opts.normalize()
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	return &Engine{
		opts:   opts,
		st:     &Status{State: "running", Phase: PhaseIdle, StartedAt: &now, Workers: opts.Workers},
		ctx:    ctx,
		cancel: cancel,
		onLog:  onLog,
	}
}

// Status returns a snapshot for UI.
func (e *Engine) Status() *Status { return e.st.Snapshot() }

// Logs returns log entries newer than seq.
func (e *Engine) Logs(since uint64) []Log { return e.st.logsSince(since) }

// Cancel requests cancellation of the job.
func (e *Engine) Cancel() { e.cancel() }

// Done reports whether the context was canceled.
func (e *Engine) Canceled() bool { return e.ctx.Err() != nil }

func (e *Engine) logf(level, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	e.st.addLog(level, msg)
	if e.onLog != nil {
		e.onLog(level, msg)
	}
}

func (e *Engine) setPhase(p string, detail ...string) {
	d := ""
	if len(detail) > 0 {
		d = detail[0]
	}
	e.st.mu.Lock()
	e.st.Phase = p
	e.st.PhaseDetail = stateDesc(p) + func() string {
		if d != "" {
			return "：" + d
		}
		return ""
	}()
	e.st.mu.Unlock()
	e.logf("info", "【阶段】%s", stateDesc(p)+func() string {
		if d != "" {
			return "：" + d
		}
		return ""
	}())
}

func (e *Engine) addWarning(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	e.st.mu.Lock()
	e.st.Warnings = append(e.st.Warnings, msg)
	e.st.mu.Unlock()
	e.logf("warn", "%s", msg)
}

func (e *Engine) fail(err error) error {
	e.st.mu.Lock()
	e.st.State = "failed"
	e.st.Error = err.Error()
	now := time.Now()
	e.st.FinishedAt = &now
	e.st.mu.Unlock()
	e.logf("error", "迁移失败：%v", err)
	return err
}

func fmtBytes(n int64) string {
	f := float64(n)
	for _, u := range []string{"B", "KB", "MB", "GB", "TB"} {
		if f < 1024 {
			return fmt.Sprintf("%.1f%s", f, u)
		}
		f /= 1024
	}
	return fmt.Sprintf("%.1fPB", f)
}

// sessionSetup applies common settings to a migration connection.
func sessionSetup(c *pgwire.Conn, ddl bool) {
	set := []string{
		"SET statement_timeout = 0",
		"SET idle_in_transaction_session_timeout = 0",
		"SET client_min_messages = warning",
	}
	if ddl {
		set = append(set,
			"SET search_path = pg_catalog, public",
			"SET check_function_bodies = off",
			"SET synchronous_commit = off")
	} else {
		set = append(set, "SET synchronous_commit = off")
	}
	for _, s := range set {
		_, _ = c.Exec(s)
	}
	_, _ = c.Exec("SET maintenance_work_mem = '512MB'")
}

// Run executes the whole migration. Blocking.
func (e *Engine) Run() error {
	opts := e.opts
	ctx := e.ctx

	if opts.Incremental {
		return e.runIncremental()
	}

	// ---------- connect source ----------
	e.setPhase(PhasePrecheck, "连接源库")
	srcCfg := opts.Source.toCfg()
	src, err := pgwire.Connect(ctx, srcCfg)
	if err != nil {
		return e.fail(fmt.Errorf("连接源库失败(%s): %w", srcCfg.Sanitized(), err))
	}
	defer src.Close()
	sessionSetup(src, true)
	srcVer := src.ServerVersion()
	e.logf("info", "源库连接成功：%s", srcVer)
	if !strings.Contains(srcVer, " 11.") {
		e.addWarning("源库版本为 %s（本工具面向 PostgreSQL 11 设计，其他版本未经过完整验证）", srcVer)
	}

	// ---------- collect catalog ----------
	e.setPhase(PhasePrecheck, "读取源库对象定义")
	e.logf("info", "正在读取源库 %q 的全库对象定义……", opts.Source.DB)
	cat, err := CollectCatalog(src, &opts)
	if err != nil {
		return e.fail(err)
	}
	tCount := len(cat.Tables)
	e.logf("ok", "对象统计：扩展 %d，模式 %d，表 %d（分区表 %d），序列 %d，视图 %d，物化视图 %d，函数 %d，聚合 %d，索引 %d，约束 %d，触发器 %d，策略 %d，枚举 %d，域 %d，复合 %d",
		len(cat.Exts), len(cat.Schemas), tCount, countPartTables(cat.Tables), len(cat.Seqs), len(cat.Views),
		len(cat.Matviews), len(cat.Funcs), len(cat.Aggs), len(cat.Indexes), len(cat.Constraints),
		len(cat.Triggers), len(cat.Policies), len(cat.Enums), len(cat.Domains), len(cat.Composites))
	e.logf("info", "源库大小：%s，编码 %s，LC_COLLATE %s", fmtBytes(cat.DB.Size), cat.DB.Encoding, cat.DB.Collate)

	hasOIDs := 0
	for _, t := range cat.Tables {
		if t.HasOIDs {
			hasOIDs++
		}
	}
	if hasOIDs > 0 {
		e.addWarning("%d 张表带 WITH OIDS（PostgreSQL 12+ 已移除该特性，OID 列无法迁移）", hasOIDs)
	}
	if cat.HasForeignTables > 0 {
		e.addWarning("存在 %d 张外部表(FDW)，其数据不在本库内，已跳过数据拷贝", cat.HasForeignTables)
	}
	if cat.HasLargeObjects {
		e.logf("info", "源库包含大对象(Large Object)，将在数据拷贝后一并迁移（需超级用户权限，图片等二进制内容）")
	}

	// ---------- target admin connection ----------
	e.setPhase(PhasePrecheck, "连接目标库")
	admCfg := opts.Target.toCfg()
	adm, err := pgwire.Connect(ctx, admCfg)
	if err != nil {
		return e.fail(fmt.Errorf("连接目标库失败(%s): %w", admCfg.Sanitized(), err))
	}
	sessionSetup(adm, false)
	tgtVer := adm.ServerVersion()
	e.logf("info", "目标库连接成功：%s", tgtVer)
	if !strings.Contains(tgtVer, " 18.") {
		e.addWarning("目标库版本为 %s（本工具面向 PostgreSQL 18 设计）", tgtVer)
	}
	r, err := adm.Query(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)", QuoteLit(opts.TargetDB)))
	if err != nil {
		return e.fail(fmt.Errorf("查询目标库是否存在失败: %w", err))
	}
	targetExists := len(r.Rows) > 0 && boolean(r, 0, 0)

	if targetExists {
		if opts.DropTarget {
			e.setPhase(PhaseCreateDB, "删除已存在的目标库")
			e.logf("warn", "按配置删除已存在的目标库 %q（WITH FORCE 强制断开连接）", opts.TargetDB)
			if _, err := adm.Exec(fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", QuoteIdent(opts.TargetDB))); err != nil {
				if _, err2 := adm.Exec(fmt.Sprintf("DROP DATABASE %s", QuoteIdent(opts.TargetDB))); err2 != nil {
					return e.fail(fmt.Errorf("删除目标库 %s 失败: %w（若有连接占用，请先断开或手动删除）", opts.TargetDB, err))
				}
			}
			targetExists = false
		} else {
			// must be empty
			e.logf("info", "目标库 %q 已存在，检查是否为空……", opts.TargetDB)
			probe, err := pgwire.Connect(ctx, func() *pgwire.Config {
				c := opts.Target.toCfg()
				c.DB = opts.TargetDB
				return c
			}())
			if err != nil {
				return e.fail(fmt.Errorf("连接目标库 %s 失败: %w", opts.TargetDB, err))
			}
			objCount := 0
			if rr, err := probe.Query(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind IN ('r','p','v','m','S') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'`); err == nil {
				objCount = int(int64v(rr, 0, 0))
			}
			probe.Close()
			if objCount > 0 {
				return e.fail(fmt.Errorf("目标库 %q 已存在且包含 %d 个对象，拒绝在非空库上迁移；请勾选“删除重建目标库”或手工清理", opts.TargetDB, objCount))
			}
			e.logf("ok", "目标库 %q 存在且为空，直接使用", opts.TargetDB)
		}
	}
	if !targetExists {
		if !opts.CreateTarget {
			return e.fail(fmt.Errorf("目标库 %q 不存在，且未勾选“自动创建目标库”", opts.TargetDB))
		}
		e.setPhase(PhaseCreateDB, "创建目标库 "+opts.TargetDB)
		e.logf("info", "创建目标库 %q（编码 %s，排序规则 %s）……", opts.TargetDB, cat.DB.Encoding, cat.DB.Collate)
		attempts := []string{
			createDatabaseSQL(opts.TargetDB, cat.DB.Encoding, cat.DB.Collate, cat.DB.Ctype),
			createDatabaseSQL(opts.TargetDB, cat.DB.Encoding, "", ""),
			fmt.Sprintf("CREATE DATABASE %s TEMPLATE template0", QuoteIdent(opts.TargetDB)),
		}
		created := false
		var lastErr error
		for i, sql := range attempts {
			if _, err := adm.Exec(sql); err == nil {
				created = true
				if i > 0 {
					e.addWarning("CREATE DATABASE 使用了降级参数（第 %d 种尝试）：排序规则/区域设置与源库不完全一致，列级 COLLATE 已按目标环境处理", i+1)
				}
				break
			} else {
				lastErr = err
				e.logf("warn", "创建尝试 %d 失败：%v", i+1, err)
			}
		}
		if !created {
			return e.fail(fmt.Errorf("创建目标库 %s 失败: %w", opts.TargetDB, lastErr))
		}
		e.logf("ok", "目标库 %s 创建成功", opts.TargetDB)
	}
	adm.Close()

	// ---------- main target connection ----------
	e.setPhase(PhasePrecheck, "连接目标库 "+opts.TargetDB)
	tgtCfg := opts.Target.toCfg()
	tgtCfg.DB = opts.TargetDB
	tgt, err := pgwire.Connect(ctx, tgtCfg)
	if err != nil {
		return e.fail(fmt.Errorf("连接目标库 %s 失败: %w", opts.TargetDB, err))
	}
	defer tgt.Close()
	sessionSetup(tgt, true)

	// ---------- extensions ----------
	e.setPhase(PhaseExtension)
	needExts := []ExtInfo{}
	for _, ex := range cat.Exts {
		if ex.Name == "plpgsql" {
			continue
		}
		needExts = append(needExts, ex)
	}
	if len(needExts) > 0 {
		avail := map[string]string{}
		if rr, err := tgt.Query(fmt.Sprintf(`SELECT name, default_version FROM pg_available_extensions WHERE name IN (%s)`,
			strings.Join(extLits(needExts), ", "))); err == nil {
			for i := range rr.Rows {
				avail[str(rr, i, 0)] = str(rr, i, 1)
			}
		}
		for _, ex := range needExts {
			ver, ok := avail[ex.Name]
			if !ok {
				e.addWarning("目标服务器缺少扩展 %s（源库版本 %s）。离线容器环境请注意：官方 postgres 镜像已自带全部 contrib 扩展；PostGIS/TimescaleDB 等第三方扩展必须改用包含它们的镜像（如 postgis/postgis:18）或向容器内手动安装扩展文件。依赖该扩展的对象将迁移失败", ex.Name, ex.Version)
				continue
			}
			if ver != ex.Version {
				e.logf("warn", "扩展 %s 源库版本 %s 与目标可用版本 %s 不同，将按目标版本安装（数据格式通常兼容，若报错请核对）", ex.Name, ex.Version, ver)
			}
			okInstall, extErr := createExtensionBestEffort(tgt, ex)
			if okInstall {
				e.logf("ok", "扩展 %s 安装完成（目标版本 %s，模式 %s）", ex.Name, ver, ex.Schema)
			} else {
				e.addWarning("扩展 %s 安装失败，依赖该扩展的对象将迁移失败。最后一次服务器错误：%s。常见原因：离线容器内缺少扩展的动态库/控制文件——请确认镜像是否包含该扩展（PostGIS 需 postgis/postgis 镜像），或进入容器执行 ls $(pg_config --sharedir)/extension | grep %s 检查",
					ex.Name, oneLine(extErr), ex.Name)
			}
		}
	} else {
		e.logf("info", "源库没有自定义扩展")
	}

	// ---------- legacy PostGIS objects ----------
	// Functions bound to $libdir/postgis-2.x can never be created on a 3.x
	// image (missing .so) and are superseded by the new extension. Drop them
	// from the plan whether or not the new postgis installed successfully.
	if nf, na := dropLegacyPostgisObjects(cat); nf+na > 0 {
		e.logf("info", "已跳过 %d 个绑定 postgis-2.x 动态库的遗留函数/聚合（目标 PostGIS 3.x 自带等价 ST_* 实现，无需迁移）", nf+na)
	}

	// ---------- schemas ----------
	e.setPhase(PhaseSchema)
	if len(cat.Schemas) > 0 {
		if fs := runWithRetry(ctx, tgt, schemaStmts(cat.Schemas), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("模式", fs)
		} else {
			e.logf("ok", "模式迁移完成（%d 个）", len(cat.Schemas))
		}
	}

	// ---------- types ----------
	e.setPhase(PhaseTypes)
	collLookup := collLookupMap(cat.Collations)
	if len(cat.Collations) > 0 {
		if fs := runWithRetry(ctx, tgt, collationStmts(cat.Collations, cat.DB.EncID), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("排序规则", fs)
		}
	}
	if len(cat.Enums) > 0 {
		if fs := runWithRetry(ctx, tgt, enumStmts(cat.Enums), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("枚举类型", fs)
		} else {
			e.logf("ok", "枚举类型迁移完成（%d 个）", len(cat.Enums))
		}
	}
	if len(cat.Domains) > 0 {
		if fs := runWithRetry(ctx, tgt, domainStmts(cat.Domains, collLookup), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("域类型", fs)
		}
	}
	if len(cat.Composites) > 0 {
		if fs := runWithRetry(ctx, tgt, compositeStmts(cat.Composites), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("复合类型", fs)
		}
	}
	if len(cat.Ranges) > 0 {
		if fs := runWithRetry(ctx, tgt, rangeStmts(cat.Ranges), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("范围类型", fs)
		}
	}
	e.logf("ok", "类型迁移完成")

	// ---------- functions ----------
	e.setPhase(PhaseFuncs)
	totalFuncs := len(cat.Funcs) + len(cat.Aggs)
	if totalFuncs > 0 {
		if fs := runWithRetry(ctx, tgt, funcStmts(cat.Funcs, cat.Aggs), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("函数", fs)
		} else {
			e.logf("ok", "函数/存储过程迁移完成（%d 个）", totalFuncs)
		}
	}

	// ---------- sequences (create) ----------
	e.setPhase(PhaseSeqs)
	if len(cat.Seqs) > 0 {
		if fs := runWithRetry(ctx, tgt, seqStmts(cat.Seqs), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("序列", fs)
		} else {
			e.logf("ok", "序列迁移完成（%d 个）", len(cat.Seqs))
		}
	}

	// ---------- tables ----------
	e.setPhase(PhaseTables)
	stmts := tableStmts(cat)
	// log partitioned-parent DDL once for diagnostics (they are the tables the
	// application queries directly)
	for _, st := range stmts {
		if st.Kind == "table" && strings.Contains(st.SQL, "PARTITION BY") {
			e.logf("info", "分区父表 DDL：%s", st.SQL)
		}
	}
	if fs := runWithRetry(ctx, tgt, stmts, func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
		e.reportFailures("表结构", fs)
	}
	// determine which tables actually exist on target
	existingTables := map[string]bool{}
	if rr, err := tgt.Query(`SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'`); err == nil {
		for i := range rr.Rows {
			existingTables[str(rr, i, 0)+"."+str(rr, i, 1)] = true
		}
	}
	missing := 0
	for _, t := range cat.Tables {
		if !existingTables[t.Schema+"."+t.Name] {
			missing++
		}
	}
	if missing > 0 {
		e.addWarning("%d 张表在目标库缺失（创建失败，详见日志），其数据将被跳过", missing)
	} else {
		e.logf("ok", "表结构迁移完成（%d 张）", len(cat.Tables))
	}

	// ---------- views ----------
	e.setPhase(PhaseViews)
	if len(cat.Views) > 0 {
		if fs := runWithRetry(ctx, tgt, viewStmts(cat.Views), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("视图", fs)
		} else {
			e.logf("ok", "视图迁移完成（%d 个）", len(cat.Views))
		}
	}

	// ---------- matviews ----------
	e.setPhase(PhaseMatviews)
	if len(cat.Matviews) > 0 {
		if fs := runWithRetry(ctx, tgt, matviewStmts(cat.Matviews), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("物化视图", fs)
		} else {
			e.logf("ok", "物化视图迁移完成（%d 个，稍后刷新数据）", len(cat.Matviews))
		}
	}

	// ---------- data ----------
	var finalFailures []Failure
	if opts.CopyData {
		e.setPhase(PhaseData)
		var dataTables []*TableInfo
		for _, t := range cat.Tables {
			if t.Kind != 'r' {
				continue // partitioned parents have no storage
			}
			if !existingTables[t.Schema+"."+t.Name] {
				continue
			}
			dataTables = append(dataTables, t)
		}
		// extension-owned tables (PostGIS spatial_ref_sys): merge data like
		// the official postgis_restore.pl does
		var extJobs []*TableInfo
		skipExt := 0
		for _, t := range cat.ExtTables {
			if t.Kind != 'r' {
				continue
			}
			if !existingTables[t.Schema+"."+t.Name] {
				if t.MergeSafe {
					e.addWarning("扩展表 %s.%s 在目标库不存在（扩展可能未安装成功），跳过其数据合并", t.Schema, t.Name)
				}
				continue
			}
			if !t.MergeSafe {
				skipExt++
				continue
			}
			extJobs = append(extJobs, t)
		}
		if skipExt > 0 {
			e.logf("info", "跳过 %d 张扩展内部表的数据（由扩展自行管理，不在合并白名单）", skipExt)
		}
		// largest first for better parallel utilization
		sort.SliceStable(dataTables, func(a, b int) bool { return dataTables[a].EstRows > dataTables[b].EstRows })
		if len(dataTables) == 0 && len(extJobs) == 0 {
			e.logf("info", "没有需要拷贝的数据表")
		} else {
			if err := e.runDataCopy(dataTables, extJobs, cat); err != nil {
				if ctx.Err() != nil {
					e.st.mu.Lock()
					e.st.State = "canceled"
					e.st.mu.Unlock()
					return fmt.Errorf("迁移已取消")
				}
				e.addWarning("数据拷贝阶段出现错误：%v", err)
			}
			// sequence values
			e.setPhase(PhaseData, "同步序列当前值")
			e.syncSequences(src, tgt, cat)
			// large objects (images etc.)
			e.setPhase(PhaseData, "迁移大对象(Large Object)")
			e.migrateLargeObjects(src, tgt)
		}
	} else {
		e.logf("info", "按配置跳过数据拷贝")
	}

	// ---------- post-data ----------
	e.setPhase(PhasePostData, "行级安全")
	if fs := runWithRetry(ctx, tgt, rlsStmts(cat.Tables), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
		e.reportFailures("行级安全", fs)
	}
	e.setPhase(PhasePostData, "主键/唯一/检查约束")
	preCon, fkCon := constraintStmts(cat)
	if fs := runWithRetry(ctx, tgt, preCon, func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
		e.reportFailures("约束", fs)
	} else if len(preCon) > 0 {
		e.logf("ok", "主键/唯一/检查约束完成（%d 个）", len(preCon))
	}
	e.setPhase(PhasePostData, "索引")
	if len(cat.Indexes) > 0 {
		if fs := runWithRetry(ctx, tgt, indexStmts(cat), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("索引", fs)
		} else {
			e.logf("ok", "索引迁移完成（%d 个）", len(cat.Indexes))
		}
	}
	e.setPhase(PhasePostData, "触发器")
	if len(cat.Triggers) > 0 {
		if fs := runWithRetry(ctx, tgt, triggerStmts(cat.Triggers), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("触发器", fs)
		} else {
			e.logf("ok", "触发器迁移完成（%d 个）", len(cat.Triggers))
		}
	}
	e.setPhase(PhasePostData, "序列归属")
	if len(cat.SeqOwns) > 0 {
		if fs := runWithRetry(ctx, tgt, seqOwnStmts(cat.SeqOwns), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("序列归属", fs)
		}
	}
	e.setPhase(PhasePostData, "策略")
	if len(cat.Policies) > 0 {
		if fs := runWithRetry(ctx, tgt, policyStmts(cat.Policies, cat.Tables), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("策略", fs)
		}
	}
	e.setPhase(PhasePostData, "注释")
	if len(cat.Comments) > 0 {
		if fs := runWithRetry(ctx, tgt, commentStmts(cat.Comments), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("注释", fs)
		} else {
			e.logf("ok", "对象注释迁移完成（%d 条）", len(cat.Comments))
		}
	}
	if opts.RefreshMatviews && len(cat.Matviews) > 0 {
		e.setPhase(PhasePostData, "刷新物化视图")
		var mv []Stmt
		for _, v := range cat.Matviews {
			mv = append(mv, Stmt{
				SQL:  fmt.Sprintf("REFRESH MATERIALIZED VIEW %s.%s", QuoteIdent(v.Schema), QuoteIdent(v.Name)),
				Desc: "刷新物化视图 " + v.Schema + "." + v.Name, Kind: "refresh",
			})
		}
		if fs := runWithRetry(ctx, tgt, mv, func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("物化视图刷新", fs)
		} else {
			e.logf("ok", "物化视图刷新完成（%d 个）", len(cat.Matviews))
		}
	}
	if opts.MigrateRoles && len(cat.Roles) > 0 {
		e.setPhase(PhasePostData, "角色")
		existingRoles := map[string]bool{}
		if rr, err := tgt.Query(`SELECT rolname FROM pg_roles`); err == nil {
			for i := range rr.Rows {
				existingRoles[str(rr, i, 0)] = true
			}
		}
		if fs := runWithRetry(ctx, tgt, roleStmts(cat.Roles, existingRoles), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("角色", fs)
		}
	}
	if opts.MigrateOwners {
		e.setPhase(PhasePostData, "属主")
		if fs := runWithRetry(ctx, tgt, ownerStmts(cat, map[string]bool{}), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("属主", fs)
		} else {
			e.logf("ok", "属主迁移完成")
		}
	}
	if opts.MigrateACLs && len(cat.ACLs) > 0 {
		e.setPhase(PhasePostData, "权限(ACL)")
		if fs := runWithRetry(ctx, tgt, aclStmts(cat.ACLs), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("权限", fs)
		} else {
			e.logf("ok", "权限迁移完成（%d 条授权）", len(cat.ACLs))
		}
	}
	e.setPhase(PhasePostData, "外键约束")
	if len(fkCon) > 0 {
		if fs := runWithRetry(ctx, tgt, fkCon, func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("外键", fs)
		} else {
			e.logf("ok", "外键约束完成（%d 个）", len(fkCon))
		}
	}
	if len(cat.DBSettings) > 0 {
		if fs := runWithRetry(ctx, tgt, dbSettingStmts(opts.TargetDB, cat.DBSettings), func(l, m string) { e.logf(l, "%s", m) }); len(fs) > 0 {
			e.reportFailures("库参数", fs)
		}
	}
	if opts.Analyze {
		e.setPhase(PhasePostData, "ANALYZE 更新优化器统计")
		e.logf("info", "执行 ANALYZE（大库可能需要一些时间）……")
		if _, err := tgt.Exec("ANALYZE"); err != nil {
			e.addWarning("ANALYZE 失败：%v", err)
		} else {
			e.logf("ok", "ANALYZE 完成")
		}
	}

	// ---------- verify ----------
	if opts.Verify {
		e.setPhase(PhaseVerify)
		rep := e.runVerify(src, tgt, cat, existingTables)
		e.st.mu.Lock()
		e.st.Verification = rep
		e.st.mu.Unlock()
		if rep.OK {
			e.logf("ok", "校验通过：所有表行数与序列值一致")
		} else {
			for _, msg := range rep.Errors {
				e.addWarning("%s", msg)
			}
		}
	}

	finalFailures = append(finalFailures, e.st.collectFailures()...)
	if opts.CopyData {
		// record watermarks so a later incremental run can pick up new rows
		e.setPhase(PhasePostData, "记录增量水位")
		e.logf("info", "正在计算各表增量水位（大表的时间列无索引时可能较慢）……")
		plans := BuildIncrPlans(src, cat, ParseIncrOverrides(opts.IncrKeys))
		recordWatermarks(ctx, src, plans, opts.WatermarkFile, e.logf)
	}
	e.setPhase(PhaseDone)
	e.st.mu.Lock()
	e.st.State = "done"
	now := time.Now()
	e.st.FinishedAt = &now
	e.st.mu.Unlock()
	e.logf("ok", "迁移完成！数据行 %d，字节 %s", atomic.LoadUint64(&e.st.RowsCopied), fmtBytes(int64(atomic.LoadUint64(&e.st.BytesCopied))))
	return nil
}

func countPartTables(tables []*TableInfo) int {
	n := 0
	for _, t := range tables {
		if t.Kind == 'p' {
			n++
		}
	}
	return n
}

// reportFailures logs and records phase failures as warnings.
func (e *Engine) reportFailures(what string, fs []Failure) {
	for _, f := range fs {
		sql := f.SQL
		if len(sql) > 400 {
			sql = sql[:400] + "…"
		}
		e.addWarning("%s失败【%s】：%s\nSQL: %s", what, f.Desc, oneLine(f.Err), sql)
		e.st.mu.Lock()
		e.st.PhaseFailures = append(e.st.PhaseFailures, f)
		e.st.mu.Unlock()
	}
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

// ---- data copy ----

func (e *Engine) runDataCopy(tables []*TableInfo, extTables []*TableInfo, cat *Catalog) error {
	ctx := e.ctx
	jobs := make([]*TableInfo, 0, len(tables)+len(extTables))
	jobs = append(jobs, tables...)
	jobs = append(jobs, extTables...) // staged merge jobs run last
	nw := e.opts.Workers
	if nw > len(jobs) {
		nw = len(jobs)
	}

	e.st.mu.Lock()
	e.st.Tables = make([]TableStat, 0, len(jobs))
	var est uint64
	for _, t := range jobs {
		er := t.EstRows
		if er < 0 {
			er = 0
		}
		est += uint64(er)
		e.st.Tables = append(e.st.Tables, TableStat{
			Schema: t.Schema, Name: t.Name, EstRows: er, SizeBytes: t.SizeBytes, State: "pending",
		})
	}
	e.st.TotalTables = len(jobs)
	e.st.EstRows = est
	e.st.mu.Unlock()

	work := make(chan int, len(jobs))
	for i := range jobs {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	var errCount int64
	start := time.Now()
	for w := 0; w < nw; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var srcConn, dstConn *pgwire.Conn
			defer func() {
				if srcConn != nil {
					srcConn.Close()
				}
				if dstConn != nil {
					dstConn.Close()
				}
			}()
			for idx := range work {
				if ctx.Err() != nil {
					return
				}
				t := jobs[idx]
				// connect lazily
				if srcConn == nil {
					c := e.opts.Source.toCfg()
					sc, err := pgwire.Connect(ctx, c)
					if err != nil {
						e.logf("error", "工作线程 %d 连接源库失败：%v", worker, err)
						atomic.AddInt64(&errCount, 1)
						return
					}
					sessionSetup(sc, false)
					srcConn = sc
					c2 := e.opts.Target.toCfg()
					c2.DB = e.opts.TargetDB
					dc, err := pgwire.Connect(ctx, c2)
					if err != nil {
						e.logf("error", "工作线程 %d 连接目标库失败：%v", worker, err)
						atomic.AddInt64(&errCount, 1)
						srcConn.Close()
						srcConn = nil
						return
					}
					sessionSetup(dc, false)
					dstConn = dc
				}
				e.copyTable(ctx, worker, srcConn, dstConn, t, cat)
			}
		}(w)
	}
	wg.Wait()
	e.logf("info", "数据拷贝阶段结束，用时 %s", time.Since(start).Round(time.Second))
	if atomic.LoadInt64(&errCount) > 0 {
		return fmt.Errorf("%d 个工作线程异常退出", errCount)
	}
	return nil
}

func (e *Engine) markTable(key string, fn func(*TableStat)) {
	e.st.mu.Lock()
	defer e.st.mu.Unlock()
	for i := range e.st.Tables {
		if e.st.Tables[i].Key() == key {
			fn(&e.st.Tables[i])
			break
		}
	}
}

func (e *Engine) bumpCounters(done, failed int, rows, bytes uint64) {
	e.st.mu.Lock()
	e.st.DoneTables += done
	e.st.FailedTables += failed
	e.st.RowsCopied += rows
	e.st.BytesCopied += bytes
	e.st.mu.Unlock()
}

func (e *Engine) copyTable(ctx context.Context, worker int, srcConn, dstConn *pgwire.Conn, t *TableInfo, cat *Catalog) {
	key := t.Schema + "." + t.Name
	start := time.Now()
	e.markTable(key, func(s *TableStat) { s.State = "copying" })
	e.appendCurrent(t.Schema + "." + t.Name)
	verb := "开始拷贝"
	if t.MergeSafe {
		verb = "开始合并扩展表数据"
	}
	e.logf("info", "[W%d] %s %s.%s（估算 %d 行，%s）", worker, verb, t.Schema, t.Name, t.EstRows, fmtBytes(t.SizeBytes))

	// extension-owned tables (PostGIS spatial_ref_sys): merge via temp staging
	// table, same net effect as the official postgis_restore.pl
	if t.MergeSafe {
		rows, bytesN, err := e.stagedLoad(ctx, srcConn, dstConn, t)
		elapsed := time.Since(start).Seconds()
		e.removeCurrent(t.Schema + "." + t.Name)
		if ctx.Err() != nil {
			e.markTable(key, func(s *TableStat) { s.State = "skipped"; s.Err = "已取消"; s.Seconds = elapsed })
			return
		}
		if err != nil {
			e.markTable(key, func(s *TableStat) { s.State = "failed"; s.Err = oneLine(err.Error()); s.Seconds = elapsed })
			e.bumpCounters(0, 1, 0, 0)
			e.logf("error", "[W%d] 合并 %s.%s 失败：%s", worker, t.Schema, t.Name, oneLine(err.Error()))
			return
		}
		e.markTable(key, func(s *TableStat) { s.State = "done"; s.Rows = rows; s.Seconds = elapsed })
		e.bumpCounters(1, 0, uint64(rows), 0)
		e.logf("ok", "[W%d] %s.%s 合并完成：%d 行，%s，用时 %.1fs", worker, t.Schema, t.Name, rows,
			fmtBytes(int64(bytesN)), elapsed)
		return
	}

	// old-style inheritance parents: copy ONLY their own rows
	srcSQL := "COPY " + t.QName() + " TO STDOUT"
	if kids, ok := cat.Inherits[t.OID]; ok && len(kids) > 0 && t.Kind == 'r' {
		srcSQL = "COPY (SELECT * FROM ONLY " + t.QName() + ") TO STDOUT"
	}
	dstSQL := "COPY " + t.QName() + " FROM STDIN"

	pr, pw := io.Pipe()
	cw := &countingWriter{w: pw, engine: e}
	errCh := make(chan error, 1)
	go func() {
		err := srcConn.CopyTo(ctx, srcSQL, cw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		errCh <- err
	}()

	tag, err := dstConn.CopyFrom(ctx, dstSQL, pr)
	srcErr := <-errCh
	if err == nil && srcErr != nil {
		err = srcErr
	}
	elapsed := time.Since(start).Seconds()
	if ctx.Err() != nil {
		e.markTable(key, func(s *TableStat) { s.State = "skipped"; s.Err = "已取消"; s.Seconds = elapsed })
		e.removeCurrent(t.Schema + "." + t.Name)
		return
	}
	if err != nil {
		e.markTable(key, func(s *TableStat) { s.State = "failed"; s.Err = oneLine(err.Error()); s.Seconds = elapsed })
		e.bumpCounters(0, 1, 0, 0)
		e.logf("error", "[W%d] 拷贝 %s.%s 失败：%s", worker, t.Schema, t.Name, oneLine(err.Error()))
		e.removeCurrent(t.Schema + "." + t.Name)
		return
	}
	rows := int64(0)
	if strings.HasPrefix(tag, "COPY ") {
		rows, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(tag, "COPY ")), 10, 64)
	}
	e.markTable(key, func(s *TableStat) { s.State = "done"; s.Rows = rows; s.Seconds = elapsed })
	e.bumpCounters(1, 0, uint64(rows), 0)
	e.logf("ok", "[W%d] %s.%s 完成：%d 行，%s，用时 %.1fs", worker, t.Schema, t.Name, rows,
		fmtBytes(int64(cw.n.Load())), elapsed)
	e.removeCurrent(t.Schema + "." + t.Name)
}

// stagedLoad merges an extension-owned table's data (e.g. PostGIS
// spatial_ref_sys) into the target: source rows are staged in a temp table and
// inserted with ON CONFLICT DO NOTHING, so rows already defined by the target
// extension keep their (newer) definitions while source-only rows (custom
// SRIDs) are added — the same net result as the official postgis_restore.pl.
func (e *Engine) stagedLoad(ctx context.Context, srcConn, dstConn *pgwire.Conn, t *TableInfo) (int64, uint64, error) {
	const tmp = "pgmigrate_stg"
	if _, err := dstConn.Exec("DROP TABLE IF EXISTS " + tmp); err != nil {
		return 0, 0, fmt.Errorf("清理临时中转表失败: %w", err)
	}
	if _, err := dstConn.Exec(fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0", tmp, t.QName())); err != nil {
		return 0, 0, fmt.Errorf("创建临时中转表失败: %w", err)
	}
	pr, pw := io.Pipe()
	cw := &countingWriter{w: pw, engine: e}
	errCh := make(chan error, 1)
	go func() {
		err := srcConn.CopyTo(ctx, "COPY "+t.QName()+" TO STDOUT", pw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		errCh <- err
	}()
	stag, err := dstConn.CopyFrom(ctx, "COPY "+tmp+" FROM STDIN", pr)
	srcErr := <-errCh
	if err == nil && srcErr != nil {
		err = srcErr
	}
	fed := int64(0)
	if err == nil && strings.HasPrefix(stag, "COPY ") {
		fed, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(stag, "COPY ")), 10, 64)
	}
	if err != nil {
		_, _ = dstConn.Exec("DROP TABLE IF EXISTS " + tmp)
		return 0, cw.n.Load(), err
	}
	itag, ierr := dstConn.Exec(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s ON CONFLICT DO NOTHING", t.QName(), tmp))
	_, _ = dstConn.Exec("DROP TABLE IF EXISTS " + tmp)
	if ierr != nil {
		return fed, cw.n.Load(), fmt.Errorf("合并载入扩展表数据失败: %w", ierr)
	}
	rows := fed
	if strings.HasPrefix(itag, "INSERT") {
		f := strings.Fields(itag)
		if len(f) >= 3 {
			if n, perr := strconv.ParseInt(f[len(f)-1], 10, 64); perr == nil {
				rows = n
			}
		}
	}
	return rows, cw.n.Load(), nil
}

func (e *Engine) appendCurrent(name string) {	e.st.mu.Lock()
	parts := []string{}
	if e.st.Current != "" {
		parts = strings.Split(e.st.Current, ", ")
	}
	parts = append(parts, name)
	if len(parts) > 12 {
		parts = parts[len(parts)-12:]
	}
	e.st.Current = strings.Join(parts, ", ")
	e.st.mu.Unlock()
}

func (e *Engine) removeCurrent(name string) {
	e.st.mu.Lock()
	parts := []string{}
	for _, p := range strings.Split(e.st.Current, ", ") {
		if p != name && p != "" {
			parts = append(parts, p)
		}
	}
	e.st.Current = strings.Join(parts, ", ")
	e.st.mu.Unlock()
}

type countingWriter struct {
	w      io.Writer
	engine *Engine
	n      atomic.Uint64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(uint64(n))
	c.engine.st.mu.Lock()
	c.engine.st.BytesCopied += uint64(n)
	c.engine.st.mu.Unlock()
	return n, err
}

// syncSequences copies current sequence values from source to target.
func (e *Engine) syncSequences(src, tgt *pgwire.Conn, cat *Catalog) {
	okCount, failCount := 0, 0
	for _, s := range cat.Seqs {
		if e.ctx.Err() != nil {
			return
		}
		r, err := src.Query(fmt.Sprintf("SELECT last_value, is_called FROM %s.%s", QuoteIdent(s.Schema), QuoteIdent(s.Name)))
		if err != nil {
			e.addWarning("读取序列 %s.%s 当前值失败：%s", s.Schema, s.Name, oneLine(err.Error()))
			failCount++
			continue
		}
		if len(r.Rows) == 0 {
			continue
		}
		lastV := r.Col(0, 0)
		called := r.Col(0, 1) == "t" || r.Col(0, 1) == "true"
		if lastV == "" {
			e.addWarning("序列 %s.%s 当前值不可读（权限不足或为空），跳过同步", s.Schema, s.Name)
			failCount++
			continue
		}
		sql := fmt.Sprintf("SELECT setval(%s::regclass, %s, %t)",
			QuoteLit(s.Schema+"."+s.Name), lastV, called)
		if _, err := tgt.Exec(sql); err != nil {
			e.addWarning("同步序列 %s.%s 失败：%s", s.Schema, s.Name, oneLine(err.Error()))
			failCount++
			continue
		}
		okCount++
	}
	e.logf("ok", "序列当前值同步完成（成功 %d，失败 %d）", okCount, failCount)
}

// ---- verification ----

func (e *Engine) runVerify(src, tgt *pgwire.Conn, cat *Catalog, existing map[string]bool) *VerifyReport {
	ctx := e.ctx
	e.logf("info", "开始校验（逐表行数 + 序列值）……")
	start := time.Now()
	rep := &VerifyReport{OK: true}

	var dataTables []*TableInfo
	for _, t := range cat.Tables {
		if t.Kind != 'r' {
			continue
		}
		if !existing[t.Schema+"."+t.Name] {
			continue
		}
		dataTables = append(dataTables, t)
	}

	// NOTE: one pgwire Conn must never be shared across concurrent queries.
	// Verification therefore uses a worker pool where every worker owns its
	// own source and target connections (same pattern as the data copy).
	type verifyJob struct {
		t    *TableInfo
		from string
	}
	jobs := make([]verifyJob, 0, len(dataTables))
	for _, t := range dataTables {
		from := t.QName()
		if kids, ok := cat.Inherits[t.OID]; ok && len(kids) > 0 && t.Kind == 'r' {
			from = "(SELECT * FROM ONLY " + t.QName() + ") v"
		}
		jobs = append(jobs, verifyJob{t: t, from: from})
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var totalSrc, totalDst int64
	nw := e.opts.Workers
	if nw > len(jobs) {
		nw = len(jobs)
	}
	if nw < 1 {
		nw = 1
	}
	work := make(chan int, len(jobs))
	for i := range jobs {
		work <- i
	}
	close(work)

	for w := 0; w < nw; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var srcConn, dstConn *pgwire.Conn
			defer func() {
				if srcConn != nil {
					srcConn.Close()
				}
				if dstConn != nil {
					dstConn.Close()
				}
			}()
			for i := range work {
				if e.ctx.Err() != nil {
					return
				}
				j := jobs[i]
				if srcConn == nil {
					sc, err := pgwire.Connect(ctx, e.opts.Source.toCfg())
					if err != nil {
						mu.Lock()
						rep.Errors = append(rep.Errors, fmt.Sprintf("校验工作线程连接源库失败：%s", oneLine(err.Error())))
						mu.Unlock()
						return
					}
					sessionSetup(sc, false)
					srcConn = sc
					c2 := e.opts.Target.toCfg()
					c2.DB = e.opts.TargetDB
					dc, err := pgwire.Connect(ctx, c2)
					if err != nil {
						mu.Lock()
						rep.Errors = append(rep.Errors, fmt.Sprintf("校验工作线程连接目标库失败：%s", oneLine(err.Error())))
						mu.Unlock()
						srcConn.Close()
						srcConn = nil
						return
					}
					sessionSetup(dc, false)
					dstConn = dc
				}
				var sc, dc int64
				var srcErr, dstErr error
				if r, err := srcConn.Query("SELECT count(*) FROM " + j.from); err == nil {
					sc = int64v(r, 0, 0)
				} else {
					srcErr = err
				}
				if r, err := dstConn.Query("SELECT count(*) FROM " + j.from); err == nil {
					dc = int64v(r, 0, 0)
				} else {
					dstErr = err
				}
				mu.Lock()
				if srcErr != nil {
					rep.Errors = append(rep.Errors, fmt.Sprintf("源表 %s.%s 计数失败：%s", j.t.Schema, j.t.Name, oneLine(srcErr.Error())))
				}
				if dstErr != nil {
					rep.Errors = append(rep.Errors, fmt.Sprintf("目标表 %s.%s 计数失败：%s", j.t.Schema, j.t.Name, oneLine(dstErr.Error())))
				}
				rep.Tables = append(rep.Tables, TableVerify{Schema: j.t.Schema, Name: j.t.Name, Src: sc, Dst: dc, OK: sc == dc && srcErr == nil && dstErr == nil})
				totalSrc += sc
				totalDst += dc
				if sc != dc || srcErr != nil || dstErr != nil {
					rep.OK = false
				}
				mu.Unlock()
				mark := "✓"
				if sc != dc || srcErr != nil || dstErr != nil {
					mark = "✗"
				}
				e.logf("info", "校验 %s.%s：源 %d / 目标 %d %s", j.t.Schema, j.t.Name, sc, dc, mark)
			}
		}(w)
	}
	wg.Wait()

	for _, s := range cat.Seqs {
		sv := SeqVerify{Schema: s.Schema, Name: s.Name, OK: true}
		if r, err := src.Query(fmt.Sprintf("SELECT last_value||','||is_called FROM %s.%s", QuoteIdent(s.Schema), QuoteIdent(s.Name))); err == nil && len(r.Rows) > 0 {
			sv.Src = r.Col(0, 0)
		}
		if r, err := tgt.Query(fmt.Sprintf("SELECT last_value||','||is_called FROM %s.%s", QuoteIdent(s.Schema), QuoteIdent(s.Name))); err == nil && len(r.Rows) > 0 {
			sv.Dst = r.Col(0, 0)
		}
		if sv.Src != sv.Dst {
			// setval bump: after COPY, target sequence was set with setval; equal is expected
			sv.OK = false
			rep.Errors = append(rep.Errors, fmt.Sprintf("序列 %s.%s 值不一致：源=%s 目标=%s", s.Schema, s.Name, sv.Src, sv.Dst))
		}
		rep.Seqs = append(rep.Seqs, sv)
	}
	for _, v := range cat.Matviews {
		r, err := tgt.Query(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname=%s AND n.nspname=%s AND c.relkind='m')",
			QuoteLit(v.Name), QuoteLit(v.Schema)))
		ex := false
		if err == nil && len(r.Rows) > 0 {
			ex = boolean(r, 0, 0)
		}
		if !ex {
			rep.OK = false
			rep.Errors = append(rep.Errors, fmt.Sprintf("物化视图 %s.%s 未创建", v.Schema, v.Name))
		}
		rep.Matviews = append(rep.Matviews, MVVerify{Schema: v.Schema, Name: v.Name, Exists: ex})
	}

	// large objects: compare row count and total bytes on both sides
	{
		var sc, ss, dc, ds int64
		var sOK, dOK bool
		if r, err := src.Query(`SELECT count(*), COALESCE(sum(length(data)), 0) FROM pg_catalog.pg_largeobject`); err == nil && len(r.Rows) > 0 {
			sc, ss = int64v(r, 0, 0), int64v(r, 0, 1)
			sOK = true
		}
		if r, err := tgt.Query(`SELECT count(*), COALESCE(sum(length(data)), 0) FROM pg_catalog.pg_largeobject`); err == nil && len(r.Rows) > 0 {
			dc, ds = int64v(r, 0, 0), int64v(r, 0, 1)
			dOK = true
		}
		if sOK && dOK {
			if sc != dc || ss != ds {
				rep.OK = false
				rep.Errors = append(rep.Errors, fmt.Sprintf("大对象不一致：源 %d 个/%s，目标 %d 个/%s（图片可能不可显示）",
					sc, fmtBytes(ss), dc, fmtBytes(ds)))
			} else if sc > 0 {
				e.logf("ok", "大对象校验一致：%d 个，共 %s", sc, fmtBytes(ss))
			}
		}
	}

	e.logf("info", "校验完成（用时 %s）：表 %d 张，源总行数 %d，目标总行数 %d，不一致 %d 张",
		time.Since(start).Round(time.Second), len(rep.Tables), totalSrc, totalDst, len(rep.Errors))
	return rep
}

// collectFailures returns all phase failures recorded so far.
func (s *Status) collectFailures() []Failure {
	return append([]Failure(nil), s.PhaseFailures...)
}

// runIncremental performs a watermark-based incremental sync: the target
// database and table structures must already exist (from a previous full run).
// Only rows newer than the recorded watermark are copied and upserted.
func (e *Engine) runIncremental() error {
	opts := e.opts
	ctx := e.ctx

	// ---------- source ----------
	e.setPhase(PhasePrecheck, "连接源库")
	srcCfg := opts.Source.toCfg()
	src, err := pgwire.Connect(ctx, srcCfg)
	if err != nil {
		return e.fail(fmt.Errorf("连接源库失败(%s): %w", srcCfg.Sanitized(), err))
	}
	defer src.Close()
	sessionSetup(src, true)
	srcVer := src.ServerVersion()
	e.logf("info", "源库连接成功：%s（增量水位模式）", srcVer)

	// ---------- target ----------
	e.setPhase(PhasePrecheck, "连接目标库 "+opts.TargetDB)
	tgtCfg := opts.Target.toCfg()
	tgtCfg.DB = opts.TargetDB
	tgt, err := pgwire.Connect(ctx, tgtCfg)
	if err != nil {
		return e.fail(fmt.Errorf("连接目标库 %s 失败: %w（增量模式要求目标库已存在，请先执行一次全量迁移）", opts.TargetDB, err))
	}
	defer tgt.Close()
	sessionSetup(tgt, true)
	e.logf("info", "目标库连接成功：%s", tgt.ServerVersion())

	// ---------- catalog & plans ----------
	e.setPhase(PhasePrecheck, "读取源库对象定义与增量方案探测")
	cat, err := CollectCatalog(src, &opts)
	if err != nil {
		return e.fail(err)
	}
	plans := BuildIncrPlans(src, cat, ParseIncrOverrides(opts.IncrKeys))
	usable, noneCount, pkCount, tsCount := 0, 0, 0, 0
	for _, p := range plans {
		switch p.Mode {
		case IncrPK:
			pkCount++
		case IncrTS:
			tsCount++
		default:
			noneCount++
			e.addWarning("表 %s.%s 无法增量：%s", p.Schema, p.Table, p.Note)
		}
	}
	usable = pkCount + tsCount
	e.logf("ok", "增量方案：%d 张表可增量（主键水位 %d，时间列水位 %d），%d 张表需人工处理或全量重跑", usable, pkCount, tsCount, noneCount)

	existing := map[string]bool{}
	if rr, err := tgt.Query(`SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'`); err == nil {
		for i := range rr.Rows {
			existing[str(rr, i, 0)+"."+str(rr, i, 1)] = true
		}
	}

	wm := loadWatermarks(opts.WatermarkFile)

	// ---------- build jobs ----------
	type incrJob struct {
		t      *TableInfo
		plan   IncrPlan
		where  string
		first  bool // first full merge (no watermark yet, or target table was missing)
	}
	byKey := map[string]*TableInfo{}
	for _, t := range cat.Tables {
		byKey[t.Key()] = t
	}
	var jobs []incrJob
	createdCount := 0
	collLookup := collLookupMap(cat.Collations)
	for _, p := range plans {
		if p.Mode == IncrNone {
			continue
		}
		t := byKey[p.Key()]
		if t == nil {
			continue
		}
		w, ok := wm[p.Key()]
		if ok && (w.Mode != p.Mode || w.Column != p.Column) {
			e.addWarning("表 %s 的水位记录(%s/%s)与当前方案(%s/%s)不一致，将从头全量合并（幂等，不影响正确性）", p.Key(), w.Mode, w.Column, p.Mode, p.Column)
			w = Watermark{}
		}
		if !ok {
			w = Watermark{}
		}
		full := w.Value == ""
		if !existing[p.Key()] {
			if t.PartOf != 0 || len(t.ParentNames) > 0 {
				e.addWarning("表 %s 为分区/继承表且目标库缺失，增量模式无法自动建表，请先执行全量迁移", p.Key())
				continue
			}
			byOid := map[uint32]*TableInfo{t.OID: t}
			if _, err := tgt.Exec(createTableStmt(t, byOid, collLookup)); err != nil {
				e.addWarning("自动创建目标表 %s 失败：%s（已跳过）", p.Key(), oneLine(err.Error()))
				continue
			}
			existing[p.Key()] = true
			createdCount++
			full = true
			e.logf("ok", "目标库缺少表 %s，已自动创建并做首次全量合并", p.Key())
		}
		where, werr := buildWhere(p, w)
		if werr != nil {
			e.addWarning("表 %s 构造增量条件失败：%s（已跳过）", p.Key(), oneLine(werr.Error()))
			continue
		}
		jobs = append(jobs, incrJob{t: t, plan: p, where: where, first: full})
	}
	if createdCount > 0 {
		e.logf("info", "本次增量自动补建了 %d 张缺失的目标表", createdCount)
	}

	// ---------- sync ----------
	e.st.mu.Lock()
	e.st.Tables = make([]TableStat, 0, len(jobs))
	var est uint64
	for _, j := range jobs {
		er := j.t.EstRows
		if er < 0 {
			er = 0
		}
		est += uint64(er)
		e.st.Tables = append(e.st.Tables, TableStat{
			Schema: j.t.Schema, Name: j.t.Name, EstRows: er, SizeBytes: j.t.SizeBytes, State: "pending",
		})
	}
	e.st.TotalTables = len(jobs)
	e.st.EstRows = est
	e.st.mu.Unlock()

	if len(jobs) > 0 {
		e.setPhase(PhaseData)
		nw := e.opts.Workers
		if nw > len(jobs) {
			nw = len(jobs)
		}
		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < nw; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				var srcConn, dstConn *pgwire.Conn
				defer func() {
					if srcConn != nil {
						srcConn.Close()
					}
					if dstConn != nil {
						dstConn.Close()
					}
				}()
				for i := range jobs {
					if ctx.Err() != nil {
						return
					}
					j := jobs[i]
					if srcConn == nil {
						sc, err := pgwire.Connect(ctx, opts.Source.toCfg())
						if err != nil {
							e.logf("error", "工作线程 %d 连接源库失败：%v", worker, err)
							return
						}
						sessionSetup(sc, false)
						srcConn = sc
						c2 := opts.Target.toCfg()
						c2.DB = opts.TargetDB
						dc, err := pgwire.Connect(ctx, c2)
						if err != nil {
							e.logf("error", "工作线程 %d 连接目标库失败：%v", worker, err)
							return
						}
						sessionSetup(dc, false)
						dstConn = dc
					}
					key := j.t.Key()
					jt := time.Now()
					e.markTable(key, func(s *TableStat) { s.State = "copying" })
					e.appendCurrent(key)
					verb := "增量同步"
					if j.first {
						verb = "首次全量合并"
					}
					e.logf("info", "[W%d] %s %s（水位 %s/%s）", worker, verb, key, j.plan.Mode, j.plan.Column)
					rows, err := e.mergeCopy(ctx, worker, srcConn, dstConn, j.t, j.where, j.plan.PKCols)
					elapsed := time.Since(jt).Seconds()
					e.removeCurrent(key)
					if ctx.Err() != nil {
						e.markTable(key, func(s *TableStat) { s.State = "skipped"; s.Err = "已取消" })
						return
					}
					if err != nil {
						e.markTable(key, func(s *TableStat) { s.State = "failed"; s.Err = oneLine(err.Error()); s.Seconds = elapsed })
						e.bumpCounters(0, 1, 0, 0)
						e.logf("error", "[W%d] %s %s 失败：%s", worker, verb, key, oneLine(err.Error()))
						continue
					}
					e.markTable(key, func(s *TableStat) { s.State = "done"; s.Rows = rows; s.Seconds = elapsed })
					e.bumpCounters(1, 0, uint64(rows), 0)
					e.logf("ok", "[W%d] %s %s 完成：合并 %d 行，用时 %.1fs", worker, verb, key, rows, elapsed)
				}
			}(w)
		}
		wg.Wait()
		e.logf("info", "增量同步结束，用时 %s", time.Since(start).Round(time.Second))
	} else {
		e.logf("info", "没有可增量同步的表（全部为 none 或无水位变化）")
	}

	// ---------- large objects ----------
	e.setPhase(PhaseData, "迁移大对象(Large Object)")
	e.migrateLargeObjects(src, tgt)

	// ---------- refresh watermarks ----------
	e.setPhase(PhasePostData, "刷新增量水位")
	syncedPlans := make([]IncrPlan, 0, len(jobs))
	for _, j := range jobs {
		syncedPlans = append(syncedPlans, j.plan)
	}
	recordWatermarks(ctx, src, syncedPlans, opts.WatermarkFile, e.logf)

	// ---------- verify ----------
	if opts.Verify {
		e.setPhase(PhaseVerify)
		e.logf("info", "开始校验（增量模式下若源库删除过行，行数可能不一致，属预期行为）……")
		rep := e.runVerify(src, tgt, cat, existing)
		e.st.mu.Lock()
		e.st.Verification = rep
		e.st.mu.Unlock()
		if rep.OK {
			e.logf("ok", "校验通过：所有表行数与序列值一致")
		} else {
			for _, msg := range rep.Errors {
				e.addWarning("%s", msg)
			}
		}
	}

	e.setPhase(PhaseDone)
	e.st.mu.Lock()
	e.st.State = "done"
	now := time.Now()
	e.st.FinishedAt = &now
	e.st.mu.Unlock()
	e.logf("ok", "增量同步完成！本次合并行数 %d", atomic.LoadUint64(&e.st.RowsCopied))
	return nil
}
