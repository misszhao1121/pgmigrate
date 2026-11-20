// pgmigrate —— PostgreSQL 11 → 18 全库迁移工具（纯 Go，零第三方依赖）。
//
// 子命令：
//   serve   启动 Web 页面（默认端口 8765），在浏览器中配置源/目标库并发起迁移
//   run     命令行模式直接执行迁移（适合脚本/自动化）
//   version 打印版本
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pgmigrate/internal/migrate"
	"pgmigrate/internal/pgwire"
	"pgmigrate/internal/web"
)

// httpServer wraps net/http with graceful shutdown.
type httpServer struct {
	addr string
	h    http.Handler
}

func (s *httpServer) listenAndServe() {
	hs := &http.Server{Addr: s.addr, Handler: s.h, ReadHeaderTimeout: 15 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("\n正在退出 Web 服务……")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = hs.Shutdown(ctx)
}

var version = "1.0.8"

func usage() {
	fmt.Print(`pgmigrate - PostgreSQL 11 → 18 全库迁移工具

用法:
  pgmigrate serve  [--addr :8765]
      启动 Web 管理页面，浏览器中配置源库/目标库并发起迁移。

  pgmigrate run -source <DSN> -target <DSN> [选项]
      命令行模式直接迁移。
      DSN 支持 URL 形式 postgres://user:pass@host:5432/db?sslmode=disable
      或关键字形式 "host=x port=5432 user=x password=x dbname=substation_new sslmode=prefer"

  pgmigrate run -config config.json
      从 JSON 配置文件读取（字段与 Web API /api/start 相同）。

run 模式常用选项:
  -targetdb string        目标数据库名（默认与源库同名）
  -workers int            并行拷贝线程数 (默认 4)
  -no-data                跳过数据拷贝（仅结构）
  -no-verify              跳过校验
  -no-analyze             跳过 ANALYZE
  -no-matview-refresh     跳过物化视图刷新
  -drop-target            删除重建目标库
  -owners                 迁移属主
  -acls                   迁移权限(ACL)
  -roles                  迁移角色
  -incremental            增量同步模式（需先完成一次全量，按水位只同步新增/修改）
  -incr-keys string       增量列覆盖 schema.table=column[:pk|ts]，逗号分隔
  -watermark string       水位文件路径（默认 pgmigrate-watermark.json）
  -exclude string         排除表（逗号分隔，支持通配符 public.tmp*）
  -include string         仅迁移这些表（逗号分隔，支持通配符）

  pgmigrate version
`)
}

func main() {
	log.SetFlags(log.LstdFlags)
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && !startsWithDash(args[0]) {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "serve":
		runServe(args)
	case "run":
		if err := runHeadless(args); err != nil {
			log.Fatalf("迁移失败: %v", err)
		}
	case "version", "-v", "--version":
		fmt.Println("pgmigrate", version)
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func startsWithDash(s string) bool { return len(s) > 0 && s[0] == '-' }

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8765", "Web 服务监听地址")
	cfgFile := fs.String("config", "", "预填配置文件(JSON)")
	_ = fs.Parse(args)
	srv := web.New()
	if *cfgFile != "" {
		b, err := os.ReadFile(*cfgFile)
		if err != nil {
			log.Fatalf("读取配置文件失败: %v", err)
		}
		var c map[string]interface{}
		if err := json.Unmarshal(b, &c); err != nil {
			log.Fatalf("配置文件解析失败: %v", err)
		}
		srv.SetDefaults(c)
		fmt.Println("已加载预填配置（页面上仍可修改）")
	} else if b, err := os.ReadFile("pgmigrate-last.json"); err == nil {
		var c map[string]interface{}
		if err := json.Unmarshal(b, &c); err == nil {
			srv.SetDefaults(c)
			fmt.Println("已自动预填上次成功发起迁移时的连接配置（pgmigrate-last.json）")
		}
	}
	hint := ""
	displayAddr := *addr
	if strings.HasPrefix(*addr, ":") {
		displayAddr = "127.0.0.1" + *addr
		hint = fmt.Sprintf("（或 http://<本机IP>%s）", *addr)
	}
	fmt.Printf("=============================================================\n")
	fmt.Printf("  PostgreSQL 11 → 18 全库迁移工具  v%s\n", version)
	fmt.Printf("  请用浏览器打开:  http://%s  %s\n", displayAddr, hint)
	fmt.Printf("  按 Ctrl+C 退出\n")
	fmt.Printf("=============================================================\n")
	httpSrv := &httpServer{addr: *addr, h: srv.Handler()}
	httpSrv.listenAndServe()
}

func runHeadless(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	srcDSN := fs.String("source", "", "源库 DSN（PostgreSQL 11）")
	tgtDSN := fs.String("target", "", "目标服务器 DSN（PostgreSQL 18，dbname 为管理库，如 postgres）")
	cfgFile := fs.String("config", "", "JSON 配置文件（优先）")
	targetDB := fs.String("targetdb", "", "目标数据库名（默认与源库同名）")
	workers := fs.Int("workers", 4, "并行拷贝线程数")
	noData := fs.Bool("no-data", false, "跳过数据拷贝")
	noVerify := fs.Bool("no-verify", false, "跳过校验")
	noAnalyze := fs.Bool("no-analyze", false, "跳过 ANALYZE")
	noMV := fs.Bool("no-matview-refresh", false, "跳过物化视图刷新")
	drop := fs.Bool("drop-target", false, "删除重建目标库")
	owners := fs.Bool("owners", false, "迁移属主")
	acls := fs.Bool("acls", false, "迁移权限(ACL)")
	roles := fs.Bool("roles", false, "迁移角色")
	incr := fs.Bool("incremental", false, "增量同步模式（需先完成一次全量，按水位只同步新增/修改）")
	incrKeys := fs.String("incr-keys", "", "增量列覆盖 schema.table=column[:pk|ts]，逗号分隔")
	wmFile := fs.String("watermark", "", "水位文件路径（默认 pgmigrate-watermark.json）")
	exclude := fs.String("exclude", "", "排除表（逗号分隔）")
	include := fs.String("include", "", "仅迁移这些表（逗号分隔）")
	_ = fs.Parse(args)

	split := func(s string) []string {
		out := []string{}
		for _, p := range splitComma(s) {
			out = append(out, p)
		}
		return out
	}
	opts := migrate.Options{
		Workers:         *workers,
		CopyData:        !*noData,
		Verify:          !*noVerify,
		Analyze:         !*noAnalyze,
		RefreshMatviews: !*noMV,
		DropTarget:      *drop,
		CreateTarget:    true,
		TargetDB:        *targetDB,
		MigrateOwners:   *owners,
		MigrateACLs:     *acls,
		MigrateRoles:    *roles,
		Incremental:     *incr,
		IncrKeys:        split(*incrKeys),
		WatermarkFile:   *wmFile,
	}
	opts.ExcludeTables = split(*exclude)
	opts.IncludeTables = split(*include)

	if *cfgFile != "" {
		b, err := os.ReadFile(*cfgFile)
		if err != nil {
			return fmt.Errorf("读取配置文件失败: %w", err)
		}
		if err := json.Unmarshal(b, &opts); err != nil {
			return fmt.Errorf("配置文件解析失败: %w", err)
		}
	} else {
		if *srcDSN == "" || *tgtDSN == "" {
			usage()
			return fmt.Errorf("必须提供 -source 与 -target 或 -config")
		}
		sc, err := pgwire.ParseDSN(*srcDSN)
		if err != nil {
			return fmt.Errorf("源库 DSN 错误: %w", err)
		}
		tc, err := pgwire.ParseDSN(*tgtDSN)
		if err != nil {
			return fmt.Errorf("目标库 DSN 错误: %w", err)
		}
		opts.Source = migrate.ConnInfo{Host: sc.Host, Port: sc.Port, User: sc.User, Password: sc.Password, DB: sc.DB, SSLMode: sc.SSLMode}
		opts.Target = migrate.ConnInfo{Host: tc.Host, Port: tc.Port, User: tc.User, Password: tc.Password, DB: tc.DB, SSLMode: tc.SSLMode}
	}
	opts.CreateTarget = true
	if opts.TargetDB == "" {
		opts.TargetDB = opts.Source.DB
	}

	// 打印进度到 stdout
	eng := migrate.New(opts, func(level, msg string) {
		fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), msg)
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n收到中断信号，正在取消……")
		eng.Cancel()
	}()

	err := eng.Run()

	// 汇总
	st := eng.Status()
	if st.Verification != nil {
		fmt.Println("\n===== 校验汇总 =====")
		bad := 0
		for _, t := range st.Verification.Tables {
			if !t.OK {
				bad++
				fmt.Printf("  ✗ %s.%s  源=%d  目标=%d\n", t.Schema, t.Name, t.Src, t.Dst)
			}
		}
		if bad == 0 {
			fmt.Printf("  ✓ %d 张表行数全部一致\n", len(st.Verification.Tables))
		}
	}
	if st.State == "canceled" {
		return fmt.Errorf("迁移已取消")
	}
	return err
}

func splitComma(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' || r == '，' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
