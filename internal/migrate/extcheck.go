package migrate

import (
	"context"
	"fmt"
	"strings"

	"pgmigrate/internal/pgwire"
)

// extLits returns quoted SQL literals for extension names.
func extLits(exts []ExtInfo) []string {
	out := make([]string, 0, len(exts))
	for _, ex := range exts {
		out = append(out, QuoteLit(ex.Name))
	}
	return out
}

// createExtensionBestEffort installs an extension trying a candidate ladder:
//   1. CREATE EXTENSION ... WITH SCHEMA s VERSION v   (exact match)
//   2. CREATE EXTENSION ... VERSION v                 (keep source version)
//   3. CREATE EXTENSION ... WITH SCHEMA s             (target default version)
//   4. CREATE EXTENSION ...                           (target default version)
//
// This matters for offline containers where only the versions bundled in the
// image exist: a pinned source version may be unavailable.
func createExtensionBestEffort(tgt *pgwire.Conn, ex ExtInfo) bool {
	base := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", QuoteIdent(ex.Name))
	withSchema := ""
	if ex.Relocatable && ex.Schema != "" && ex.Schema != "public" {
		withSchema = " WITH SCHEMA " + QuoteIdent(ex.Schema)
	}
	withVersion := ""
	if ex.Version != "" {
		withVersion = " VERSION " + QuoteLit(ex.Version)
	}
	candidates := []string{}
	if withVersion != "" && withSchema != "" {
		candidates = append(candidates, base+withSchema+withVersion)
	}
	if withVersion != "" {
		candidates = append(candidates, base+withVersion)
	}
	if withSchema != "" {
		candidates = append(candidates, base+withSchema)
	}
	candidates = append(candidates, base)
	for _, sql := range candidates {
		if _, err := tgt.Exec(sql); err == nil {
			return true
		}
	}
	return false
}

// ExtCheckItem describes one source extension and its availability on the
// target server.
type ExtCheckItem struct {
	Name          string `json:"name"`
	SrcVersion    string `json:"src_version"`
	Schema        string `json:"schema"`
	Relocatable   bool   `json:"relocatable"`
	Available     bool   `json:"available"`
	TargetVersion string `json:"target_version,omitempty"`
	Status        string `json:"status"` // ok | version-diff | missing
	Note          string `json:"note"`
}

// CheckExtensionCompatibility reads the source database's extensions and checks
// each against the target SERVER's pg_available_extensions (cluster-wide, any
// database works). Designed for offline container targets where only extensions
// bundled in the image can be installed.
func CheckExtensionCompatibility(ctx context.Context, srcCfg, tgtCfg *pgwire.Config) ([]ExtCheckItem, error) {
	src, err := pgwire.Connect(ctx, srcCfg)
	if err != nil {
		return nil, fmt.Errorf("连接源库失败: %w", err)
	}
	defer src.Close()
	r, err := src.Query(`SELECT e.extname, e.extversion, n.nspname, e.extrelocatable FROM pg_extension e
		JOIN pg_namespace n ON n.oid = e.extnamespace ORDER BY e.extname`)
	if err != nil {
		return nil, fmt.Errorf("读取源库扩展失败: %w", err)
	}
	srcExts := []ExtInfo{}
	for i := range r.Rows {
		if str(r, i, 0) == "plpgsql" {
			continue // built-in on every server
		}
		srcExts = append(srcExts, ExtInfo{
			Name: str(r, i, 0), Version: str(r, i, 1), Schema: str(r, i, 2), Relocatable: boolean(r, i, 3),
		})
	}

	tgt, err := pgwire.Connect(ctx, tgtCfg)
	if err != nil {
		return nil, fmt.Errorf("连接目标库失败: %w", err)
	}
	defer tgt.Close()

	items := []ExtCheckItem{}
	if len(srcExts) == 0 {
		return items, nil
	}
	avail := map[string]string{}
	rr, err := tgt.Query(fmt.Sprintf(`SELECT name, default_version FROM pg_available_extensions WHERE name IN (%s)`,
		strings.Join(extLits(srcExts), ", ")))
	if err != nil {
		return nil, fmt.Errorf("查询目标可用扩展失败: %w", err)
	}
	for i := range rr.Rows {
		avail[str(rr, i, 0)] = str(rr, i, 1)
	}
	for _, ex := range srcExts {
		item := ExtCheckItem{
			Name: ex.Name, SrcVersion: ex.Version, Schema: ex.Schema, Relocatable: ex.Relocatable,
		}
		ver, ok := avail[ex.Name]
		switch {
		case !ok:
			item.Status = "missing"
			item.Note = "目标服务器不存在该扩展。离线容器环境请改用包含该扩展的镜像（contrib 扩展官方 postgres 镜像已自带；PostGIS 等需 postgis/postgis 类镜像），或向容器内手动安装扩展文件后重试"
		case ver != ex.Version && ex.Version != "":
			item.Available = true
			item.TargetVersion = ver
			item.Status = "version-diff"
			item.Note = fmt.Sprintf("目标可用版本 %s 与源库版本 %s 不同，工具将按目标版本安装（contrib 类通常兼容；PostGIS 大版本差异需评估）", ver, ex.Version)
		default:
			item.Available = true
			item.TargetVersion = ver
			item.Status = "ok"
			item.Note = "兼容，可直接安装"
		}
		items = append(items, item)
	}
	return items, nil
}
