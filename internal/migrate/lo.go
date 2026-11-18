package migrate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pgmigrate/internal/pgwire"
)

// Large Object migration: substation inspection systems typically store
// images as large objects (a loid reference in a business table + the bytes
// in pg_largeobject). Rows carry the loid across with the normal table COPY,
// and this module streams the bytes over using the lo_create/lo_put API so
// images keep working after the migration (no server-side files needed).
//
// Read path  : SELECT pageno, data FROM pg_largeobject ... (superuser) with a
//              lo_get_fragment fallback per LO; bytea arrives as "\x..." hex.
// Write path : SELECT lo_create(oid)  (preserves the OID referenced by tables)
//              SELECT lo_put(oid, offset, decode('<hex>','hex'))  per ~1MB chunk
// Owner      : ALTER LARGE OBJECT oid OWNER TO <source owner> when that role
//              exists on the target, so the application account can read the
//              images without extra grants.

type loItem struct {
	oid  uint32
	size int64
}

const loChunkDecodedBytes = 1 << 20 // ~1MB per lo_put

// migrateLargeObjects copies all large objects from source to target.
// Existing target OIDs are skipped (incremental friendly).
func (e *Engine) migrateLargeObjects(src, tgt *pgwire.Conn) {
	ctx := e.ctx

	// enumerate source large objects with sizes
	r, err := src.Query(`SELECT loid, COALESCE(sum(length(data)), 0) AS bytes FROM pg_catalog.pg_largeobject GROUP BY loid ORDER BY loid`)
	if err != nil {
		e.addWarning("无法读取大对象目录（读取 pg_largeobject 需要超级用户权限），大对象数据未迁移，库内引用的大对象将无法显示：%s",
			oneLine(err.Error()))
		return
	}
	var los []loItem
	var total int64
	for i := range r.Rows {
		oid := uint32(int64v(r, i, 0))
		if oid == 0 {
			continue
		}
		los = append(los, loItem{oid: oid, size: int64v(r, i, 1)})
		total += int64v(r, i, 1)
	}
	if len(los) == 0 {
		e.logf("info", "源库没有大对象")
		return
	}
	e.logf("info", "开始迁移大对象：%d 个，共 %s（图片等二进制内容）", len(los), fmtBytes(total))

	// target existing OIDs (skip on incremental / re-runs)
	exist := map[uint32]bool{}
	if rr, err := tgt.Query(`SELECT loid FROM pg_catalog.pg_largeobject_metadata`); err == nil {
		for i := range rr.Rows {
			exist[uint32(int64v(rr, i, 0))] = true
		}
	} else {
		e.addWarning("无法查询目标库大对象清单：%s（将尝试全部迁移）", oneLine(err.Error()))
	}

	// source owners + target roles for owner transfer
	srcOwners := map[uint32]string{}
	if rr, err := src.Query(`SELECT loid, pg_get_userbyid(lomowner) FROM pg_catalog.pg_largeobject_metadata`); err == nil {
		for i := range rr.Rows {
			srcOwners[uint32(int64v(rr, i, 0))] = str(rr, i, 1)
		}
	}
	targetRoles := map[string]bool{}
	if rr, err := tgt.Query(`SELECT rolname FROM pg_catalog.pg_roles`); err == nil {
		for i := range rr.Rows {
			targetRoles[str(rr, i, 0)] = true
		}
	}

	canReadDirect := true
	if _, err := src.Query(`SELECT pageno, data FROM pg_catalog.pg_largeobject LIMIT 1`); err != nil {
		canReadDirect = false
	}

	var done, skipped, failed int64
	var copiedBytes int64
	var mu sync.Mutex
	var lastProgressLog int64

	nw := e.opts.Workers
	if nw > len(los) {
		nw = len(los)
	}
	if nw < 1 {
		nw = 1
	}
	start := time.Now()

	var wg sync.WaitGroup
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
			for i := range los {
				if ctx.Err() != nil {
					return
				}
				lo := los[i]
				mu.Lock()
				if exist[lo.oid] {
					skipped++
					mu.Unlock()
					continue
				}
				mu.Unlock()

				if srcConn == nil {
					sc, err := pgwire.Connect(ctx, e.opts.Source.toCfg())
					if err != nil {
						mu.Lock()
						failed++
						mu.Unlock()
						e.logf("error", "大对象工作线程连接源库失败：%s", oneLine(err.Error()))
						return
					}
					sessionSetup(sc, false)
					srcConn = sc
					c2 := e.opts.Target.toCfg()
					c2.DB = e.opts.TargetDB
					dc, err := pgwire.Connect(ctx, c2)
					if err != nil {
						mu.Lock()
						failed++
						mu.Unlock()
						e.logf("error", "大对象工作线程连接目标库失败：%s", oneLine(err.Error()))
						srcConn.Close()
						srcConn = nil
						return
					}
					sessionSetup(dc, false)
					dstConn = dc
				}

				n, b, err := e.copyOneLO(ctx, worker, srcConn, dstConn, lo, canReadDirect)
				mu.Lock()
				if err != nil {
					failed++
					e.logf("error", "大对象 %d 迁移失败：%s", lo.oid, oneLine(err.Error()))
				} else {
					done++
					copiedBytes += b
					total := atomic.LoadInt64(&done)
					if b := atomic.LoadInt64(&copiedBytes); b-lastProgressLog >= 64<<20 {
						lastProgressLog = b
						e.logf("info", "大对象迁移进度：%d/%d 个（%s）", total, len(los), fmtBytes(b))
					}
					_ = n
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	// owner transfer so the application account can read the images
	ownerSet := 0
	for _, lo := range los {
		owner := srcOwners[lo.oid]
		if owner == "" || !targetRoles[owner] {
			continue
		}
		if _, err := tgt.Exec(fmt.Sprintf("ALTER LARGE OBJECT %d OWNER TO %s", lo.oid, QuoteIdent(owner))); err == nil {
			ownerSet++
		}
	}

	if failed > 0 {
		e.addWarning("大对象迁移完成但存在失败：%d 成功 / %d 跳过(已存在) / %d 失败，共 %s。失败的大对象对应图片将无法显示；如因权限失败请以超级用户重跑",
			done, skipped, failed, fmtBytes(copiedBytes))
	} else {
		e.logf("ok", "大对象迁移完成：%d 个成功 / %d 个跳过(已存在)，共 %s，用时 %s；已同步属主 %d 个",
			done, skipped, fmtBytes(copiedBytes), time.Since(start).Round(time.Second), ownerSet)
	}
}

// copyOneLO streams one large object to the target, preserving its OID.
func (e *Engine) copyOneLO(ctx context.Context, worker int, srcConn, dstConn *pgwire.Conn, lo loItem, canReadDirect bool) (int64, int64, error) {
	if _, err := dstConn.Exec(fmt.Sprintf("SELECT pg_catalog.lo_create(%d)", lo.oid)); err != nil {
		return 0, 0, fmt.Errorf("lo_create(%d) 失败（OID 可能被占用）: %w", lo.oid, err)
	}

	offset := int64(0)
	var chunk strings.Builder
	chunkDecoded := 0
	flush := func() error {
		if chunkDecoded == 0 {
			return nil
		}
		sql := fmt.Sprintf("SELECT pg_catalog.lo_put(%d, %d, decode('%s','hex'))", lo.oid, offset, chunk.String())
		if _, err := dstConn.Exec(sql); err != nil {
			return err
		}
		offset += int64(chunkDecoded)
		chunk.Reset()
		chunkDecoded = 0
		return nil
	}

	if canReadDirect {
		r, err := srcConn.Query(fmt.Sprintf("SELECT pageno, data FROM pg_catalog.pg_largeobject WHERE loid = %d ORDER BY pageno", lo.oid))
		if err != nil {
			return offset, 0, err
		}
		for i := range r.Rows {
			v := r.ColPtr(i, 0+1)
			if v == nil {
				continue
			}
			hexData := strings.TrimPrefix(*v, "\\x")
			chunk.WriteString(hexData)
			chunkDecoded += len(hexData) / 2
			if chunkDecoded >= loChunkDecodedBytes {
				if err := flush(); err != nil {
					return offset, 0, err
				}
			}
		}
	} else {
		// fallback: server-side fragment API (works for LO owners without
		// direct pg_largeobject read rights)
		const frag = 1 << 20
		var off int64
		for {
			if ctx.Err() != nil {
				return offset, 0, ctx.Err()
			}
			r, err := srcConn.Query(fmt.Sprintf("SELECT pg_catalog.lo_get_fragment(%d, %d, %d)", lo.oid, off, frag))
			if err != nil {
				return offset, 0, err
			}
			if len(r.Rows) == 0 || r.ColPtr(0, 0) == nil {
				break
			}
			hexData := strings.TrimPrefix(r.Col(0, 0), "\\x")
			if hexData == "" {
				break
			}
			chunk.WriteString(hexData)
			chunkDecoded += len(hexData) / 2
			off += int64(len(hexData) / 2)
			if chunkDecoded >= loChunkDecodedBytes {
				if err := flush(); err != nil {
					return offset, 0, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return offset, 0, err
	}
	return offset, offset, nil
}
