# pgmigrate —— PostgreSQL 11 → 18 全库迁移工具

纯 Go（仅标准库，零第三方依赖）实现的 PostgreSQL 数据库整体迁移工具，专为
**PG 11（源）→ PG 18（目标）** 设计，内置中文 Web 操作页面，无需安装
`pg_dump` / `pg_restore` / `psql` 等任何外部客户端。

典型场景：把 `substation_new` 数据库从旧服务器的 PostgreSQL 11 整库搬迁到
PostgreSQL 18。

## 功能特性

- **全库对象迁移**：扩展、模式(schema)、枚举/域/复合/范围类型、排序规则、
  函数/存储过程/聚合、序列（含当前值与 OWNED BY 归属）、表（含 UNLOGGED、
  标识列 IDENTITY、表参数、声明式分区、继承表）、视图、物化视图（含刷新）、
  主键/唯一/检查/外键约束、索引（含分区索引挂接）、触发器、行级安全与策略、
  对象注释、库级参数（ALTER DATABASE SET）
- **数据迁移**：`COPY TO STDOUT → COPY FROM STDIN` 流式管道，多线程并行，
  全程不落地磁盘，内存占用恒定
- **目标库管理**：自动检测/创建目标库（继承源库编码与排序规则，跨平台自动
  降级重试），可选"删除重建"，拒绝在非空库上误迁移
- **校验**：迁移完成后逐表 `count(*)` 比对 + 序列值比对 + 物化视图存在性检查
- **Web 页面**：连接配置、连接测试、实时进度（行数/字节/并发表）、逐表状态、
  运行日志、校验报告、一键取消、报告下载（JSON）
- **命令行模式**：`run` 子命令适合脚本/定时任务，支持 JSON 配置文件
- **认证**：trust / cleartext / MD5 / SCRAM-SHA-256（PG10+ 默认），支持 SSL
  （disable / prefer / require / verify-full）
- **稳健性**：DDL 依赖顺序自动重试（最多 6 轮）；跨平台排序规则缺失时自动
  剥离 `COLLATE` 子句重试；单表失败不影响其它表；所有不可迁移项以警告形式
  明确列出

## 快速开始（Web 模式，推荐）

把二进制上传到 **Linux 服务器**（与源库/目标库网络互通的任意机器均可，
无需安装任何依赖）：

```bash
chmod +x pgmigrate-linux-amd64      # 首次运行需赋予执行权限
./pgmigrate-linux-amd64 serve --addr :8765
```

在您自己的电脑浏览器打开 `http://<服务器IP>:8765`（注意放行防火墙端口）：

1. 填写 **源库**（PG11）连接信息，数据库名 `substation_new`，点"测试源库连接"
2. 填写 **目标库**（PG18）连接信息，"管理库"填 `postgres`（用于创建目标库），
   "目标库"填 `substation_new`，点"测试目标库连接"
3. 按需勾选选项（默认：拷贝数据 + 完成后校验 + 自动建库 + 刷新物化视图 + ANALYZE）
4. 点"**开始迁移**"，实时查看进度与日志；结束后查看校验结果，可下载 JSON 报告

> 鲲鹏/飞腾等 ARM 服务器请使用 `pgmigrate-linux-arm64`。Windows 机器上也可
> 运行 `pgmigrate-windows-amd64.exe`，效果相同。

## 命令行模式

```bash
./pgmigrate-linux-amd64 run \
  -source "host=pg11-host.example.com port=5432 user=postgres password=xxx dbname=substation_new sslmode=prefer" \
  -target "host=pg18-host.example.com port=5432 user=postgres password=xxx dbname=postgres sslmode=prefer" \
  -workers 8
```

或使用 JSON 配置文件（字段与 Web API 相同，见 `config.example.json`）：

```bash
./pgmigrate-linux-amd64 run -config config.json
```

常用参数：`-targetdb substation_new`（目标库名，默认与源库同名）、`-workers N`
（并行线程数）、`-drop-target`（删除重建目标库）、`-exclude "public.tmp*"`
（排除表，支持通配符）、`-owners/-acls/-roles`（属主/权限/角色，默认关闭）、
`-no-data`（仅迁移结构）、`-no-verify`。

## 迁移流程（引擎内部顺序）

1. 连接源库校验版本，读取全库对象定义（系统目录）
2. 连接目标库；目标库不存在则按源库编码/区域创建（三段降级尝试）；
   已存在且非空则拒绝（除非 `-drop-target`，使用 `DROP DATABASE WITH (FORCE)`）
3. 安装扩展（目标服务器缺少的扩展会警告并列出受影响对象）
4. 建模式 → 排序规则 → 枚举 → 域 → 复合类型 → 范围类型 → 函数/聚合 → 序列
5. 建表（分区父表 → 分区 → 普通表，继承关系拓扑排序）、视图、物化视图（空壳）
6. **并行 COPY 数据**（线程数可配；旧继承父表用 `SELECT * FROM ONLY` 防重复）
7. 同步序列当前值（`setval`）
8. 后置对象：行级安全 → 主键/唯一/检查约束 → 索引（含分区索引 ATTACH）→
   触发器 → 序列归属 → 策略 → 注释 → 刷新物化视图 → 角色/属主/权限（可选）→
   外键 → 库参数 → ANALYZE
9. 逐表行数校验、序列值校验、物化视图检查，输出报告

> 数据先装载、后建索引/约束/外键，与 pg_restore 相同，吞吐最优。

## 离线容器环境与扩展（重要）

目标库若运行在**离线容器**中，只有**容器镜像里自带的扩展**才能安装：

1. **迁移前先点"扩展兼容性检查"按钮**（只读操作）：工具会对比源库已装扩展与
   目标服务器 `pg_available_extensions`，逐个给出 ✅兼容 / ⚠版本不同 / ❌缺失
   结论，缺失的会在迁移开始前暴露，避免迁到一半才发现
2. **官方 postgres 镜像自带全部 contrib 扩展**（pg_trgm、uuid-ossp、hstore、
   ltree、fuzzystrmatch 等），这类扩展在离线容器中可直接安装
3. **第三方扩展必须换镜像**：PostGIS 用 `postgis/postgis:18` 系列，TimescaleDB
   用 `timescale/timescaledb` 系列；换镜像即换 tag，无需联网
4. **版本差异自动处理**：源库 PG11 的扩展版本（如 pg_trgm 1.4）与镜像自带版本
   （如 1.6）不同时，工具自动按镜像版本安装并记录警告；工具按"精确版本 →
   目标版本 → 带模式 → 默认"四级尝试，不会因版本钉死而中断
5. 扩展所在的**自定义 schema 会保留**（仅对可重定位扩展生效，如 `WITH SCHEMA postgis`）
6. 若某扩展在目标侧确实无法安装，工具不会中断，而是逐条列出依赖它而创建失败
   的对象（表/函数等），便于评估换镜像或手工处理
7. 工具本身零依赖、纯静态二进制，运行它的机器同样无需联网

## 增量迁移（水位模式）

适合"先全量验证、低峰期补增量、短停机切换"的流程，**无需修改源库任何配置、
无需重启 PG11**：

1. **第一次：全量迁移**（不勾选增量）。完成后工具自动探测每张表的增量方案并
   记录水位到 `pgmigrate-watermark.json`：
   - 表有**自增主键** → 主键水位（捕获新增行）
   - 表有 **update/modify 类时间戳列** → 时间列水位（捕获新增 + 修改，幂等 upsert）
   - 无主键 / 复合主键 → 不支持增量（报告中列出，可人工指定单调列覆盖，或定期全量重跑）
2. **之后任意次：增量同步**——勾选"增量同步模式"再运行：跳过建库与结构，只
   搬运水位之后的行，按主键 upsert 合并（重复数据自动去重，可放心反复执行）；
   目标库缺少的新表会自动建表并做首次全量合并；每次完成后水位自动推进
3. **切换**：停写源库 → 最后跑一次增量 → 看校验结果 → 业务切到 PG18

使用前可点"**增量方案探测**"按钮预览每张表的方式/水位列/说明。

**限制（重要）**：水位法**不捕获 DELETE**（删除的行不会同步到目标）；若业务
会删除数据，切换前以最后一次校验结果为准，差异行需人工核对。也可用
`增量列覆盖`（`public.t1=gxsj:ts` 格式）人工指定水位列。

CLI：`-incremental` 开启，`-incr-keys "public.t1=gxsj:ts,public.t2=id:pk"` 覆盖，
`-watermark` 指定水位文件路径。

## PostGIS 空间数据迁移（替代 postgis_restore.pl）

如果源库装了 PostGIS（如 `substation_new` 之前用 postgis_restore.pl 处理的
场景），本工具内置了等价处理，**无需再用 Perl 脚本过滤 dump**：

- 扩展自有的类型/函数/操作符/表（geometry 类型、st_* 函数、spatial_ref_sys
  表等）**不再重复创建**——先在目标库安装镜像自带的 PostGIS（自动处理 2.5 →
  3.x 的版本降级），再迁移用户对象
- **`spatial_ref_sys` 数据自动合并**（与官方 postgis_restore.pl 语义一致）：
  源库坐标系经临时表以 `INSERT ... ON CONFLICT DO NOTHING` 载入——目标镜像
  自带的 SRID 定义保留（更新更准），源库自定义 SRID（自定义坐标系）全部合入，
  不会主键冲突
- 用户表中的 `geometry` / `geography` 列按其类型修饰符原样重建，数据以文本
  COPY 传输（WKB/EWKB 跨 2.5→3.x 兼容）
- 迁移后日志与校验报告会标明扩展表合并的行数；其它扩展内部表（非白名单）
  不做数据合并，避免破坏扩展元数据

## 已知边界（会以警告形式提示）

- **大对象（Large Object）**：已支持迁移——按 OID 原样迁入（图片等二进制内容），
  并同步属主；读取源库 `pg_largeobject` 需要超级用户权限（普通用户自动降级为
  服务端分段 API）；校验阶段会比对两侧大对象数量与字节总量。**v1.0.3 及更早
  版本不迁移大对象，库内引用的图片会无法显示——请升级到 v1.0.4+ 重跑**
- `WITH OIDS` 表：PG12+ 已移除该特性，OID 列无法迁移（表结构正常迁移）
- 外部表（FDW）：建表跳过、数据跳过（数据在远端）
- 表空间：不迁移自定义表空间，对象落在目标库默认表空间
- 跨操作系统时列级 `COLLATE`（如 Windows 区域名在 Linux 上不存在）会自动
  剥离并重试，结果以警告说明
- 角色/属主/权限默认**不**迁移（避免权限不足中断）；需要时显式开启选项；
  迁移角色需要能读源库 `pg_authid`（超级用户）。注意：大对象迁移后属主会
  尽量同步，若属主角色在目标库不存在，应用账号可能读不了图片——请开启
  "迁移角色"或手动授权

## 源码构建

```bash
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o pgmigrate-linux-amd64 .
```

零第三方依赖，任何安装了 Go 1.21+ 的机器（离线亦可）直接构建；交叉编译只需
设置 `GOOS=linux GOARCH=amd64|arm64`。`dist/` 下已提供预编译二进制：

| 文件 | 平台 |
|---|---|
| `pgmigrate-linux-amd64` | Linux x86-64（静态 ELF，无 glibc 依赖） |
| `pgmigrate-linux-arm64` | Linux ARM64（鲲鹏/飞腾等） |
| `pgmigrate-windows-amd64.exe` | Windows 64 位 |
| `pgmigrate-windows-386.exe` | Windows 32 位 |

## 质量保障

- `internal/pgwire`：7 项协议级单元测试（启动/查询/错误/COPY 双向流/MD5/
  SCRAM 完整密钥校验/DSN 解析），内置进程内假 PostgreSQL 服务器
- `internal/migrate`：端到端集成测试——模拟 PG11 源库与 PG18 目标库，
  驱动真实引擎完整走完"目录采集 → 建库 → DDL → COPY 数据 → 序列同步 →
  校验"全流程并断言目标端收到的每条 DDL

```bat
go test ./...
```

## 运行要求与建议

- 源库账号建议为表属主或超级用户；目标库账号需要 `CREATEDB` 权限
  （建库），建议目标端为超级用户（扩展安装、属主/权限迁移需要）
- 迁移期间请勿在源库执行 DDL；工具为一次性快照迁移，不做增量同步
- 取消任务不会回滚已完成部分；重跑前勾选"删除重建目标库"
- 大表场景可调大 `-workers`（每线程占用源/目标各 1 个连接）
