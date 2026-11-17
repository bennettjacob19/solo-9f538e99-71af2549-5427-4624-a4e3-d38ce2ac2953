# 制品目录历史查询服务

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：先整体写 SQLite（行变更 + 提交点同一事务）
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）  ←→  FastAPI 网页 / JSON API
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| 多行事务整体可见 | 一个源事务的全部行变更与提交点在**同一个 SQLite 事务**落盘，不存在半条事务 |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type` 与提交点；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| 源断连 / 强杀重启后续接 | 复制槽持久化在源库；应用重启从槽位重放，SQLite 以源事务 xid 为主键幂等去重；断线自动指数/固定退避重连 |
| 不确认尚未落盘的更改 | 严格“先 SQLite commit，后 `send_feedback(flush_lsn=...)`”；keepalive 与无关消息一律不回送反馈。自测断言槽 `confirmed_flush_lsn` 永不超前已落盘提交点 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR`；未到 STREAMING 时 `/api/points`、`/api/directory`、`/api/history/*` 一律 503 |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |

### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、基线 LSN、保留水位、最近提交点/消息 LSN、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN 与数值化 LSN，支持按提交点过滤与编号重建。
- `retention_events`：每次基线固化（历史清理）的审计记录——保留起点、
  清除的事务数/变更行数、新基线行数、VACUUM 前后页数。

## 1.5 基线固化（历史清理 / 保留起点）

值班工程师在网页提交点表选择一个**已捕获事务**作为保留起点，把该点的整张
目录原子固化为新基线，并清除更早的逐笔变更，控制 SQLite 历史库增长。

### 正确性不变量

| 需求 | 实现方式 |
| --- | --- |
| 不出现半份基线 | 快照该点目录、删除旧变更/事务、插入新基线、更新水位、写审计全部在**同一个 SQLite 事务**内完成；中途强杀整体回滚，重启后库内状态与调用前完全一致 |
| 旧提交点明确不可查 | 记录保留水位 `retention_horizon`；早于水位的提交点 `/api/directory` 明确返回 **410 Gone**（区别于从未捕获的 404），且不再出现在提交点列表 |
| 起点及之后目录准确 | 目录重建按 `(commit_lsn_num DESC, id DESC)` 排序取每个编号最新一条——固化产生的新基线行自增 id 更大，必须先按 LSN 定时序，不能只按 id |
| 自新基线起单编号历史准确 | 水位之前的变更链随清理删除，保留下来的编号历史从新 BASELINE 行开始；固化点已删除的编号历史为空，不会残留 |
| 已清理事务不得重现 | 复制流重放出 LSN ≤ 水位的事务时，`persist_transaction` 判为 `before_horizon` 拒绝落盘（含水位点本身，其 txns 记录已删），但仍 `send_feedback` 确认槽位，避免卡流 |
| 与逻辑复制捕获交错安全 | 存储层所有写入在同一把进程内锁串行化；固化与捕获线程并发时互不破坏，多行事务仍整体可见 |
| 复用腾出的空间 | 固化事务提交后执行 `VACUUM` + `PRAGMA wal_checkpoint(TRUNCATE)`，把空闲页还给文件系统；实测大库可缩到原大小的约 2%，后续写入复用空闲页 |
| 槽位不确认未落盘变更 | 固化只动本地 SQLite，不改变"先 commit 后 send_feedback"的顺序；槽 `confirmed_flush_lsn` 永不超前已落盘位置 |

### 新增 HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/retention/freeze` | 请求体 `{"point": "0/16B3748"}`，把该点目录固化为新基线 |
| GET | `/api/retention/events` | 历次固化的审计记录 |

`/api/directory` 状态码：固化水位之前的点返回 **410**，从未捕获的 LSN 返回 **404**，
非法 LSN 格式返回 **400**，未就绪返回 **503**。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章、复制槽、基线/最近提交 LSN、已捕获事务数、
   历史行数、最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。
3. **提交点回看**：列出 BASELINE 与每个源事务提交点（LSN、xid、时间、
   变更行数），点击任一行即查看该时刻整张目录；每行带“固化为新基线”按钮。
4. **单编号完整历史**：自当前基线起含 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”。
5. **基线固化 / 历史清理**：选定保留起点一键固化，展示清除量、VACUUM
   前后页数与历次审计；已清理旧点回看时明确提示 410 不可查。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚 |
| GET | `/api/points` | 全部可查询提交点（未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录；已清理旧点 410 |
| GET | `/api/history/{artifact_no}` | 单编号自当前基线起的完整变更链 |
| POST | `/api/retention/freeze` | 选定提交点固化为新基线并清理更早历史 |
| GET | `/api/retention/events` | 历次基线固化审计记录 |

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
```

## 5. 自测

### 5.1 解析器与存储层单元测试

```bash
python3 -m unittest tests.test_decoder tests.test_storage -v
```

- `test_decoder`：BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、其他表/DDL 忽略。
- `test_storage`：固化边界与快照、新基线 id 更大时的目录重建、单编号历史
  自新基线起准确、已清理事务（含水位点本身）重放被拦截、重复固化、非法起点、
  非 STREAMING 拒绝、事务原子回滚。

### 5.2 端到端自测（65 项断言）

`tests/selftest.sh` 在本机自动拉起一个便携 PostgreSQL（不依赖 Docker）、
启停应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh
```

覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢；
8. **基线固化边界**：固化与源写入并发交错；旧提交点 410、未知点 404、
   重复固化旧点 410；固化点目录前后逐字节一致且全部来自新 BASELINE；
   起点之后的点仍可查；事务数/历史行数下降、VACUUM 后页数不增长；
   固化后普通 SQL 继续捕获、槽位不超前落盘点、审计记录落盘；
9. **固化中强杀**：测试钩子在固化事务 COMMIT 前 `os._exit`，重启后无半份
   基线、无重复、审计不新增、目录不变；重试固化成功且已清理事务不重现；
9b. 重启重放后保留水位不变、已清理事务不回弹、槽位安全、复制流不卡死；
9c. **真实 walsender 重放**（`tests/replay_guard_check.py`，独立小型
   PG+SQLite 自包含）：预建临时槽重放固化前的逻辑流，确认 LSN ≤ 水位的
   事务全部被 `before_horizon` 拦截、库状态零变化、重启后新事务仍正常捕获；
10. SQLite 与 PostgreSQL 数据目录均落盘持久化。

最近一次结果：**65 passed / 0 failed**；单元测试 **13 passed / 0 failed**。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |
| `TEST_FREEZE_CRASH_HOOK` | `0`(关) | 测试钩子：置 `1` 才允许固化 COMMIT 前强杀，生产默认禁用，API 字段会被忽略 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 沙箱环境未提供 Docker，Compose 文件未经本机 `docker compose` 实跑，
  但应用本体已用同等配置在本机 PostgreSQL 15 + uvicorn 下完成全部端到端
  验证；Compose 镜像基于官方 `python:3.12-slim` 与 `postgres:16-bookworm`。
