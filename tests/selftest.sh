#!/usr/bin/env bash
# 端到端自测：在本机用便携 PostgreSQL + uvicorn 跑全链路验证。
# 用法：bash tests/selftest.sh
# 不依赖 Docker；Docker Compose 场景的启动方式见 README。
set -u
cd "$(dirname "$0")/.."
REPO=$PWD

# ------------------------------------------------------------- 环境定位
PGBIN=${PGBIN:-/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin}
PGCLIENT=${PGCLIENT:-/tmp/pgdeb/client/usr/lib/postgresql/15/bin}
export PATH="$PGCLIENT:$PGBIN:$PATH"
PGDATA=${PGDATA:-/tmp/selftest-pgdata}
PGPORT=55433
PGSOCK=/tmp/selftest-pgsock
SQLDIR=/tmp/selftest-sqlite
APPPORT=8011
BASE="http://127.0.0.1:$APPPORT"
SHA64="a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
DSN="host=$PGSOCK port=$PGPORT dbname=postgres user=postgres"
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (got '$1' want '$2')"; fi; }
api()  { curl -s -m 10 "http://127.0.0.1:$APPPORT$1"; }
wait_phase(){ # wait_phase <expected> <timeout_s>
  local want=$1 deadline=$((SECONDS+$2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(api /api/progress | jq -r .phase)" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
start_app(){ # start_app <sqlite_dir> [init_sleep_ms]
  local dir=$1 sleep_ms=${2:-0}
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$dir"
  PG_DSN="$DSN" SQLITE_PATH="$dir/history.db" CAPTURE_INIT_SLEEP_MS="$sleep_ms" \
    TEST_FREEZE_CRASH_HOOK="${TEST_FREEZE_CRASH_HOOK:-0}" \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
_wait_app_ready(){ # _wait_app_ready <expected_pid>
  local expected=$1
  for _ in $(seq 1 40); do
    if ! kill -0 "$expected" 2>/dev/null; then
      echo "new app exited early:"; cat /tmp/selftest-app.log; return 1
    fi
    if curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1; then
      # 确认响应者确实是新进程而不是占用端口的旧实例
      [ "$(app_pid)" = "$expected" ] && return 0
    fi
    sleep 0.5
  done
  echo "app failed to become ready:"; cat /tmp/selftest-app.log; return 1
}
stop_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do
    app_pid | grep -q . || return 0
    sleep 0.5
  done
}
force_kill_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
  sleep 1
}
app_pid(){ ps -eo pid,comm,args | awk '$2=="python3" && /uvicorn app.main:app/ {print $1}'; }
pg_ctl_q(){ pg_ctl -D "$PGDATA" -l /tmp/selftest-pg.log "$@"; }

cleanup(){
  stop_app || true
  pg_ctl_q stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ------------------------------------------------------------- 0. 准备源库
echo "=== 0. 准备全新源 PostgreSQL（wal_level=logical）==="
pg_ctl_q stop -m fast >/dev/null 2>&1 || true
rm -rf "$PGDATA" "$PGSOCK" "$SQLDIR"; mkdir -p "$PGSOCK"
initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGSOCK'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
pg_ctl_q start -w >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -q -f init/01_schema.sql
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc 'select count(*) from artifacts')" \
      "3" "源表初始化 3 行示例数据"

# ------------------------------------------------------------- 1. 初始化未完成不开放历史（503）
echo "=== 1. 初始化窗口历史接口 503 ==="
start_app "$SQLDIR" 8000
sleep 1
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/points")
check "$code" "503" "INITIALIZING 期间 /api/points 返回 503"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=0/1")
check "$code" "503" "INITIALIZING 期间 /api/directory 返回 503"
check "$(api /api/progress | jq -r .phase)" "INITIALIZING" "进度接口暴露 INITIALIZING"
wait_phase STREAMING 15 && ok "初始化完成进入 STREAMING" || bad "初始化超时"
base_lsn=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
[ -n "$base_lsn" ] && ok "基线提交点存在: $base_lsn" || bad "缺少基线提交点"
check "$(api "/api/directory?point=$base_lsn" | jq '.artifacts|length')" "3" "基线目录 3 行"

# ------------------------------------------------------------- 2. 基线前开始、基线后提交的事务不漏
echo "=== 2. 跨基线事务（初始化窗口开启，基线后提交）==="
stop_app; sleep 1
rm -rf "$SQLDIR"
# 同步等待旧槽完全删除，避免新实例误走 resume 分支
for _ in $(seq 1 20); do
  psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots" 2>/dev/null || true
  n=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select count(*) from pg_replication_slots")
  [ "$n" = "0" ] && break
  sleep 0.5
done
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from pg_replication_slots")" "0" "旧复制槽已彻底删除，确保走全新基线"
# 清理历史失败运行可能残留的测试行（应用已停，不会再写入），
# 把源目录恢复到与 init/01_schema.sql 一致的 3 行初始状态
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "delete from artifacts where artifact_no not in (1001,1002,1003)" >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "insert into artifacts values
   (1001,'billing-gateway','1.4.2','a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90'),
   (1002,'risk-engine','2.7.0','0f1e2d3c4b5a69788796a5b4c3d2e1f009182736455463728190aabbccddeeff'),
   (1003,'notify-hub','0.9.3','ffeeddccbbaa9988776655443322110099887766554433221100998877665544')
   on conflict do nothing" >/dev/null
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc "select count(*) from artifacts")" \
      "3" "源目录已重置为 3 行初始状态"
# 用应用内置测试钩子确定性构造"基线前开始、基线后提交"的事务：
# 应用在建槽/基线前打开源事务写入 2001 并持有，基线落盘后才提交。
start_app_hook(){
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$SQLDIR"
  PG_DSN="$DSN" SQLITE_PATH="$SQLDIR/history.db" \
    TEST_INFLIGHT_ARTIFACT_NO=2001 TEST_INFLIGHT_SLEEP_S=3 \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
start_app_hook || bad "带钩子的应用启动失败"
wait_phase STREAMING 20 || bad "钩子场景初始化超时"
grep -q "TEST hook: inflight transaction opened" /tmp/selftest-app.log \
  && ok "测试钩子事务已在基线前打开" \
  || bad "测试钩子未生效: $(grep -c 'TEST hook' /tmp/selftest-app.log)"
# 基线快照里不允许出现 2001（事务尚未提交）
new_base=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
check "$(api "/api/directory?point=$new_base" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "0" "跨基线事务未混入基线快照（不重）"
# 等钩子事务在基线后提交并被复制流捕获
for _ in $(seq 1 30); do
  n=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
  [ "$n" = "1" ] && break
  sleep 1
done
found=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
check "$found" "1" "基线前开始、基线后提交的事务被复制流补捕获，无遗漏"
sleep 2
inflight_lsn=$(api /api/points | jq -r '.points[-1].lsn')
check "$(api "/api/directory?point=$inflight_lsn" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "1" "跨基线事务在其提交点目录中可见"

# ------------------------------------------------------------- 3. 多行事务整体可见 + 删除后重用编号
echo "=== 3. 多行事务原子性与编号重用 ==="
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":3001,\"service_name\":\"g1\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"insert\",\"artifact_no\":3002,\"service_name\":\"g2\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":1002}
 ]}" >/dev/null
sleep 2
points=$(api /api/points | jq '[.points[]|select(.kind=="TXN" and .change_count>=3)]|length')
[ "$points" -ge 1 ] && ok "三行操作只产生一个提交点（整体可见）" || bad "多行事务被拆分"
latest=$(api /api/points | jq -r '.points[-1].lsn')
dir3001=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==3001)]|length')
dir1002=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==1002)]|length')
check "$dir3001" "1" "同事务插入的 3001 在提交点可见"
check "$dir1002" "0" "同事务删除的 1002 在提交点消失"
# 重用 1002
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":1002,\"service_name\":\"reborn\",\"version\":\"2\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
chain=$(api /api/history/1002 | jq -r '[.history[].change_type]|join(",")')
case "$chain" in
  *BASELINE*DELETE*INSERT*) ok "删除后重用编号，旧历史完整保留: $chain" ;;
  *) bad "编号重用历史链异常: $chain" ;;
esac

# ------------------------------------------------------------- 4. 失败整组回滚 + 输入校验
echo "=== 4. 回滚与固定结构校验 ==="
code=$(curl -s -o /tmp/r.json -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":9001,\"service_name\":\"ghost\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":999999}
 ]}")
check "$code" "400" "组内失败返回 400"
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from artifacts where artifact_no=9001")" "0" "失败整组回滚，源库无残留"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d '{"operations":[{"op":"insert","artifact_no":1,"service_name":"x","version":"1","sha256":"bad"}]}')
check "$code" "422" "非法 sha256 被 pydantic 拒绝"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=not-a-lsn")
check "$code" "400" "非法 LSN 参数 400"

# ------------------------------------------------------------- 5. 强杀应用后续接：无重复、无丢失、槽位不超前落盘
echo "=== 5. 强杀重启一致性 ==="
before_txns=$(api /api/progress | jq .captured_transactions)
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"7.7.7\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
after_txns=$(api /api/progress | jq .captured_transactions)
[ "$after_txns" -gt "$before_txns" ] && ok "新事务已捕获 ($before_txns -> $after_txns)" || bad "新事务未捕获"
kill -9 "$(app_pid)" 2>/dev/null || true; sleep 2
start_app "$SQLDIR"
wait_phase STREAMING 15 || true
sleep 3
check "$(api /api/progress | jq .captured_transactions)" "$after_txns" "强杀重启后事务数不重复"
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="7.7.7")]|length')" "1" "强杀前最后一次更新仍在"
flush=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select confirmed_flush_lsn from pg_replication_slots where slot_name='artifacts_history_slot'")
last=$(api /api/progress | jq -r .last_commit_lsn)
if [ -n "$flush" ] && [ -n "$last" ]; then
  fnum=$(python3 -c "h,l='$flush'.split('/');print((int(h,16)<<32)+int(l,16))")
  lnum=$(python3 -c "h,l='$last'.split('/');print((int(h,16)<<32)+int(l,16))")
  [ "$fnum" -le "$lnum" ] && ok "槽 confirmed_flush_lsn($flush) 不超前已落盘提交点($last)" \
                            || bad "槽位超前落盘位置: $flush > $last"
fi
# 重启后再写一条，验证续接后的流仍然工作
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"8.8.8\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="8.8.8")]|length')" "1" "续接后新变更继续被捕获"

# ------------------------------------------------------------- 6. 源库断连恢复
echo "=== 6. 源库重启后自动重连续接 ==="
pg_ctl_q restart -w >/dev/null
sleep 1
for _ in $(seq 1 20); do
  api /api/progress | jq -e '.phase=="STREAMING"' >/dev/null && break
  sleep 1
done
check "$(api /api/progress | jq -r .phase)" "STREAMING" "源库重启后应用自动恢复 STREAMING"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"9.9.9\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="9.9.9")]|length')" "1" "断连恢复后的变更未丢失"

# ------------------------------------------------------------- 8. 基线固化：清理边界与目录准确性
echo "=== 8. 基线固化（历史清理）边界验证 ==="
# 先制造一批可清理的逐笔事务，并记录库文件大小
for i in 4001 4002 4003; do
  curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
   \"operations\":[{\"op\":\"insert\",\"artifact_no\":$i,\"service_name\":\"s$i\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
done
sleep 3
# 取倒数第二个提交点作为保留起点（最后一个事务必须保留下来）
mapfile -t all_points < <(api /api/points | jq -r '.points[].lsn')
freeze_lsn="${all_points[-2]}"
last_lsn="${all_points[-1]}"
[ -n "$freeze_lsn" ] && ok "选定保留起点: $freeze_lsn（最后点 $last_lsn 保留）" || bad "无法选定固化起点"
# 记录固化前的已捕获事务数/行数与文件页数
prog_before=$(api /api/progress)
txns_before=$(echo "$prog_before" | jq .captured_transactions)
rows_before=$(echo "$prog_before" | jq .history_rows)
pages_before=$(echo "$prog_before" | jq .db_pages)
# 起点之前的一个旧点（用于验证 410）
old_lsn="${all_points[0]}"
# 源目录在固化点的真值（固化前留存，固化后逐字节比对）
dir_at_freeze=$(api "/api/directory?point=$freeze_lsn" | jq -c '[.artifacts[].artifact_no]|sort')
# 并发交错：固化请求与源库写入同时发出（HTTP 处理与捕获线程并发，
# 存储层同一把锁串行化，验证不会出现半份基线或重复变更）
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":5001,\"service_name\":\"interleave\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null &
writer_pid=$!
curl -s -X POST "$BASE/api/retention/freeze" -H 'Content-Type: application/json' \
  -d "{\"point\":\"$freeze_lsn\"}" > /tmp/freeze-resp.json &
freeze_pid=$!
wait $writer_pid
wait $freeze_pid
freeze_resp=$(cat /tmp/freeze-resp.json)
echo "$freeze_resp" | jq -e .event_id >/dev/null \
  && ok "固化接口原子返回成功: $(echo "$freeze_resp" | jq -c '{removed_txns,removed_history_rows,new_baseline_rows,db_pages_before,db_pages_after}')" \
  || bad "固化接口返回异常: $freeze_resp"
sleep 3
# 旧提交点明确不可查（410），而不是静默返回错误目录
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=$old_lsn")
check "$code" "410" "已清理的旧提交点 $old_lsn 返回 410 明确不可查"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=0/DEADBEEF")
check "$code" "404" "从未捕获的 LSN 仍返回 404"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/retention/freeze" \
  -H 'Content-Type: application/json' -d "{\"point\":\"$old_lsn\"}")
check "$code" "410" "对已清理旧点再次固化返回 410"
# 起点及之后的目录仍然准确：与固化前同点结果逐字节一致
dir_after=$(api "/api/directory?point=$freeze_lsn" | jq -c '[.artifacts[].artifact_no]|sort')
check "$dir_after" "$dir_at_freeze" "固化点目录固化前后完全一致"
# 起点目录里的行全部来自新基线
kinds=$(api "/api/directory?point=$freeze_lsn" | jq -r '[.artifacts[].change_type]|unique|join(",")')
check "$kinds" "BASELINE" "新基线目录行来源全部为 BASELINE"
# 保留下来的最后事务点目录可查且内容正确
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=$last_lsn")
check "$code" "200" "保留起点之后的提交点 $last_lsn 仍可查"
# 提交点列表：首点已是新基线，旧点全部消失
points_after=$(api /api/points | jq -r '.points[].lsn')
echo "$points_after" | grep -q "^$freeze_lsn$" && ok "保留起点作为新基线出现在提交点表" || bad "新基线点缺失"
if echo "$points_after" | grep -q "^$old_lsn$"; then bad "已清理旧点仍出现在提交点表"; else ok "旧提交点已从列表移除"; fi
echo "$points_after" | head -1 | grep -q "^$freeze_lsn$" && ok "提交点表首点即新基线" || bad "首点不是新基线"
# 单编号历史自新基线起准确：新插入的编号只有基线/之后的链，旧编号旧链已清
h4001=$(api /api/history/4001 | jq -r '[.history[].change_type]|join(",")')
case "$h4001" in
  BASELINE*) ok "保留编号 4001 历史自新基线起: $h4001" ;;
  *) bad "4001 历史链异常: $h4001" ;;
esac
# 计数与空间回收
prog_after=$(api /api/progress)
txns_after=$(echo "$prog_after" | jq .captured_transactions)
rows_after=$(echo "$prog_after" | jq .history_rows)
pages_after=$(echo "$prog_after" | jq .db_pages)
[ "$txns_after" -lt "$txns_before" ] && ok "已捕获事务数下降（清理生效）: $txns_before -> $txns_after" || bad "事务数未下降"
[ "$rows_after" -lt "$rows_before" ] && ok "历史行数下降: $rows_before -> $rows_after" || bad "历史行数未下降"
[ "$pages_after" -le "$pages_before" ] && ok "VACUUM 后 SQLite 页数未增长: $pages_before -> $pages_after" || bad "页数异常增长"
# 交错写入与后续写入继续被捕获，槽位不确认未落盘变更
for _ in $(seq 1 20); do
  n=$(api /api/history/5001 | jq '.history|length')
  [ "$n" -ge 1 ] && break
  sleep 1
done
check "$(api /api/history/5001 | jq '.history|length')" "1" "固化交错期间的写入仍被捕获，无丢失"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"10.0.1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 3
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="10.0.1")]|length')" "1" "固化后普通 SQL 修改继续捕获"
flush=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select confirmed_flush_lsn from pg_replication_slots where slot_name='artifacts_history_slot'")
last=$(api /api/progress | jq -r .last_commit_lsn)
fnum=$(python3 -c "h,l='$flush'.split('/');print((int(h,16)<<32)+int(l,16))")
lnum=$(python3 -c "h,l='$last'.split('/');print((int(h,16)<<32)+int(l,16))")
[ "$fnum" -le "$lnum" ] && ok "固化后槽 confirmed_flush_lsn($flush) 仍不超前落盘点($last)" \
  || bad "固化后槽位超前: $flush > $last"
# 审计记录可查
check "$(api /api/retention/events | jq '.events|length')" "1" "固化审计事件已记录"

# ------------------------------------------------- 9. 固化中途强杀：无半份基线、无重现
echo "=== 9. 固化中强杀重启的原子性 ==="
# 再攒两个事务，取其中点为固化目标
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":6001,\"service_name\":\"crash\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
mapfile -t pts2 < <(api /api/points | jq -r '.points[].lsn')
crash_freeze_lsn="${pts2[-2]}"
snap_before_crash=$(api "/api/directory?point=$crash_freeze_lsn" | jq -c '[.artifacts[].artifact_no]|sort')
events_before=$(api /api/retention/events | jq '.events|length')
txns_before_crash=$(api /api/progress | jq .captured_transactions)
rows_before_crash=$(api /api/progress | jq .history_rows)
# 显式开启崩溃钩子后重启应用（生产默认关闭，此处仅测试用）
TEST_FREEZE_CRASH_HOOK=1 start_app "$SQLDIR"
# 钩子：服务端在固化事务 COMMIT 前 os._exit(88) 强杀
code=$(curl -s -o /tmp/crash.json -w '%{http_code}' -X POST "$BASE/api/retention/freeze" \
  -H 'Content-Type: application/json' \
  -d "{\"point\":\"$crash_freeze_lsn\",\"crash_before_commit\":true}" --max-time 10)
[ "$code" != "200" ] && ok "强杀钩子中断了固化请求（http=$code）" || bad "强杀钩子未生效"
sleep 2
if app_pid | grep -q .; then bad "应用未按钩子退出"; else ok "应用在固化 COMMIT 前已退出"; fi
# 重启（钩子恢复默认关闭）：库必须保持固化前状态（无半份基线、无重复、审计不新增）
TEST_FREEZE_CRASH_HOOK=0 start_app "$SQLDIR"
wait_phase STREAMING 15 || bad "强杀后重启未恢复 STREAMING"
sleep 2
check "$(api /api/retention/events | jq '.events|length')" "$events_before" "强杀后无新增固化事件（事务整体回滚）"
check "$(api /api/progress | jq .captured_transactions)" "$txns_before_crash" "强杀重启后事务数与固化前一致，无半份清理"
check "$(api /api/progress | jq .history_rows)" "$rows_before_crash" "强杀重启后历史行数与固化前一致"
# 目标点仍是普通可查点且目录不变
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=$crash_freeze_lsn")
check "$code" "200" "回滚后目标提交点仍正常可查"
snap_after_crash=$(api "/api/directory?point=$crash_freeze_lsn" | jq -c '[.artifacts[].artifact_no]|sort')
check "$snap_after_crash" "$snap_before_crash" "回滚后目录与强杀前一致"
# 现在重新（不带钩子）固化同一目标点，必须成功且结果正确
retry=$(curl -s -X POST "$BASE/api/retention/freeze" -H 'Content-Type: application/json' \
  -d "{\"point\":\"$crash_freeze_lsn\"}")
echo "$retry" | jq -e .event_id >/dev/null && ok "重启后重试固化成功" || bad "重试固化失败: $retry"
check "$(api /api/retention/events | jq '.events|length')" "$((events_before+1))" "重试固化恰好新增一条审计"
# 已清理事务不得重现：再写新事务触发复制流活动后，事务计数只增不减不回弹
base_txns=$(api /api/progress | jq .captured_transactions)
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":6002,\"service_name\":\"post\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 3
new_txns=$(api /api/progress | jq .captured_transactions)
[ "$new_txns" -eq "$((base_txns+1))" ] && ok "固化后重放不导致已清理事务重现（$base_txns -> $new_txns）" \
  || bad "事务计数异常: $base_txns -> $new_txns"
check "$(api /api/history/6002 | jq '.history|length')" "1" "强杀恢复后新变更继续捕获"

# ---------------------------------------------- 9b. 重启重放：水位拦截 + 槽位安全
echo "=== 9b. 重启后复制流重放已清理事务的防护 ==="
horizon_lsn=$(api /api/progress | jq -r .retention_horizon_lsn)
txns_prev=$(api /api/progress | jq .captured_transactions)
force_kill_app
start_app "$SQLDIR"
wait_phase STREAMING 15 || bad "重放测试重启未恢复 STREAMING"
sleep 4
# 槽位仍安全：confirmed_flush_lsn 不超前最新落盘点
flush2=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select confirmed_flush_lsn from pg_replication_slots where slot_name='artifacts_history_slot'")
last2=$(api /api/progress | jq -r .last_commit_lsn)
f2=$(python3 -c "h,l='$flush2'.split('/');print((int(h,16)<<32)+int(l,16))")
l2=$(python3 -c "h,l='$last2'.split('/');print((int(h,16)<<32)+int(l,16))")
[ "$f2" -le "$l2" ] && ok "重放后槽 confirmed_flush_lsn($flush2) 不超前落盘点($last2)" \
  || bad "重放后槽位超前: $flush2 > $last2"
# 已清理事务未重现：水位不变、事务数不回弹
check "$(api /api/progress | jq -r .retention_horizon_lsn)" "$horizon_lsn" "重启后保留水位不变"
check "$(api /api/progress | jq .captured_transactions)" "$txns_prev" "重放后已清理事务未重现"
# 重放后新事务仍正常捕获（复制流未被水位拦截卡死）
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":7001,\"service_name\":\"replay\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 3
check "$(api /api/history/7001 | jq '.history|length')" "1" "重放防护后复制流仍正常推进"

# --------------------------------- 9c. 真实 walsender 重放已清理事务的纵深防护
echo "=== 9c. 真实复制流重放已固化事务的拦截（独立小型环境）==="
# 自包含：脚本自建一套独立 PG（55444）+ 独立 SQLite，独立槽，
# 不与主流程的 55433/8011 冲突，结束自动清理。
RG_LOG=/tmp/replay-guard-runs.log
if PGPORT=55444 APPPORT=8022 \
   PGDATA=/tmp/replay-guard-pgdata PGSOCK=/tmp/replay-guard-pgsock \
   SQLDIR=/tmp/replay-guard-sqlite \
   PYTHONPATH="$REPO" python3 tests/replay_guard_check.py > "$RG_LOG" 2>&1; then
  ok "真实 walsender 重放：水位及更早事务全部拦截、状态零变化、流不卡死"
  grep -E "PASS:" "$RG_LOG" | sed 's/^/    /'
else
  bad "真实 walsender 重放防护失败"; sed 's/^/    /' "$RG_LOG"
fi

# ------------------------------------------------------------- 10. 持久化校验
echo "=== 10. 两侧持久化 ==="
[ -f "$SQLDIR/history.db" ] && ok "SQLite 历史库落盘: $SQLDIR/history.db" || bad "SQLite 未落盘"
[ -d "$PGDATA/pg_wal" ] && ok "PostgreSQL 数据目录持久化: $PGDATA" || bad "PG 数据目录缺失"

echo
echo "================ 自测结果: $PASS passed / $FAIL failed ================"
[ "$FAIL" -eq 0 ]
