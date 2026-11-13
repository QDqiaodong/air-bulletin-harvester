# Air Bulletin Harvester

将虚构环境监测站的 HTML 公报采集为可追溯的结构化观测数据，供后续环境数据整理使用。
基线实现真实 HTTP 获取、本地文件获取、表格解析、浓度单位与时区归一、业务键去重和 SQLite
事务保存。它不提供健康判断，不连接真实监测机构，不模拟线上历史。

## 安装

Python 3.11+，macOS/Linux 命令相同。每仓独立虚拟环境：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install -r requirements.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
```

运行依赖全部来自标准库；构建工具固定版本。首次安装构建工具需要联网，安装后演示和测试离线。
未使用全局依赖或其他项目代码。

## 演示与测试

在仓库根目录运行。`run/` 初始不存在即为干净状态；需另一次干净演示时更换 `--db` 路径。

```sh
.venv/bin/python -m air_harvest.demo --db run/air.sqlite
.venv/bin/python -m air_harvest.demo --db run/air.sqlite
.venv/bin/python -m air_harvest --input data/bulletin.html --db run/file.sqlite
.venv/bin/python -m air_harvest.conditional_demo --dir run/conditional-demo
.venv/bin/python -m unittest discover -s tests -v
```

demo 在 loopback 临时端口提供自制 HTML，执行真实 GET 后关闭服务。无需额外服务、账号或外网。
4 行输入归为 3 条观测，1 行是同一时刻的等价单位重复。首次 `inserted=3, duplicates=1`，
重放 `inserted=0, duplicates=4, total=3`。SQLite 中 River & Park 的 PM2.5 为 25 ug/m3，
Hill 的 NO2 为 NULL（ND），PM10 为 0。CLI 错误以非零状态退出，不伪报成功。

```sh
.venv/bin/python -c 'import sqlite3; print(sqlite3.connect("run/air.sqlite").execute("SELECT station,pollutant,value_ug_m3,qualifier FROM observations ORDER BY station,pollutant").fetchall())'
```

可用 `--url http://127.0.0.1:PORT/bulletin` 连接自管本地服务。外部站点、重定向和携带认证信息的
URL 不受支持；不存在已获核实使用条件的外部数据接口。数据来源见 `data/PROVENANCE.md`。

## 日覆盖报告

采集库构建完成后，可用独立的只读入口按计划生成每日覆盖与描述性均值报告，不改变任何采集事实：

```sh
.venv/bin/python -m air_harvest.daily \
  --db run/air.sqlite \
  --plan data/daily-plan.example.json \
  --start-date 2026-01-01 --end-date 2026-01-31 \
  --output run/daily-report.json
```

日期为 `YYYY-MM-DD` 且范围两端包含。计划是 UTF-8 JSON，校验失败、日期非法、范围倒置、
时区不可用或数据库缺失均以非零状态退出且不创建/覆盖任何文件；输出采用同目录临时文件加
原子替换，旧报告在任何失败下保持完整，修正后可直接重跑。统计口径（UTC 小时槽、本地日归属、
numeric/nd_only/missing 三态、小时等权日均、整数覆盖率判定、精确有理数与 ROUND_HALF_UP 六位
展示、单快照只读、来源追溯与字节稳定性）完整定义在 `docs/daily-report.md`。

时区通过标准库 `zoneinfo` 使用系统 IANA 时区数据库；精简容器需安装 `tzdata` 一类系统包，
计划中的时区名无法加载时按配置项定位报错，不静默回退 UTC。该能力只使用 Python 标准库，
不引入数据库服务或外部接口，仍是合成演示数据，不输出健康或监管判断。

## 多采集库归并数仓

不同操作员各自采集同一批合成公报后，可用独立的离线归并入口把多个 SQLite 采集库合并为一个
自包含、可直接用 SQL 查询的归并库。归并不改动任何输入库，不调用建表/升级逻辑，不隐式建库，
也从不按文件顺序、最大值或平均值自动裁决：同一业务键出现不同候选值时必须由 manifest 给出
显式裁决，且裁决的 `expected` 必须完整等于快照中的全部见证，输入库一旦变化旧裁决即失效。

```sh
# 经真实 HTML 解析与 ingest 生成示例输入库与 manifest（合成数据）。
.venv/bin/python -m air_harvest.merge_demo --dir run/merge-example
# 清单模板另见 data/merge-manifest.example.json。
.venv/bin/python -m air_harvest.merge \
  --manifest run/merge-example/manifest.json \
  --output run/merge-example/warehouse.sqlite
```

每个输入库在一个只读 URI 连接中固定同一读取事务，全部 sources 与 observations 来自该库的同一
快照；库间没有共同事务。归并业务键仍是 station + 完整 UTC instant + pollutant，微秒精度保留，
同一时刻的不同偏移表示归为一键；浓度以 `decimal.Decimal` 精确比较（25 与 25.0 相同，ND 的
NULL 与真实测量值 0 不同），全程不经过 float。输出库保存 manifest 原始字节与 SHA256、每库规范
逻辑快照（含全部字段、稳定排序、独立 SHA256，而非库文件二进制哈希）、逐 shard 来源、规范候选
值、完整观测见证、唯一最终事实，以及冲突、裁决理由、完整预期集合与落选候选；移走 manifest 和
全部输入库后仍可从输出重建各输入快照并重新解释全部事实。写入在输出同目录的私有临时库中完成
校验、外键与完整性检查后原子替换，任何失败均非零退出、旧输出与输入保持完整、临时文件被清理。
数据模型、查询示例、失败重跑与全部校验规则见 `docs/merge-warehouse.md`。

## 条件采集缓存（fetch / status / export）

在既有采集库之上提供可选的增量条件采集，不改变旧 `ingest`、`--input/--url`、日报与归并路径：

```sh
.venv/bin/python -m air_harvest fetch --url http://127.0.0.1:PORT/bulletin --db run/cond.sqlite
.venv/bin/python -m air_harvest status --db run/cond.sqlite
.venv/bin/python -m air_harvest export --db run/cond.sqlite --output run/original.html \
  --url http://127.0.0.1:PORT/bulletin
.venv/bin/python -m air_harvest.conditional_demo --dir run/conditional-demo --clean
```

URL 原串只用于寻址本地回环 fixture，不作为标识、内容或路径；缓存键为规范化 URL 的
SHA256，仅接受 `http`、`127.0.0.1`/`localhost`/`::1`，拒绝凭据、片段、空白、控制字符、
非 2 MiB 响应、非 200/304、重定向、短读、长度不符与异常编码。验证器以 ETag 优先、保留弱
标签，缺失时清除；`304` 必须响应条件请求且缓存字节、来源、解析结果与观测全部一致，不入库、
不改代次。`200` 将原始字节作为 BLOB、来源保首次、观测按既有业务键/单位/ND/数值语义写入，
并在同一事务内将代次加 1；网络期间不持写锁，提交前以请求起点的代次与摘要做 CAS（含冷启动），
迟到响应报 `stale_response` 且不重试，冲突或提交前故障全部回滚，提交后回执失败不撤销提交。
旧库不重建原文，未知结构（多余/缺失表、错列、缺外键）直接拒绝。`status`/`export` 离线只读、
缺库不创建；导出使用同目录临时文件加原子替换，拒绝覆盖库文件、链接别名，失败保留旧文件。
完整契约、传输细节、事务边界与失败分类见 `docs/conditional-cache.md`。

## 结构与约束

- `air_harvest/source.py`：有界文件/HTTP 获取。
- `air_harvest/parser.py`：HTML 表格契约与语义归一。
- `air_harvest/store.py`：去重、冲突拒绝、事务及来源哈希。
- `air_harvest/daily.py`：只读日覆盖报告、计划校验、精确算术与原子写出。
- `air_harvest/merge.py`：多采集库只读快照、精确归并、显式裁决与自包含归并库写出。
- `air_harvest/merge_demo.py`：经真实解析与 ingest 生成合成归并示例输入库与 manifest。
- `air_harvest/demo.py`：完整离线 HTTP 链路。
- `air_harvest/conditional.py`：条件采集缓存、校验器、CAS 事务、只读状态与原子导出。
- `air_harvest/conditional_cli.py`：`fetch`/`status`/`export` 子命令。
- `air_harvest/conditional_demo.py`：本地回环的条件采集演示。
- `data/`：明确标注的合成 fixture（含日报告示例计划 `daily-plan.example.json`、
  归并清单模板 `merge-manifest.example.json` 与小时级公报 `daily-bulletin.html`）；
  `tests/`：语义与失败验证。
- `docs/contract.md`：字段、空值、单位、时间、存储与重复处理定义。
- `docs/daily-report.md`：日报告统计定义、JSON 结构、时区环境与失败恢复说明。
- `docs/merge-warehouse.md`：归并清单契约、快照规范、数仓模型、SQL 查询与重跑说明。
- `docs/conditional-cache.md`：条件采集 URL/传输/验证器契约、缓存模型、CAS 与失败恢复。

当前为单批次、单采集者基线；条件采集只提供每 URL 的代次与请求日志，没有自动调度或跨批次纠错。
运行库及环境均忽略。
MIT 许可覆盖本项目代码与自制样例；未复制第三方代码或数据。
