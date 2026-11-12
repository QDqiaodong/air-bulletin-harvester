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

## 结构与约束

- `air_harvest/source.py`：有界文件/HTTP 获取。
- `air_harvest/parser.py`：HTML 表格契约与语义归一。
- `air_harvest/store.py`：去重、冲突拒绝、事务及来源哈希。
- `air_harvest/demo.py`：完整离线 HTTP 链路。
- `data/`：明确标注的合成 fixture；`tests/`：语义与失败验证。
- `docs/contract.md`：字段、空值、单位、时间、存储与重复处理定义。

当前为单批次、单采集者基线；没有自动调度、增量游标或跨批次纠错。运行库及环境均忽略。
MIT 许可覆盖本项目代码与自制样例；未复制第三方代码或数据。
