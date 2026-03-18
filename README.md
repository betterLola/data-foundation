# data-foundation

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![MySQL](https://img.shields.io/badge/MySQL-5.7%2B-orange?logo=mysql)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Production--Ready-brightgreen)

> 基于公司内网及友盟 Open API 的数据爬取、清洗与入库自动化工作流，为后续**自动化报告生成**和**智能问数系统（NL2SQL）**提供数据基础层支撑。

---

## 目录

- [项目背景](#项目背景)
- [整体架构](#整体架构)
- [功能特性](#功能特性)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [脚本说明](#脚本说明)
- [数据库结构](#数据库结构)
- [定时任务](#定时任务)
- [Roadmap](#roadmap)
- [依赖列表](#依赖列表)

---

## 项目背景

本项目服务于政务/企业运营数据的自动化采集需求，解决以下问题：

- **友盟数据分散**：多端（安卓/苹果/鸿蒙/小程序）日活、留存、事件数据无法自动汇总。
- **内网系统无 API**：注册用户数、实名用户数等核心指标仅能通过 Web 界面获取。
- **报告依赖人工**：每日运营日报需人工从多系统汇总数据后填写。
- **数据质量参差**：多脚本写入同一张表，存在字段互覆盖风险。

**业务目标**：一键自动完成从数据采集 → 清洗 → 入库的全流程，使下游自动化报告和 NL2SQL 智能问答系统能稳定读取到干净的运营数据。

---

## 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                     main.py（总调度）                    │
└────────────────────┬────────────────────────────────────┘
                     │ 按序调用
     ┌───────────────┼───────────────┐
     ▼               ▼               ▼
┌──────────┐  ┌──────────────┐  ┌───────────────────┐
│UmengAPI  │  │internal_     │  │smart_frontend_    │
│.py       │  │network_      │  │dau_spider.py      │
│(友盟API) │  │spider.py     │  │(智能前端爬虫)      │
└──────────┘  │(内网爬虫)    │  └───────────────────┘
     │        └──────────────┘          │
     │               │                  │
     ▼               ▼                  ▼
┌──────────────────────────────────────────────────────┐
│            MySQL · daily 数据库                       │
│  platform_daily_metrics  │  5100_detail              │
│  app_retention           │  resource_total           │
└──────────────────────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
  自动化日报生成           NL2SQL 智能问答
  (generate_daily_report)  (streamlit_sql2nlp)
```

**数据写入职责分离，字段互不覆盖：**

| 脚本                             | 负责写入字段                                   |
| ------------------------------ | ---------------------------------------- |
| `UmengAPI.py`                  | `android_dau`, `ios_dau`, `harmonyos_dau`, `app_dau`, `mini_program_dau`, `alipay_dau` |
| `internal_network_spider.py`   | `new_register_users`, `new_realname_users` |
| `smart_frontend_dau_spider.py` | `smart_frontend_dau`                     |
| `5100_detail.py`               | `5100_detail` 表（事件明细）                    |
| `fetch_retention.py`           | `app_retention` 表（次日留存率）                 |
| `resource_total.py`            | `resource_total` 表（关键资源位点击）              |

---

## 功能特性

- **多源数据聚合**：同时覆盖友盟 Open API（原生APP + 小程序）、内网系统爬虫、智能前端系统爬虫三条数据通路。
- **字段级安全写入**：所有入库操作均采用 `INSERT ... ON DUPLICATE KEY UPDATE` + 显式字段列表，严防字段互覆盖。
- **业务汇总自动计算**：全平台总日活 `platform_dau`、累计注册/实名用户滚动累加均在 `main.py` 统一结算。
- **全库自动去重**：每次主流程结束后，自动扫描并清理所有表中完全重复的行。
- **爬虫反检测**：隐藏 `navigator.webdriver` 特征、模拟真人随机交互轨迹、Chrome 专用 Profile 隔离。
- **弹窗自愈**：自动处理浏览器原生 Alert 及 ElementUI 模态框，免人工干预。
- **历史补录**：提供 `import_history_appdau.py`（从 CSV 补录离线数据）和 `import_platform_mau.py`（导入月活 Excel）。

---

## 快速开始

### 环境要求

- Python 3.11+
- MySQL 5.7+ / 8.0+
- Google Chrome（爬虫模块需要）

### 1. 克隆项目

```bash
git clone https://github.com/your-org/data-foundation.git
cd data-foundation
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

或手动安装：

```bash
pip install pymysql DrissionPage pandas openpyxl
```

### 3. 配置敏感信息

**推荐方式**：复制配置模板并填写真实值（`config.py` 已在 `.gitignore` 中被排除，不会上传到 Git）：

```bash
cp config.example.py config.py
# 编辑 config.py，填入真实的数据库密码、API Key、AppKey 等
```

**备用方式**：每个脚本顶部均有独立的内联配置区，按注释提示填写：

```python
# UmengAPI.py / 5100_detail.py / fetch_retention.py 等 API 脚本
API_KEY      = "更换为友盟API_KEY"       # 友盟开放平台 → 账号设置 → API Key
API_SECURITY = "更换为友盟API_SECRET"    # 友盟开放平台 → 账号设置 → API Secret

PLATFORM_APPKEYS = {
    "安卓": "更换为安卓端AppKey",         # 友盟控制台 → 对应应用 → AppKey
    "苹果": "更换为苹果端AppKey",
    "鸿蒙": "更换为鸿蒙端AppKey",
}

DB_CONFIG = {
    'host':     'localhost',
    'port':     3306,
    'user':     'root',
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset':  'utf8mb4',
}

# internal_network_spider.py
TARGET_URL     = "更换为内网系统登录地址"
LOGIN_USERNAME = "更换为内网系统账号"
LOGIN_PASSWORD = "更换为内网系统密码"

# smart_frontend_dau_spider.py
LOGIN_URL = '更换为智能前端系统登录地址'
USERNAME  = '更换为系统账号'
PASSWORD  = '更换为系统密码'
```

### 4. 初始化数据库

连接 MySQL 后，手动创建数据库：

```sql
CREATE DATABASE IF NOT EXISTS `daily` DEFAULT CHARACTER SET utf8mb4;
```

> `fetch_retention.py` 会自动创建 `app_retention` 表；`5100_detail.py` 会自动创建 `5100_detail` 表。
> `platform_daily_metrics` 表结构见下方[数据库结构](#数据库结构)章节。

### 5. 运行全流程

```bash
python main.py
```

或按模块单独运行（适合调试/补数场景）：

```bash
python UmengAPI.py                    # 友盟多端日活
python internal_network_spider.py     # 内网新增用户数
python smart_frontend_dau_spider.py   # 智能前端日活
python fetch_retention.py             # 次日留存率
python resource_total.py              # 资源位点击量
python 5100_detail.py                 # 事项细分服务次数
```

---

## 配置说明

### 友盟 Open API 配置

前往 [友盟开放平台](https://developer.umeng.com/) 获取：

| 配置项            | 说明         | 位置                           |
| -------------- | ---------- | ---------------------------- |
| `API_KEY`      | 开放平台账号唯一标识 | 账号设置 → API Key               |
| `API_SECURITY` | 接口签名密钥     | 账号设置 → API Secret            |
| `AppKey`       | 单个应用标识     | 应用列表 → 对应 App → AppKey       |
| `DataSourceId` | 小程序数据源标识   | 小程序列表 → 对应小程序 → DataSourceId |

### 数据库配置

所有脚本共用同一套 MySQL 配置（`DB_CONFIG` 字典），修改一处后其余脚本同步调整：

```python
DB_CONFIG = {
    'host':     'localhost',           # 数据库主机地址
    'port':     3306,                  # 端口，默认 3306
    'user':     'root',
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset':  'utf8mb4',
}
```

### 爬虫环境配置

爬虫脚本使用 [DrissionPage](https://g1879.gitee.io/drissionpagedocs/) 接管本地 Chrome，无需 WebDriver：

```python
CHROME_PORT    = 9335   # Chrome 远程调试端口（可自定义，避免冲突）
CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\smart_spider_chrome'
DOWNLOAD_DIR   = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau'
```

---

## 脚本说明

| 脚本                             | 类型   | 调用方式         | 核心职责                                     |
| ------------------------------ | ---- | ------------ | ---------------------------------------- |
| `main.py`                      | 总调度  | 手动 / 定时任务    | 按序调用所有采集模块，完成业务汇总与全库去重                   |
| `UmengAPI.py`                  | API  | `main.py` 调用 | 获取原生APP（安卓/苹果/鸿蒙）及小程序日活，写入 `platform_daily_metrics` |
| `5100_detail.py`               | API  | `main.py` 调用 | 获取指定事件各子服务的点击分布，写入 `5100_detail`         |
| `internal_network_spider.py`   | 爬虫   | `main.py` 调用 | 爬取内网平台新增注册/实名用户数                         |
| `smart_frontend_dau_spider.py` | 爬虫   | `main.py` 调用 | 登录智能前端系统，导出 UV 数据，解析 Excel 后写入           |
| `fetch_retention.py`           | API  | `main.py` 调用 | 拉取三端次日留存率（最近 14 天），写入 `app_retention`    |
| `resource_total.py`            | API  | `main.py` 调用 | 获取关键资源位（Banner/新闻等）昨日点击量                 |
| `resource_total_history.py`    | API  | 手动           | 历史数据批量回填（按日期范围拉取）                        |
| `search_detail_import.py`      | API  | 手动           | 批量拉取搜索词明细并入库 `search_detail` 表           |
| `import_history_appdau.py`     | 工具   | 手动           | 从 CSV 文件批量导入历史事件明细数据                     |
| `import_platform_mau.py`       | 工具   | 手动           | 从 Excel 导入历史月活数据到 `platform_mau` 表       |

---

## 数据库结构

### `platform_daily_metrics`（核心日指标表）

```sql
CREATE TABLE `platform_daily_metrics` (
    `stat_date`             DATE NOT NULL UNIQUE COMMENT '统计日期（唯一键）',

    -- 友盟 API 写入字段 (UmengAPI.py)
    `android_dau`           INT UNSIGNED    COMMENT '安卓端日活跃用户数',
    `ios_dau`               INT UNSIGNED    COMMENT '苹果端日活跃用户数',
    `harmonyos_dau`         INT UNSIGNED    COMMENT '鸿蒙端日活跃用户数',
    `app_dau`               INT UNSIGNED    COMMENT 'APP总日活 (三端之和)',
    `mini_program_dau`      INT UNSIGNED    COMMENT '微信小程序日活',
    `alipay_dau`            INT UNSIGNED    COMMENT '支付宝小程序日活',

    -- 内网爬虫写入字段 (internal_network_spider.py)
    `new_register_users`    INT UNSIGNED    COMMENT '当日新增注册用户数',
    `new_realname_users`    INT UNSIGNED    COMMENT '当日新增实名用户数',

    -- 智能前端爬虫写入字段 (smart_frontend_dau_spider.py)
    `smart_frontend_dau`    INT UNSIGNED    COMMENT '智能前端平台日活',

    -- main.py 汇总计算字段
    `platform_dau`          INT UNSIGNED    COMMENT '全平台总日活（各端汇总）',
    `total_service_times`   BIGINT UNSIGNED COMMENT '累计服务总次数（滚动累加）',
    `total_register_users`  BIGINT UNSIGNED COMMENT '累计注册用户数（滚动累加）',
    `total_realname_users`  BIGINT UNSIGNED COMMENT '累计实名用户数（滚动累加）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平台每日核心指标汇总表';
```

### `5100_detail`（事项服务明细表）

```sql
CREATE TABLE `5100_detail` (
    `id`              INT AUTO_INCREMENT PRIMARY KEY,
    `service_amount`  BIGINT          COMMENT '服务使用次数',
    `resource_name`   VARCHAR(255)    COMMENT '事件名称',
    `service_name`    VARCHAR(255)    COMMENT '子服务名称',
    `stat_date`       DATE            COMMENT '统计日期',
    `port`            VARCHAR(50)     COMMENT '端口（安卓/苹果/鸿蒙）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### `app_retention`（次日留存率表）

```sql
CREATE TABLE `app_retention` (
    `id`               INT AUTO_INCREMENT PRIMARY KEY,
    `platform`         VARCHAR(20)     COMMENT '平台名称 (ios/android/harmony)',
    `stat_date`        DATE            COMMENT '统计日期',
    `day_1_retention`  DECIMAL(5,2)    COMMENT '次日留存率 (%)',
    `created_at`       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_platform_date` (`platform`, `stat_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### `resource_total`（资源位点击表）

```sql
CREATE TABLE `resource_total` (
    `resource_amount`  BIGINT       COMMENT '点击/事件次数',
    `resource_name`    VARCHAR(255) COMMENT '事件名称（英文 key）',
    `stat_date`        DATE         COMMENT '统计日期',
    `port`             VARCHAR(50)  COMMENT '端口（安卓/苹果/鸿蒙）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## 定时任务

推荐使用 **Windows 任务计划程序** 每天定时执行 `main.py`：

1. 打开"任务计划程序"（`Win+R` → `taskschd.msc`）
2. 新建基本任务
3. **触发器**：每天 `09:00`（建议友盟数据在凌晨统计完毕后运行）
4. **操作**：启动程序
   - 程序：`python.exe`（或虚拟环境路径）
   - 参数：`main.py`
   - 起始于：`项目根目录路径`
5. 高级设置：勾选"使用最高权限运行"

Linux / macOS 环境使用 cron：

```bash
# 每天 09:00 运行（先激活虚拟环境）
0 9 * * * cd /path/to/data-foundation && /path/to/venv/bin/python main.py >> logs/main_$(date +\%Y\%m\%d).log 2>&1
```

---

## Roadmap

- [x] 友盟多端日活自动采集（API）
- [x] 内网平台注册/实名数据爬取
- [x] 智能前端 UV 数据爬取（Excel 导出解析）
- [x] 次日留存率自动拉取
- [x] 关键资源位点击量统计
- [x] 全平台总日活 / 累计用户数自动汇总
- [x] 全库自动去重清洗
- [x] `main.py` 一键全流程调度
- [x] 结构化日志写入本地文件（`logs/` 目录）
- [x] 数据质量监控（空值检测 + 日活异常跌幅告警）
- [x] 钉钉 / 邮件告警钩子（可选，配置 webhook 即启用）
- [ ] 支持 `.env` 统一管理配置（已有 `config.example.py`，规划中）
- [ ] 数据血缘追踪（字段级来源记录）
- [ ] Docker 容器化部署

---

## 依赖列表

```text
pymysql>=1.1.0       # MySQL 连接驱动
DrissionPage>=4.0.0  # 基于 Chromium 的浏览器自动化框架（替代 Selenium）
pandas>=2.0.0        # Excel / CSV 数据解析
openpyxl>=3.1.0      # Excel 文件读写支持
requests>=2.31.0     # HTTP 请求（钉钉告警等）
```

安装：

```bash
pip install pymysql DrissionPage pandas openpyxl
```

> 友盟 SDK（`aop/` 目录）已随项目内置，无需额外安装。

---

## 常见问题

| 现象                                      | 原因                 | 解决方案                            |
| --------------------------------------- | ------------------ | ------------------------------- |
| `Can't connect to MySQL server`         | MySQL 未启动或密码错误     | 检查 MySQL 服务状态，确认 `DB_CONFIG` 配置 |
| `error_code=40001: Please check appkey` | AppKey 无权限或配置错误    | 登录友盟控制台核查对应 AppKey              |
| `ModuleNotFoundError: DrissionPage`     | 使用了错误的 Python 环境   | 激活正确的虚拟环境后运行                    |
| 爬虫下载超时                                  | 网络慢或确认弹窗未点击        | 查看 `debug/` 目录截图，适当延长 `timeout` |
| UV 数据为空（返回 0）                           | Chrome 原生网络权限弹窗未处理 | 首次运行可手动点击"允许"，后续自动              |
| 数据出现重复行                                 | 脚本重复运行             | `main.py` 末尾的去重逻辑会自动清理          |

---

*本项目用于公司内部数据基础设施建设，如有问题请提 Issue 或联系维护者。*


## 更新日志 (Changelog)

### [2026-03-18] 跨项目同步与核心逻辑增强

**同步与优化内容：**

- **`internal_network_spider.py` (数据提取逻辑重构)**：
  - 弃用动态列索引推算，改为锁定 `is-scrolling-none` 主表体容器。
  - 固定使用 CSS 类名提取数据：`.el-table_1_column_6` (新增实名), `.el-table_1_column_14` (新增注册)。
  - 极大提升了在固定列（Frozen Columns）场景下的抓取准确率。
- **`data_backfilling.py` (回填模块同步更新)**：
  - 同步修正了新增实名与新增注册的列映射规则，确保补数逻辑与主程序严格一致。
- **`main.py` (历史累计逻辑鲁棒性增强)**：
  - 优化了 `total_service_times`、`total_register_users` 和 `total_realname_users` 的滚动累加逻辑。
  - 现支持自动向前追溯最近一条有效历史记录进行加总，不再受限于“前一日必须存在数据”的严格限制，提高了对间断性缺失数据的容错能力。
- **安全性与脱敏**：
  - 严格剥离账号、密码、Token 及内网 URL 等敏感信息至 `config.py`，确保代码仓库合规脱敏。

**变更文件：** `internal_network_spider.py`, `data_backfilling.py`, `main.py`

### [2026-03-17]内网爬虫列定位重构（CSS class 名精准提取）

**修复后已实现：数据回填与主流程增强**

- **核心数据回填机制**：新增 data_backfilling.py 模块，自动检查近 7 天历史数据的缺失情况并执行重试回填，覆盖友盟 API（原生/小程序）、内网平台新增用户和智能前端日活等所有维度。
- **主流程编排优化**：重构 main.py 调度逻辑。将历史数据漏填检查前置为 Step 1，若识别到缺失自动触发补数，并将正常补数后的源剔除出常规采集任务以防止重复调用。
- **智能前端爬虫更新**：适配目标站点最新的路由及登录态校验规则，重写了 _smart_login 的模拟交互逻辑并完成 URL 信息的脱敏。
- **内网数据提取优化**：优化 InternalBackfillSpider 的表格提取算法，由下拉滚动改为基于 class 定位的静态表体逐行提取，提高了新版页面结构的适配与容错率。
- **全库数据去重清洗**：在采集主流程收尾阶段引入 deduplicate_all_tables() 机制，按日自动扫描并清除全库潜在的行级重复数据。
- **配置隔离与脱敏**：将所有敏感凭证（账号、密码、Token、Key 等）剥离至外部 config.py，确保代码仓库的安全合规。

**变更文件：** `internal_network_spider.py`、`data_backfilling.py`

原方案通过扫描表头文字（`th .cell`）动态推算列序号，再用 `xpath:./td[N]//div[@class="cell"]` 取值。
当页面存在固定列（frozen column）时，`colgroup` 中各列的视觉顺序与 `td` 的 DOM 顺序不一致，导致读到错误列数据或空值。

**修复方案**

定位 `<div class="el-table__body-wrapper is-scrolling-none">` 主表体区域（固定列在其他 wrapper 中，避免重影干扰），改用列 CSS class 名直接查找单元格：

| 字段     | CSS 选择器                       | 对应数据     |
| ------ | ----------------------------- | -------- |
| 新增注册用户 | `.el-table_1_column_6 .cell`  | 当日新增注册人数 |
| 新增实名用户 | `.el-table_1_column_14 .cell` | 当日新增实名人数 |

表格行规则：**第1行 = 昨日，第2行 = 前天，依此类推**。

**internal_network_spider.py — extract_data()**

- 旧：扫描表头文字确定列索引 → `xpath:./td[N]//div[@class="cell"]`
- 新：
  1. 定位 `.el-table__body-wrapper.is-scrolling-none`（兜底使用整页）
  2. 取第一行（昨日数据）
  3. 分别用 `css:.el-table_1_column_6 .cell` / `css:.el-table_1_column_14 .cell` 提取，各自独立 `try-except` 防止单列失败影响另一列

**data_backfilling.py — InternalBackfillSpider.extract_all_rows()**

- 旧：同上，扫描表头 + td 位置索引，无法区分日期
- 新：
  1. 定位 `.el-table__body-wrapper.is-scrolling-none`
  2. 遍历所有行，**行索引推算日期**（row 0 = 昨天，row 1 = 前天）
  3. 同时尝试读取 `.el-table_1_column_2 .cell` 中的日期文本进行校验，若日期列与位置推算不符则以日期列为准；日期列读取失败时回退到位置推算
  4. 用 `.el-table_1_column_6` / `.el-table_1_column_14` 取注册/实名值，单行提取失败只 warning 不中断

------

### [2026-03-16 15:30] 修复 main.py 任务调度逻辑冲突

**更新内容：**

- **解决重复执行 Bug**：修复了在 `main.py` 中，当 `Step 1` 回填程序处理了包含“昨天”在内的日期后，`Step 2` 仍会再次触发对应的常规采集脚本（如智能前端爬虫）的问题。
- **强制任务去重**：现在只要数据源在回填阶段被处理，就会从 `script_mapping` 中动态移除，确保每个采集脚本在一次主流程中最多只运行一次。
- **优化调度顺序**：明确了“回填覆盖常规”的优先级，避免了浏览器爬虫被反复打开导致的资源浪费。

### [2026-03-16 14:30] 数据采集与回填流程重大 Bug 修复及优化

**更新内容：**

1. **数据采集流程逻辑重构 (`main.py`)**：
   - **回填功能前置**：将 `data_backfilling` 的缺失检查提前至所有采集脚本执行之前。
   - **按需执行与去冗余**：系统现在会根据近 7 天的数据缺失情况，自动判断是调用 `data_backfilling` 的回填函数还是执行每日常规脚本。如果当日数据已通过回填逻辑获取，将自动跳过对应的每日采集脚本（如内网爬虫、友盟 API 脚本），彻底解决了**重复登陆内网**和**二次 API 下拉**的问题。
2. **数据一致性与连锁加总修复**：
   - **全链路推算**：修复了回填历史数据后导致后续日期累计指标（如 `total_register_users`, `total_service_times`）失效的问题。
   - **自动重算机制**：系统会自动识别回填的最早日期，并从该日期起，逐日向后重新计算并更新所有的汇总字段（DAU 加总、注册/实名累计、服务次数累计），确保数据库中各日期之间的逻辑关系绝对正确。
3. **内网回填爬虫列定位增强 (`data_backfilling.py`)**：
   - **精准列识别**：修正了原先“日期”、“注册”、“实名”字段匹配逻辑过于宽泛的 Bug。通过正则表达式排除“日活”等干扰字符，并精准匹配“新增注册”和“新增实名”列头。
   - **修复错位问题**：彻底解决了因内网报表页面列偏移导致的回填数据为空或读取到错误列数据的问题。

**技术细节：**

- **计算逻辑**：`platform_dau = 各端日活之和`；`当日累计 = 前日累计 + 当日新增`。
- **重算范围**：从 `min(缺失日期, 昨天)` 到 `昨天`。

------

### [2026-03-04] App次日留存数据自动拉取与入库

新增了 `fetch_retention.py` 脚本，用于自动化获取友盟平台 iOS、Android、HarmonyOS 三端的次日留存率数据并存入本地 MySQL 数据库。

**关于留存数据类型的说明：**

- **当前获取的类型：** 友盟开放平台 API (`umeng.uapp.getRetentions`) 默认且**仅支持获取「新增用户次日留存率」**（即：当日新增用户在次日继续使用的比例）。
- **关于活跃用户留存：** 经过对友盟 SDK（AOP）及接口参数（尝试传入 `type="active"` 等）的深入测试，确认友盟 Open API 不提供「活跃用户留存率」的直接查询接口。如需活跃留存数据，目前只能通过友盟 Web 控制台手动导出或使用付费的 U-DMP 明细数据服务。

**功能特性：**

- **多平台支持**：一键获取 苹果、安卓、鸿蒙 三个 AppKey 对应的留存数据。
- **自动初始化**：脚本会自动创建 `daily` 数据库及 `app_retention` 数据表。
- **增量更新**：采用 `ON DUPLICATE KEY UPDATE` 逻辑，支持重复执行，自动更新或补充缺失日期的数据。
- **数据范围**：默认拉取最近 14 天的新增次留数据（考虑数据统计延迟，排除最近 2 天）。

**数据库结构 (`daily.app_retention`)：**

| 字段名               | 类型      | 描述                         |
| :---------------- | :------ | :------------------------- |
| `platform`        | VARCHAR | 平台名称 (ios/android/harmony) |
| `stat_date`       | DATE    | 统计日期 (新增用户发生的日期)           |
| `day_1_retention` | DECIMAL | 次日留存率 (%)                  |

**使用方法：**

```bash
# 使用虚拟环境运行
C:\Users\TAOYUAN\PycharmProjects\pythonProject2\venv311\Scripts\python.exe fetch_retention.py
```

------

### [2026-03-03] 新增多端自定义事件统计脚本

**新增文件**

- **resource_total.py** - 关键资源位点击量统计（支持安卓、苹果、鸿蒙三端）
- **5100_detail.py** - 510100_items 事件子服务点击详情（支持安卓、苹果、鸿蒙三端）

**功能说明**

这两个脚本旨在通过友盟自定义事件 API 获取在不同终端（安卓、苹果、鸿蒙）上的用户行为数据，并自动化存储至数据库。

**resource_total.py (关键资源位)：**

- **统计对象**：`mid_banner`, `news_click`, `person_banner_click`, `top_banner_click`, `hometopic_click`
- **处理逻辑**：循环遍历三端 AppKey，调用 `UmengUappEventGetDataRequest` 获取昨日点击总数。
- **存储表**：`resource_total`
- **关键字段**：`resource_amount`, `resource_name`, `stat_date`, `port` (用于区分端)

***5100_detail.py (5100子服务详情)：****

- **统计对象**：针对 `510100_items` 事件的 `service_name` 参数值进行细化统计。
- **处理逻辑**：调用 `UmengUappEventParamGetValueListRequest`，获取昨日各子服务的点击分布，并对 URL 编码的中文名称进行自动解码（如 `%E7%A4%BE%E4%BF%9D` -> `社保`）。
- **存储表**：`5100_detail` (脚本运行会自动检测并创建该表)
- **关键字段**：`service_amount`, `resource_name`, `service_name`, `stat_date`, `port`

**协作机制**

- ✅ 脚本自动计算“昨日”日期，无需手动输入。
- ✅ 包含完整的错误处理与重连机制。
- ✅ 自动执行数据库表结构维护（如添加缺失的 `port` 字段）。

### [2026-02-25] smart_frontend_dau_spider.py 深度修复

**修复内容一览**

| 编号   | 问题                                       | 原因                                       | 修复方案                                     |
| ---- | ---------------------------------------- | ---------------------------------------- | ---------------------------------------- |
| 1    | 脚本杀掉用户自己的 Chrome 窗口                      | 旧代码 `taskkill /IM chrome.exe` 杀全部 Chrome | 改为 `kill_chrome_on_port(9335)`，只杀占用专用端口的进程 |
| 2    | 下载被中断，永远超时                               | `wait_for_download()` 在 `page.quit()` **之后**调用，Chrome 关闭后下载中断 | 将 `wait_for_download()` 移入 `try` 块，在 `finally: page.quit()` 之前执行 |
| 3    | 导出确认弹窗点不到，下载不触发                          | 弹窗（"是否导出当前表格Excel文件?"）在 **iframe 内**渲染；`text()="确定"` 精确匹配因按钮 span 含换行符而失败 | ① `_dismiss_popups` 改用 `normalize-space()` 容忍空白；② `export_excel` 先用 `page.get_frame('tag:iframe')` 显式在 iframe 内查找并点击"确定"，失败再回退到页面级搜索 |
| 4    | 下载文件落到 Chrome 默认目录，`wait_for_download` 找不到 | incognito 模式下 DrissionPage 内部 rename 异常，文件保存到 `C:\Users\TAOYUAN\Downloads\` 而非子目录 | 新增 `DOWNLOAD_ROOT` 常量，`wait_for_download` 同时搜索两个目录；找到后若在其他目录则自动 copy 到 `DOWNLOAD_DIR` |
| 5    | UV 数据始终"暂无数据"（空 Excel）                   | 旧代码 `page.run_js('window.localStorage.clear(); window.sessionStorage.clear()')` 清除了 iframe（frontend-front）的认证 token（同源共享 localStorage） | 移除 localStorage/sessionStorage 清除操作      |
| 6    | Chrome 弹「想要访问本地网络中的其他设备」原生弹窗             | 脚本首次使用新 Chrome Profile，`local_network` 权限未预授权 | 新增 `ensure_chrome_permissions()`，在 Chrome 启动前向 `CHROME_PROFILE/Default/Preferences` 写入 `local_network` + `loopback_network` 权限（setting=1），免手动点击 |
| 7    | Chrome 调试 Profile 目录为空，权限无法持久化           | 旧代码用 `set_argument('--user-data-dir=...')` 被 DrissionPage 内部的 `auto_port()` 覆盖 | 改用 `co.set_local_port(9335)` + `co.set_user_data_path(CHROME_PROFILE)`，二者互斥，不再冲突 |

**新增常量**

```python
DOWNLOAD_ROOT = r'C:\Users\TAOYUAN\Downloads'   # Chrome 默认下载目录（备用搜索路径）
CHROME_PORT    = 9335   # 专用调试端口，只清理此端口上的旧 Chrome
CHROME_PROFILE = r'C:\Users\TAOYUAN\AppData\Local\smart_spider_chrome'  # 专用 Profile
```

**新增函数**

| 函数                              | 作用                                       |
| ------------------------------- | ---------------------------------------- |
| `ensure_chrome_permissions()`   | Chrome 启动前注入 `local_network`/`loopback_network` 权限到 Preferences，免弹原生允许对话框 |
| `kill_chrome_on_port(port)`     | 只 kill 占用指定端口的 Chrome 进程，不影响用户自己的 Chrome 窗口 |
| `_save_iframe_html(page, name)` | 保存 **iframe 内部** HTML 到 debug 目录，补充 `_save_html`（只保存外层页面）的不足 |

**修改函数**

**`_dismiss_popups(page)`**

- 旧：`xpath://button[.//span[text()="确定"]]` — 精确匹配，按钮 span 含换行符时失败
- 新：`xpath://button[normalize-space(.)="确定"]` + `xpath://button[.//span[contains(normalize-space(),"确定")]]` — 容忍空白字符

**`export_excel(page)`**

- 移除 `window.localStorage.clear()` 调用（会破坏 iframe 认证）
- 导出后先 `page.get_frame('tag:iframe')` 在 iframe 内查找确定按钮；若失败再回退 `_dismiss_popups`
- 新增 `_save_iframe_html(page, 'export_dialog_iframe.html')` 保存 iframe HTML 供调试

**`wait_for_download(timeout, not_before)`**

- 新增 `not_before` 参数（`time.time()` 时间戳），只认本次运行后生成的文件，避免误取旧文件
- 新增双目录搜索（`DOWNLOAD_DIR` + `DOWNLOAD_ROOT`），文件在其他目录时自动 copy 回来

**`main()`**

- 新增 `download_start = time.time()` 记录启动时间
- 将 `wait_for_download(not_before=download_start)` 移入 `try` 块（Chrome 关闭前）

**已知待解决**

- UV 数据在自动化浏览器中仍为"暂无数据"（空 Excel），用户手动操作可正常导出含数据的文件。推测原因：Chrome 原生「允许访问本地网络」弹窗在自动化环境中未被有效处理，导致 iframe 内 API 调用失败。已尝试：Preferences 权限注入 + `--disable-features=PrivateNetworkAccessChecks` + 隐私/非隐私模式切换，均未能使数据正常加载。

------

### [2026-02-24] 友盟多端数据接入与自动化入库

- **原生应用接入**：完成安卓、苹果、鸿蒙三大平台日活（DAU）数据的自动获取。
- **小程序接入**：完成微信小程序、支付宝小程序日活（DAU）数据的自动获取。
- **数据结构映射**：打通 umeng.uapp.getActiveUsers 与 umeng.umini.getOverview 接口，自动写入 daily.platform_daily_metrics 表。
- **安全防重**：底层引入 ON DUPLICATE KEY UPDATE 语法防重，支持任务的每日定时与幂等重试。