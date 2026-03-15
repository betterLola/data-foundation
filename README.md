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

| 脚本 | 负责写入字段 |
|------|------------|
| `UmengAPI.py` | `android_dau`, `ios_dau`, `harmonyos_dau`, `app_dau`, `mini_program_dau`, `alipay_dau` |
| `internal_network_spider.py` | `new_register_users`, `new_realname_users` |
| `smart_frontend_dau_spider.py` | `smart_frontend_dau` |
| `5100_detail.py` | `5100_detail` 表（事件明细） |
| `fetch_retention.py` | `app_retention` 表（次日留存率） |
| `resource_total.py` | `resource_total` 表（关键资源位点击） |

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

| 配置项 | 说明 | 位置 |
|-------|------|------|
| `API_KEY` | 开放平台账号唯一标识 | 账号设置 → API Key |
| `API_SECURITY` | 接口签名密钥 | 账号设置 → API Secret |
| `AppKey` | 单个应用标识 | 应用列表 → 对应 App → AppKey |
| `DataSourceId` | 小程序数据源标识 | 小程序列表 → 对应小程序 → DataSourceId |

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

| 脚本 | 类型 | 调用方式 | 核心职责 |
|-----|------|---------|---------|
| `main.py` | 总调度 | 手动 / 定时任务 | 按序调用所有采集模块，完成业务汇总与全库去重 |
| `UmengAPI.py` | API | `main.py` 调用 | 获取原生APP（安卓/苹果/鸿蒙）及小程序日活，写入 `platform_daily_metrics` |
| `5100_detail.py` | API | `main.py` 调用 | 获取指定事件各子服务的点击分布，写入 `5100_detail` |
| `internal_network_spider.py` | 爬虫 | `main.py` 调用 | 爬取内网平台新增注册/实名用户数 |
| `smart_frontend_dau_spider.py` | 爬虫 | `main.py` 调用 | 登录智能前端系统，导出 UV 数据，解析 Excel 后写入 |
| `fetch_retention.py` | API | `main.py` 调用 | 拉取三端次日留存率（最近 14 天），写入 `app_retention` |
| `resource_total.py` | API | `main.py` 调用 | 获取关键资源位（Banner/新闻等）昨日点击量 |
| `resource_total_history.py` | API | 手动 | 历史数据批量回填（按日期范围拉取） |
| `search_detail_import.py` | API | 手动 | 批量拉取搜索词明细并入库 `search_detail` 表 |
| `import_history_appdau.py` | 工具 | 手动 | 从 CSV 文件批量导入历史事件明细数据 |
| `import_platform_mau.py` | 工具 | 手动 | 从 Excel 导入历史月活数据到 `platform_mau` 表 |

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

| 现象 | 原因 | 解决方案 |
|------|------|---------|
| `Can't connect to MySQL server` | MySQL 未启动或密码错误 | 检查 MySQL 服务状态，确认 `DB_CONFIG` 配置 |
| `error_code=40001: Please check appkey` | AppKey 无权限或配置错误 | 登录友盟控制台核查对应 AppKey |
| `ModuleNotFoundError: DrissionPage` | 使用了错误的 Python 环境 | 激活正确的虚拟环境后运行 |
| 爬虫下载超时 | 网络慢或确认弹窗未点击 | 查看 `debug/` 目录截图，适当延长 `timeout` |
| UV 数据为空（返回 0） | Chrome 原生网络权限弹窗未处理 | 首次运行可手动点击"允许"，后续自动 |
| 数据出现重复行 | 脚本重复运行 | `main.py` 末尾的去重逻辑会自动清理 |

---

*本项目用于公司内部数据基础设施建设，如有问题请提 Issue 或联系维护者。*
