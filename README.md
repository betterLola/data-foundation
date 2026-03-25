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
- **内网系统无 API**：注册用户数、实名用户数等核心指标仅能通过 Web界面获取。
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
  
  *自动化报告生成和NL2SQL 智能问答详情请见另两个仓库
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
- **回填与重算机制**：支持自动检测近 7 天缺失数据并回填，回填后自动重新计算受影响的累计指标（注册/实名/服务次数）。
- **业务汇总自动计算**：全平台总日活 `platform_dau`、累计注册/实名用户滚动累加均在 `main.py` 统一结算。
- **全库自动去重**：每次主流程结束后，自动扫描并清理所有表中完全重复的行。
- **爬虫反检测**：隐藏 `navigator.webdriver` 特征、模拟真人随机交互轨迹、Chrome 专用 Profile 隔离、进程级清理与崩溃自愈。
- **历史补录**：提供离线历史导入工具，支持从 CSV/Excel 批量补录 5100 明细、月活等存量数据。

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

### 3. 配置敏感信息

**推荐方式**：复制配置模板并填写真实值（`config.py` 已在 `.gitignore` 中被排除，不会上传到 Git）：

```bash
cp config.example.py config.py
# 编辑 config.py，填入真实的数据库密码、API Key、AppKey 等
```

### 4. 初始化数据库

连接 MySQL 后，手动创建数据库：

```sql
CREATE DATABASE IF NOT EXISTS `daily` DEFAULT CHARACTER SET utf8mb4;
```

### 5. 运行全流程

```bash
python main.py
```

---

## 脚本说明

| 脚本                             | 类型   | 调用方式         | 核心职责                                     |
| ------------------------------ | ---- | ------------ | ---------------------------------------- |
| `main.py`                      | 总调度  | 手动 / 定时任务    | 按序调用所有采集模块，完成业务汇总、质量检查、去重与告警                   |
| `data_backfilling.py`          | 核心   | `main.py` 调用 | 自动检查近7天数据缺失并执行多源重抓回填                         |
| `UmengAPI.py`                  | API  | `main.py` 调用 | 获取原生APP（安卓/苹果/鸿蒙）及小程序日活，写入 `platform_daily_metrics` |
| `5100_detail.py`               | API  | `main.py` 调用 | 获取指定事件各子服务的点击分布，写入 `5100_detail`         |
| `internal_network_spider.py`   | 爬虫   | `main.py` 调用 | 爬取内网平台新增注册/实名用户数，含进程自愈逻辑                         |
| `smart_frontend_dau_spider.py` | 爬虫   | `main.py` 调用 | 登录智能前端系统，导出 UV 数据，解析 Excel 后写入           |
| `recalculate_aggregates.py`    | 工具   | 手动           | 手动重算指定日期范围内的汇总指标（DAU、累计用户数等）                  |
| `resource_total.py`            | API  | `main.py` 调用 | 获取关键资源位（Banner/新闻等）昨日点击量                 |
| `fetch_retention.py`           | API  | `main.py` 调用 | 拉取三端次日留存率，写入 `app_retention`              |
| `import_history_appdau.py`     | 工具   | 手动           | 从 CSV 文件批量导入 5100 事件明细历史数据                     |
| `import_platform_mau.py`       | 工具   | 手动           | 从 Excel 导入全平台月度核心指标历史数据                     |

---

## 更新日志 (Changelog)

### [2026-03-25] 数据底座核心脚本深度同步与架构优化

**核心改进内容：**

1.  **进程管理与稳定性增强：**
    *   在 `internal_network_spider.py`、`smart_frontend_dau_spider.py` 和 `data_backfilling.py` 中引入了精准清理特定 Profile 进程的逻辑。
    *   新增 `_clear_chrome_lock` 函数，自动修复 Chrome 锁文件及崩溃退出状态，彻底解决自动化启动弹窗阻塞。

2.  **数据采集逻辑精细化：**
    *   **内网爬虫**：重构 `extract_data` 逻辑，支持按日期遍历匹配，极大提升了数据错位时的容错性。
    *   **搜索详情**：同步了 `search_detail_import.py`，支持搜索关键词详情的批量补录。
    *   **历史导入**：完成了 `import_history_appdau.py` 和 `import_platform_mau.py` 的标准化同步。

3.  **汇总计算与重算闭环：**
    *   新增 `recalculate_aggregates.py` 独立工具，用于历史数据调整后一键重算累计指标。
    *   优化 `main.py` 计算逻辑，确保回填后自动触发连锁重算。

---

### [2026-03-24] 内网爬虫提取逻辑重构与累计数据自动重算优化

**修复与优化内容：**

1.  **internal_network_spider.py：**
    *   进程与锁管理：新增了 kill_chrome_on_port 和 clear_chrome_lock 函数，在启动前会自动清理残留的 Chrome 进程和 SingletonLock 文件。
    *   数据提取逻辑优化：extract_data 不再死板地抓取第一行，而是会遍历表格行寻找匹配 YESTERDAY 日期的行。

2.  **data_backfilling.py：**
    *   缺失检查强化：get_missing_dates 现在不仅检查 NULL 值，还会针对内网数据检查 reg == 0 或 real == 0的情况。
    *   全量回填支持：同步了完整的回填逻辑，支持一次性补抓过去 7 天内缺失的所有细分指标。

3.  **main.py：**
    *   回填逻辑联动：优化了调度逻辑，回填处理过的数据源会自动从当日任务中剔除。
    *   汇总逻辑修复：确保回填后的逻辑汇总会从最早的缺失日期开始重新连锁计算。

---

### [2026-03-23] 浏览器爬虫稳定性深度修复与环境自愈优化

**修复内容：**

**智能前端爬虫 (smart_frontend_dau_spider.py) 导出逻辑重构：**

*   **解决中断**：修复了在导出 Excel 过程中因“页面被刷新”导致的 `TimeoutError`。
*   **弹性 Iframe 处理**：重构了 `export_excel` 函数，新增动态探测并重新连接 iframe 的逻辑。

---

### [2026-03-18] 跨项目同步与核心逻辑增强

**同步与优化内容：**

- **`internal_network_spider.py` (数据提取逻辑重构)**：弃用动态列索引推算，改为锁定 `is-scrolling-none` 主表体容器，固定使用 CSS 类名提取数据。
- **`data_backfilling.py` (回填模块同步更新)**：同步修正了新增实名与新增注册的列映射规则。
- **`main.py` (历史累计逻辑鲁棒性增强)**：支持自动向前追溯最近一条有效历史记录进行加总，不再受限于“前一日必须存在数据”。

---

### [2026-03-17] 内网爬虫列定位重构（CSS class 名精准提取）

- **核心数据回填机制**：新增 data_backfilling.py 模块，自动检查近 7 天历史数据缺失。
- **全库数据去重清洗**：引入 deduplicate_all_tables() 机制，按日清除全库重复数据。

---

### [2026-03-16 15:30] 修复 main.py 任务调度逻辑冲突

- **强制任务去重**：修复了回填与常规任务重复执行的 Bug。

---

### [2026-03-16 14:30] 数据采集与回填流程重大 Bug 修复及优化

- **回填功能前置**：将缺失检查提前至所有采集脚本执行之前。
- **自动重算机制**：回填后逐日向后重新计算汇总字段，确保一致性。

---

### [2026-03-04] App次日留存数据自动拉取与入库

新增了 `fetch_retention.py` 脚本，自动化获取友盟新增用户次日留存率。

---

### [2026-03-03] 新增多端自定义事件统计脚本

- **resource_total.py** - 关键资源位点击量统计。
- **5100_detail.py** - 510100_items 事件子服务点击详情。

---

### [2026-02-25] smart_frontend_dau_spider.py 深度修复

- **进程管理优化**：改为 `kill_chrome_on_port` 避免误杀用户窗口。
- **权限注入**：新增 `ensure_chrome_permissions` 自动处理本地网络权限弹窗。

---

### [2026-02-24] 友盟多端数据接入与自动化入库

- **基础接入**：完成安卓、苹果、鸿蒙及小程序日活数据的自动获取与防重入库。
