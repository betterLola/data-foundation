# 天府市民云报告自动生成系统

> 最后更新：2026-03-18

本目录包含天府市民云工作日报与周报的自动化生成脚本，以及通用报告引擎。

---

## 目录结构

```
auto_report/
├── generate_daily_report.py   # 工作日报生成
├── weekly_report_generator.py # 周报生成
├── report_engine.py           # 通用报告引擎
├── requirements.txt           # Python 依赖
├── templates/                 # 报告模板目录
│   ├── daily_report.json      # 日报配置示例
│   ├── weekly_report.json     # 周报配置示例
│   └── README.md              # 模板使用说明
├── 报告自动生成.md             # 日报模块说明（原始文档）
└── 周报生成.md                 # 周报模块说明（原始文档）
```

---

## 环境配置

### 安装依赖

```bash
pip install -r requirements.txt
```

### 数据库配置

通过环境变量或项目根目录的 `.env` 文件配置（不要提交到版本控制）：

```ini
# .env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=daily
```

---

## 快速开始

### 生成工作日报

```bash
python generate_daily_report.py
```

输出：`报表产出/天府市民云工作日报-城运中心YYYYMMDD.docx`

### 生成周报（每周五运行）

```bash
python weekly_report_generator.py
```

输出：`周报-YY年M月D日-M月D日.docx`

### 通用报告引擎

```bash
# 生成单个报告（自动查找同名 .json 配置）
python report_engine.py --template templates/my_report.docx

# 批量生成 templates/ 目录中所有报告
python report_engine.py

# 列出可用模板
python report_engine.py --list
```

---

## 工作日报（generate_daily_report.py）

### 核心业务逻辑

| 指标 | 计算方式 |
|------|----------|
| 当日 DAU | `platform_daily_metrics.platform_dau` |
| DAU 增幅（周同比） | `(昨日 DAU - 上周同期 DAU) / 上周同期 DAU` |
| 核心功能服务人次 | `是否为服务.xlsx` 中标记为 1 的服务，汇总昨日 `5100_detail` |
| 增幅 Top5 服务 | 昨日 > 100 次 + 上周有数据，按增幅降序 |

### 文档格式

| 元素 | 规范 |
|------|------|
| 标题 | 方正小标宋_GBK 16pt，居中 |
| 副标题 | 楷体 14pt，与标题同段换行 |
| 正文 | 仿宋 16pt，首行缩进 2 字符 |
| 图表 | 增幅前5服务柱形图，6英寸宽，200 DPI |

### 路径配置

通过环境变量覆盖：

```ini
SERVICE_MAPPING_PATH=./历史数据/是否为服务.xlsx
OUTPUT_DIR=./报表产出
```

---

## 周报（weekly_report_generator.py）

### 周期定义

| 名称 | 范围 |
|------|------|
| **本周期** | 上周五 ~ 本周四（共7天） |
| **上周期** | 上上周五 ~ 上周四（共7天） |

每周五运行，自动计算两个周期的起止日期，无需手动指定。

### 报告结构

**一、日活情况**

- **整体数据**：日活均值（含社区保障e管家+1万）、环比变化
- **各端分析**（自动生成）：
  - 全部上升 → 简洁格式
  - 部分/全部下降 → 列出下降端均值、环比降幅、日均减少量
- **本周期趋势**：自动判断走势形态（震荡/先降后升/先升后降/单调）+ 折线对比图

**二、服务人次**

- 日均及环比
- 涨跌榜（本周或上周使用 ≥ 1000 次的服务，各取前三）

**三、本周总结**

- 日活趋势（自动复用）
- 人均行为次数（留空，人工填写）
- 本周宣推事件（留空，人工填写）
- 搜索词变化（自动生成增量前十 + 本周前十对比表格）

### 标准工作流

```
每周五上午
    │
    ├─① python search_detail_import.py   ← 补充搜索词数据（在 tfsmy 目录运行）
    │
    └─② python weekly_report_generator.py ← 生成周报 .docx
            │
            └─ 人工填写：日活原因 / 人均行为次数 / 宣推事件 / 搜索词分析
```

---

## 数据回填模块说明（data_backfilling.py）

> 本节说明 `tfsmy/data_backfilling.py` 的设计，供报告脚本理解数据来源。

检查 `daily` 数据库近 **7 天**（不含当日）的漏填数据，按数据来源分模块自动回填。

### 执行流程

```
main()
 ├── Step 1  get_missing_dates()              检查近7天哪些日期/字段缺失
 ├── Step 2  backfill_umeng_dau()             友盟 API → android/ios/harmony/app/mini/alipay DAU
 ├── Step 3  backfill_resource_total()        友盟 API → resource_total 事件表
 ├── Step 4  backfill_5100_detail()           友盟 API → 5100_detail 子服务明细
 ├── Step 5  backfill_smart_frontend_dau()    浏览器 → 下载30天Excel → 解析所有日期行
 └── Step 6  backfill_internal_network()      浏览器 → 内网表格 → 滑到底部提取所有日期行
```

每个 Step 独立 `try-except`，单步失败不中断其余步骤。

### 缺失检查规则（Step 1）

| 表 | 检查方式 | 触发条件 |
|---|---|---|
| `platform_daily_metrics` | 按列粒度 SELECT | `android_dau / ios_dau / harmonyos_dau / mini_program_dau / alipay_dau` 任一为 NULL |
| `platform_daily_metrics` | 按列粒度 SELECT | `smart_frontend_dau` 为 NULL |
| `platform_daily_metrics` | 按列粒度 SELECT | `new_register_users` 或 `new_realname_users` 为 NULL |
| `resource_total` | COUNT(*) 按日期 | 该日期行数为 0 |
| `5100_detail` | COUNT(*) 按日期 | 该日期行数为 0 |

### 关键优化

**Step 5（智能前端）**：一次下载 30 天 Excel，解析全部日期行为 `{date: dau}` 字典，批量覆盖多天缺口。

**Step 6（内网爬虫）**：用 JS 将 `.el-table__body-wrapper` 滚动到底部（近7天数据须滑底才完整展示），遍历所有行按日期匹配。

### 入库安全策略

| 场景 | 处理方式 |
|---|---|
| 友盟 DAU 字段（Step 2）| 行存在 → UPDATE；行不存在 → INSERT，不触碰爬虫字段 |
| resource_total / 5100_detail | 缺失检查确认无数据后直接 INSERT |
| smart_frontend / 内网数据 | `ON DUPLICATE KEY UPDATE`，仅更新对应字段 |

### 配置说明

所有账号、路径、端口在脚本顶部常量区统一管理：

```python
DB_CONFIG          = { ... }                     # 数据库连接（建议用环境变量）
API_KEY / API_SECURITY                           # 友盟 API 认证
PLATFORM_APPKEYS / MINI_PROGRAM_APPKEYS          # 各端 AppKey
SMART_* / INTERNAL_*                             # 浏览器爬虫配置
LOG_DIR                                          # 日志目录
```

---

## 通用报告引擎（report_engine.py）

### 使用方式

在 `templates/` 目录下放置：
- `my_report.docx` — Word 模板，用 `{{变量名}}` 标记占位符
- `my_report.json` — 字段配置文件（与模板同名，扩展名换 `.json`）

```bash
python report_engine.py --template templates/my_report.docx
```

### 配置文件格式

```json
{
  "report_name": "我的报告",
  "output_dir": "报表产出",
  "output_filename": "报告_{date}.docx",
  "variables": {
    "platform_dau": {
      "type": "sql",
      "query": "SELECT platform_dau FROM platform_daily_metrics WHERE stat_date = '{yesterday}'",
      "format": "wan"
    },
    "dau_growth": {
      "type": "sql",
      "query": "SELECT (a.platform_dau-b.platform_dau)/b.platform_dau FROM platform_daily_metrics a JOIN platform_daily_metrics b ON b.stat_date='{last_week}' WHERE a.stat_date='{yesterday}'",
      "format": "pct"
    }
  }
}
```

### 变量类型与格式化

| type | 说明 |
|------|------|
| `sql` | 执行 SQL，取第一行第一列 |
| `date` | 内置日期（`today` / `yesterday` / `last_week`） |
| `literal` | 固定文字 |

| format | 效果 |
|--------|------|
| `wan` | `123456` → `12.35万` |
| `pct` | `0.0523` → `+5.23%` |
| `int` | `123456` → `123,456` |
| `date_cn` | `2026-03-18` → `3月18日` |
| `raw` | 原始值（默认） |

### SQL 中的内置日期变量

| 变量 | 说明 |
|------|------|
| `{yesterday}` | 昨日（YYYY-MM-DD） |
| `{today}` | 今日 |
| `{last_week}` | 上周同日 |

---

## 数据库表说明

报告脚本依赖以下表（由 `tfsmy/main.py` 写入）：

| 表 | 用途 | 关键字段 |
|----|------|----------|
| `platform_daily_metrics` | 全平台日活汇总 | `stat_date`, `platform_dau`, `app_dau`, `alipay_dau`, `mini_program_dau`, `smart_frontend_dau`, `new_register_users`, `total_register_users`, `total_service_times` |
| `5100_detail` | 子服务使用明细 | `stat_date`, `service_name`, `service_amount`, `port` |
| `search_detail` | 搜索词明细 | `stat_date`, `search_name`, `search_amount`, `port`, `resource_name` |

---

## 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 字体缺失（方正小标宋_GBK） | 运行环境未安装该字体 | 安装字体，或 Word 会自动回退到宋体 |
| 核心服务次数为 0 | `5100_detail` 当日无数据 | 确认 `tfsmy/main.py` 采集已运行 |
| 搜索词表格为空 | `search_detail` 未入库 | 先运行 `tfsmy/search_detail_import.py` |
| `DB_PASSWORD` 未设置 | 未配置环境变量 | 创建 `.env` 文件或直接填写 `DB_CONFIG` |
