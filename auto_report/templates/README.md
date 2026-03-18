# 报告模板目录使用说明

## 目录结构

```
templates/
    my_report.docx      ← Word 模板（用 {{变量名}} 标记占位符）
    my_report.json      ← 字段配置（与模板同名，扩展名换 .json）
    daily_report.json   ← 工作日报配置示例
    weekly_report.json  ← 周报配置示例
```

## 制作模板

1. 新建或复制一个 Word 文档
2. 在需要填入数据的位置写 `{{变量名}}`，例如：

   > 当日活跃用户 **{{platform_dau}}** 万，增幅 {{dau_growth}}

3. 保存为 `.docx`，放入本目录

## 编写配置文件

与模板同名，扩展名改为 `.json`：

```json
{
  "report_name": "我的报告",
  "output_dir": "报表产出",
  "output_filename": "我的报告_{date}.docx",
  "variables": {
    "platform_dau": {
      "type": "sql",
      "query": "SELECT platform_dau FROM platform_daily_metrics WHERE stat_date = '{yesterday}'",
      "format": "wan"
    },
    "custom_text": {
      "type": "literal",
      "value": "固定文字内容"
    }
  }
}
```

## 变量类型（type）

| type | 说明 |
|------|------|
| `sql` | 执行 SQL，取第一行第一列 |
| `date` | 内置日期（`today` / `yesterday` / `last_week`） |
| `literal` | 固定文字 |

## 格式化（format）

| format | 效果 |
|--------|------|
| `wan` | `123456` → `12.35万` |
| `pct` | `0.0523` → `+5.23%` |
| `int` | `123456` → `123,456` |
| `date_cn` | `2026-03-18` → `3月18日` |
| `raw` | 原始值（默认） |

## SQL 中的内置变量

| 变量 | 说明 |
|------|------|
| `{yesterday}` | 昨日（YYYY-MM-DD） |
| `{today}` | 今日 |
| `{last_week}` | 上周同日 |

## 运行

```bash
# 生成单个报告
python report_engine.py --template templates/my_report.docx

# 批量生成模板目录中所有报告
python report_engine.py

# 列出可用模板
python report_engine.py --list
```
