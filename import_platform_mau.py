# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
import_platform_mau.py
历史月活数据导入工具：从 Excel 文件导入全平台月度核心指标 (MAU, DAU, 留存等)。
"""
import os
import sys
import pandas as pd
import pymysql
import math
import logging
from datetime import datetime

# 设置输出编码
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ── 统一配置加载 ─────────────────────────────────────────────
try:
    from config import DB_CONFIG
except ImportError:
    DB_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '更换为自己的MySQL密码',
        'database': 'daily',
        'charset': 'utf8mb4'
    }

# ── 日志 ─────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"import_mau_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def clean_val(val):
    """清理 NaN/NaT 值，转换为 None 兼容 MySQL"""
    if pd.isna(val):
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def parse_date(val):
    """安全解析 Excel 日期"""
    if pd.isna(val):
        return None

    val_str = str(val).strip()

    # 处理 Excel 数字格式日期
    if val_str.isdigit():
        try:
            dt = pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val_str), unit='D')
            return dt
        except:
            pass

    # 处理中文年月日格式
    val_str = val_str.replace('年', '-').replace('月', '').replace('日', '')

    try:
        return pd.to_datetime(val_str)
    except:
        return None


def import_data(excel_path='platform_mau.xlsx'):
    """执行导入逻辑"""
    if not os.path.exists(excel_path):
        log.error(f"找不到 Excel 文件: {excel_path}")
        return

    log.info(f"正在读取 Excel: {excel_path} ...")
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        log.error(f"读取 Excel 失败: {e}")
        return

    log.info(f"Excel 列名: {df.columns.tolist()}")

    # 识别日期列
    date_col = 'date_month'
    if date_col not in df.columns:
        date_col = df.columns[0]
        log.warning(f"未找到 '{date_col}' 列，默认使用第一列作为日期。")

    # 统一日期格式为当月1号
    try:
        df[date_col] = df[date_col].apply(parse_date).dt.strftime('%Y-%m-01')
    except Exception as e:
        log.error(f"日期列解析失败: {e}")
        return

    log.info("连接数据库...")
    conn = pymysql.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cursor:
            # 确保表存在
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS platform_mau (
                    date_month DATE PRIMARY KEY,
                    mau INT UNSIGNED,
                    mau_percent DECIMAL(5,4),
                    total_register_users BIGINT UNSIGNED,
                    dau INT UNSIGNED,
                    monthly_avg_total_register_users BIGINT UNSIGNED,
                    dau_percent DECIMAL(5,4),
                    retention_percent DECIMAL(5,4)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            sql = """
                INSERT INTO platform_mau
                (date_month, mau, mau_percent, total_register_users, dau,
                 monthly_avg_total_register_users, dau_percent, retention_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    mau = VALUES(mau),
                    mau_percent = VALUES(mau_percent),
                    total_register_users = VALUES(total_register_users),
                    dau = VALUES(dau),
                    monthly_avg_total_register_users = VALUES(monthly_avg_total_register_users),
                    dau_percent = VALUES(dau_percent),
                    retention_percent = VALUES(retention_percent)
            """

            insert_count = 0
            for _, row in df.iterrows():
                # 按列名映射数据
                vals = (
                    clean_val(row.get(date_col)),
                    clean_val(row.get('mau')),
                    clean_val(row.get('mau_percent')),
                    clean_val(row.get('total_register_users')),
                    clean_val(row.get('dau')),
                    clean_val(row.get('monthly_avg_total_register_users')),
                    clean_val(row.get('dau_percent')),
                    clean_val(row.get('retention_percent'))
                )
                
                if vals[0] is None: continue # 日期为空则跳过

                cursor.execute(sql, vals)
                insert_count += 1

            conn.commit()
            log.info(f"导入完成，共计 {insert_count} 条记录已更新。")

    except Exception as e:
        log.error(f"数据库操作异常: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    target_file = sys.argv[1] if len(sys.argv) > 1 else "platform_mau.xlsx"
    import_data(target_file)
