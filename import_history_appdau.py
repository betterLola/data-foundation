# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
import_history_appdau.py
离线历史数据导入工具：从 CSV 文件批量导入 510100_items 事件明细。
"""
import os
import sys
import pandas as pd
import pymysql
import logging
from datetime import datetime
import glob

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
            os.path.join(LOG_DIR, f"import_history_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def create_table(cursor):
    """确保目标表存在"""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS `5100_detail` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            service_amount BIGINT,
            resource_name VARCHAR(255),
            service_name VARCHAR(255),
            stat_date DATE,
            port VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''')


def get_port_from_filename(filename):
    """根据文件名识别端"""
    if 'Android' in filename:
        return '安卓'
    elif 'iPhone' in filename:
        return '苹果'
    elif 'Harmony' in filename:
        return '鸿蒙'
    return '未知'


def process_file(filepath, cursor):
    """处理单个 CSV 文件并导入"""
    filename = os.path.basename(filepath)
    port = get_port_from_filename(filename)
    resource_name = '510100_items'

    log.info(f"正在处理文件: {filename} (识别端: {port})...")

    df = None
    # 尝试不同编码读取
    for enc in ['utf-8-sig', 'gbk', 'gb2312', 'utf-8']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            break
        except Exception:
            pass

    if df is None:
        log.error(f"无法读取文件 {filename}，请检查编码格式。")
        return

    if len(df.columns) < 3:
        log.error(f"文件 {filename} 列数不足，预期至少3列。")
        return

    # 约定：1列日期，2列参数名，3列消息数
    date_col = df.columns[0]
    param_col = df.columns[1]
    amount_col = df.columns[2]

    df = df.dropna(subset=[date_col, param_col, amount_col])
    df['parsed_date'] = pd.to_datetime(df[date_col], format='mixed', errors='coerce')
    df = df.dropna(subset=['parsed_date'])

    insert_query = '''
        INSERT INTO `5100_detail` (service_amount, resource_name, service_name, stat_date, port)
        VALUES (%s, %s, %s, %s, %s)
    '''

    batch_size = 5000
    data_to_insert = []
    total_inserted = 0

    for _, row in df.iterrows():
        try:
            amount = int(row[amount_col])
            service_name = str(row[param_col]).strip()
            stat_date = row['parsed_date'].strftime('%Y-%m-%d')

            data_to_insert.append((amount, resource_name, service_name, stat_date, port))

            if len(data_to_insert) >= batch_size:
                cursor.executemany(insert_query, data_to_insert)
                total_inserted += len(data_to_insert)
                data_to_insert = []
        except:
            pass

    if data_to_insert:
        cursor.executemany(insert_query, data_to_insert)
        total_inserted += len(data_to_insert)

    log.info(f"文件 {filename} 导入完成，共计 {total_inserted} 条。")


def main(data_dir):
    """批量导入目录下所有 CSV"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        create_table(cursor)
        
        csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
        if not csv_files:
            log.warning(f"目录 {data_dir} 下未找到 CSV 文件。")
            return

        log.info(f"共发现 {len(csv_files)} 个文件待导入。")
        for f in csv_files:
            process_file(f, cursor)
            conn.commit()
            
    except Exception as e:
        log.error(f"导入过程中发生异常: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    # 示例用法：python import_history_appdau.py ./data/
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    main(target_dir)
