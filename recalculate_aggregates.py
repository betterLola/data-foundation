# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
recalculate_aggregates.py
手动重算指定日期范围内的汇总指标（DAU、累计注册、累计实名）。
通常用于回填历史数据后，重新计算关联的加总字段。
"""
import pymysql
import sys
import os
import logging
from datetime import datetime, timedelta

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
            os.path.join(LOG_DIR, f"recalc_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def update_daily_aggregates(target_date_str):
    """重算单个日期的加总指标"""
    log.info(f"正在重算 {target_date_str} 的汇总数据...")
    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()

        # 1. 获取当日各端日活和新增用户
        cursor.execute("""
            SELECT android_dau, ios_dau, harmonyos_dau, alipay_dau, mini_program_dau, smart_frontend_dau,       
                   new_register_users, new_realname_users
            FROM platform_daily_metrics
            WHERE stat_date = %s
        """, (target_date,))
        today_data = cursor.fetchone()

        if not today_data:
            log.warning(f"  警告: 未找到 {target_date_str} 的记录")
            return

        # 计算总 DAU
        platform_dau = sum([
            today_data.get('android_dau') or 0,
            today_data.get('ios_dau') or 0,
            today_data.get('harmonyos_dau') or 0,
            today_data.get('alipay_dau') or 0,
            today_data.get('mini_program_dau') or 0,
            today_data.get('smart_frontend_dau') or 0
        ])

        # 2. 获取前一日的累计用户数
        cursor.execute("""
            SELECT total_register_users, total_realname_users
            FROM platform_daily_metrics
            WHERE stat_date < %s AND total_register_users IS NOT NULL
            ORDER BY stat_date DESC LIMIT 1
        """, (target_date,))
        prev_data = cursor.fetchone()

        prev_reg = int(prev_data['total_register_users']) if prev_data else 0
        prev_real = int(prev_data['total_realname_users']) if prev_data else 0

        # 计算新的累计值
        new_total_reg = prev_reg + (today_data.get('new_register_users') or 0)
        new_total_real = prev_real + (today_data.get('new_realname_users') or 0)

        # 3. 更新数据库
        cursor.execute("""
            UPDATE platform_daily_metrics
            SET platform_dau = %s, total_register_users = %s, total_realname_users = %s
            WHERE stat_date = %s
        """, (platform_dau, new_total_reg, new_total_real, target_date))
        conn.commit()
        log.info(f"  完成: DAU={platform_dau}, 累计注册={new_total_reg}, 累计实名={new_total_real}")
    finally:
        conn.close()

if __name__ == "__main__":
    # 示例：重算最近一周的数据
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=7)

    current = start_date
    while current <= end_date:
        update_daily_aggregates(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
