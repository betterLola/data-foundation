# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import pymysql
import traceback
import os
import sys
import logging
from datetime import datetime, timedelta

# 设置输出编码
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ── 统一配置加载 ─────────────────────────────────────────────
try:
    from config import (
        DB_CONFIG,
        UMENG_API_KEY      as API_KEY,
        UMENG_API_SECRET   as API_SECURITY,
        UMENG_APP_APPKEYS  as APPS,
    )
except ImportError:
    API_KEY = "更换为友盟API_KEY"
    API_SECURITY = "更换为友盟API_SECRET"
    APPS = {
        '安卓': '更换为安卓端AppKey',
        '苹果': '更换为苹果端AppKey',
        '鸿蒙': '更换为鸿蒙端AppKey',
    }
    DB_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '更换为自己的MySQL密码',
        'database': 'daily',
        'charset': 'utf8mb4'
    }

# 需要统计的历史事件
EVENTS = [
    'search_behavior'
]

# ── 日志 ─────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"resource_history_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def get_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates

def fetch_and_store_history_data():
    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        # 示例范围
        dates_to_fetch = get_date_range('2026-01-01', '2026-03-12')
        log.info(f"开始抓取历史数据: {dates_to_fetch[0]} 至 {dates_to_fetch[-1]}")

        for target_date in dates_to_fetch:
            log.info(f"--- 日期: {target_date} ---")
            for port_name, appkey in APPS.items():
                for event_name in EVENTS:
                    # 检查是否已存在
                    check_sql = "SELECT 1 FROM resource_total WHERE resource_name = %s AND stat_date = %s AND port = %s"
                    cursor.execute(check_sql, (event_name, target_date, port_name))
                    if cursor.fetchone():
                        log.info(f"  [{port_name}] {event_name} 已存在，跳过")
                        continue

                    req = aop.api.UmengUappEventGetDataRequest()
                    req.appkey = appkey
                    req.startDate = target_date
                    req.endDate = target_date
                    req.eventName = event_name

                    try:
                        resp = req.get_response(None)
                        if resp and resp.get("success") is False:
                            log.error(f"  [{port_name}] {event_name} 失败: {resp.get('errorMsg')}")
                            continue

                        amount = 0
                        if resp and "eventData" in resp:
                            event_data = resp["eventData"]
                            if event_data and len(event_data) > 0:
                                data_list = event_data[0].get("data", [])
                                if data_list and len(data_list) > 0:
                                    amount = data_list[0]
                        
                        log.info(f"  [{port_name}] {event_name} = {amount}")

                        sql = """
                            INSERT INTO resource_total (resource_amount, resource_name, stat_date, port)
                            VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(sql, (amount, event_name, target_date, port_name))
                    except Exception as e:
                        log.error(f"  [{port_name}] {event_name} 接口异常: {e}")

            conn.commit()

        cursor.close()
        conn.close()
        log.info("历史数据抓取完成。")

    except Exception as e:
        log.error(f"执行异常：{str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_history_data()
