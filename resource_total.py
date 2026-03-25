# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import json
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

# 需要统计的事件名称列表
EVENTS = [
    'mid_banner',
    'news_click',
    'person_banner_click',
    'top_banner_click',
    'Hometopic_click'
]

# ── 日志 ─────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"resource_total_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def fetch_and_store_data():
    """获取各端事件数据并存入数据库"""
    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 基础配置
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        # 获取昨天的日期
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        log.info(f"开始获取 {yesterday_date} 的事件数据...")

        for port_name, appkey in APPS.items():
            for event_name in EVENTS:
                req = aop.api.UmengUappEventGetDataRequest()
                req.appkey = appkey
                req.startDate = yesterday_date
                req.endDate = yesterday_date
                req.eventName = event_name

                resp = req.get_response(None)
                
                if resp and resp.get("success") is False:
                    log.error(f"[{port_name}] {event_name} 接口调用失败：{resp.get('errorMsg')}")
                    continue

                resource_amount = 0
                if resp and "eventData" in resp:
                    event_data = resp["eventData"]
                    if event_data and len(event_data) > 0:
                        data_list = event_data[0].get("data", [])
                        if data_list and len(data_list) > 0:
                            resource_amount = data_list[0]
                
                log.info(f"[{port_name}] {event_name}: {resource_amount}")

                sql = """
                    INSERT INTO resource_total (resource_amount, resource_name, stat_date, port)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(sql, (resource_amount, event_name, yesterday_date, port_name))

        conn.commit()
        cursor.close()
        conn.close()
        log.info("所有数据已成功入库。")

    except Exception as e:
        log.error(f"代码执行异常：{str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_data()
