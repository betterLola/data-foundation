# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
search_detail_import.py
搜索详情数据导入工具：从友盟 API 获取 search_behavior 事件的 search_content 参数详情并入库。
"""
import sys
import os
import aop
import aop.api
import pymysql
import traceback
import urllib.parse
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

# ── 日志 ─────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"search_import_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def decode_url_encoded_str(encoded_str):
    """通用URL解码函数"""
    if not isinstance(encoded_str, str):
        return encoded_str
    try:
        return urllib.parse.unquote(encoded_str, encoding="utf-8")
    except Exception:
        return encoded_str


def get_date_range(start_date, end_date):
    """获取日期范围列表"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def fetch_and_store_search_history(start_date='2026-01-01', end_date=None):
    """抓取搜索关键词详情并入库"""
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 确保表存在
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_detail (
                search_amount BIGINT,
                search_name   VARCHAR(255),
                stat_date     DATE,
                port          VARCHAR(50),
                resource_name VARCHAR(255)
            ) DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()

        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        dates_to_fetch = get_date_range(start_date, end_date)
        log.info(f"计划抓取 {start_date} 至 {end_date} 的搜索详情数据，共 {len(dates_to_fetch)} 天...")

        for target_date in dates_to_fetch:
            log.info(f"--- 日期: {target_date} ---")
            for port_name, appkey in APPS.items():
                # 检查是否已存在
                check_sql = """
                    SELECT COUNT(*) FROM search_detail
                    WHERE stat_date = %s AND port = %s AND resource_name = 'search_behavior'
                """
                cursor.execute(check_sql, (target_date, port_name))
                if cursor.fetchone()[0] > 0:
                    log.info(f"  [{port_name}] 已存在数据，跳过")
                    continue

                req = aop.api.UmengUappEventParamGetValueListRequest()
                req.appkey = appkey
                req.startDate = target_date
                req.endDate = target_date
                req.eventName = 'search_behavior'
                req.eventParamName = 'search_content'

                try:
                    resp = req.get_response(None)
                    if resp and resp.get("success") is False:
                        log.error(f"  [{port_name}] 失败: {resp.get('errorMsg')}")
                        continue

                    param_infos = resp.get("paramInfos", [])
                    if not param_infos:
                        log.info(f"  [{port_name}] 无数据")
                        continue

                    inserted = 0
                    for item in param_infos:
                        raw_name = item.get("name", "")
                        count    = item.get("count", 0)
                        decoded  = decode_url_encoded_str(raw_name)

                        sql = """
                            INSERT INTO search_detail (search_amount, search_name, stat_date, port, resource_name)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.execute(sql, (count, decoded, target_date, port_name, 'search_behavior'))
                        inserted += 1
                    
                    log.info(f"  [{port_name}] 成功导入 {inserted} 条关键词详情")

                except Exception as e:
                    log.error(f"  [{port_name}] 异常: {e}")

            conn.commit()

        cursor.close()
        conn.close()
        log.info("搜索详情抓取任务完成。")

    except Exception as e:
        log.error(f"执行异常：{str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_search_history()
