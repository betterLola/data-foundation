# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
资源位明细数据导入脚本
逐日拉取各资源位事件的参数明细数据，入库到 resource_detail 表。
- mid_banner / news_click / top_banner_click / Hometopic_click → eventParamName=item_name
- person_banner_click → eventParamName=title
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
            os.path.join(LOG_DIR, f"resource_detail_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# 各资源位事件及其对应的 eventParamName
EVENTS = {
    'mid_banner':          'item_name',
    'news_click':          'item_name',
    'top_banner_click':    'item_name',
    'Hometopic_click':     'item_name',
    'person_banner_click': 'title',
}

PAGE_SIZE = 1000


def fetch_pages(appkey, target_date, event_name, param_name):
    """分页拉取指定日期、端、事件的参数明细，返回 (all_items, page_count)"""
    all_items = []
    page_no = 1

    while True:
        req = aop.api.UmengUappEventParamGetValueListRequest()
        req.appkey = appkey
        req.startDate = target_date
        req.endDate = target_date
        req.eventName = event_name
        req.eventParamName = param_name

        try:
            resp = req.get_response(None, pageNo=page_no, pageSize=PAGE_SIZE)
        except Exception as e:
            log.error(f"    第{page_no}页接口调用异常：{str(e)}")
            break

        if resp and resp.get("success") is False:
            log.error(f"    接口失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
            break

        param_infos = resp.get("paramInfos", []) if resp else []
        if not param_infos:
            break

        all_items.extend(param_infos)

        if len(param_infos) < PAGE_SIZE:
            break
        page_no += 1

    return all_items, page_no


def decode_url_encoded_str(encoded_str):
    """通用URL解码函数，处理中文编码"""
    if not isinstance(encoded_str, str):
        return encoded_str
    try:
        return urllib.parse.unquote(encoded_str, encoding="utf-8")
    except Exception:
        return encoded_str


def get_date_range(start_date, end_date):
    """生成日期列表"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def fetch_and_store_resource_detail(start_date='2026-01-01', end_date=None):
    """逐日拉取各资源位事件明细数据并入库"""
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 重建 resource_detail 表（确保字段正确）
        cursor.execute("DROP TABLE IF EXISTS resource_detail")
        cursor.execute("""
            CREATE TABLE resource_detail (
                resource_amount BIGINT,
                resource_name   VARCHAR(255),
                item_name       VARCHAR(255),
                stat_date       DATE,
                port            VARCHAR(50)
            ) DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()

        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        dates_to_fetch = get_date_range(start_date, end_date)
        log.info(f"开始获取 {start_date} 到 {end_date} 的资源位明细数据，共 {len(dates_to_fetch)} 天...")

        for target_date in dates_to_fetch:
            log.info(f"--- 获取日期: {target_date} ---")
            for port_name, appkey in APPS.items():
                for event_name, param_name in EVENTS.items():

                    # 检查该日期+端+事件是否已有数据，避免重复入库
                    cursor.execute("""
                        SELECT COUNT(*) FROM resource_detail
                        WHERE stat_date = %s AND port = %s AND resource_name = %s
                    """, (target_date, port_name, event_name))
                    existing = cursor.fetchone()[0]
                    if existing > 0:
                        log.info(f"  [{port_name}] {event_name} {target_date} 已有 {existing} 条数据，跳过。")
                        continue

                    all_items, page_count = fetch_pages(appkey, target_date, event_name, param_name)

                    if not all_items:
                        log.info(f"  [{port_name}] {event_name} 暂无数据。")
                        continue

                    inserted = 0
                    for item in all_items:
                        raw_name   = item.get("name", "")
                        count_val  = item.get("count", 0)
                        decoded_name = decode_url_encoded_str(raw_name)

                        cursor.execute("""
                            INSERT INTO resource_detail
                                (resource_amount, resource_name, item_name, stat_date, port)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (count_val, event_name, decoded_name, target_date, port_name))
                        inserted += 1

                    log.info(f"  [{port_name}] {event_name} 共 {page_count} 页，入库 {inserted} 条明细数据。")

            # 每天提交一次，减少锁等待
            conn.commit()

        cursor.close()
        conn.close()
        log.info("所有资源位明细数据已成功入库。")

    except Exception as e:
        log.error(f"代码执行异常：{str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    fetch_and_store_resource_detail()
