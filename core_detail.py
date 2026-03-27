# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
核心功能点击明细数据导入脚本
逐日拉取 core_function_click 事件的参数明细数据，入库到 core_detail 表。
eventParamName 优先使用 item_name，若无数据则回退到 item-name。
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
            os.path.join(LOG_DIR, f"core_detail_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

EVENT_NAME = 'core_function_click'
PARAM_CANDIDATES = ['item_name', 'item-name']  # 按优先级依次尝试
PAGE_SIZE = 1000


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


def fetch_pages(appkey, target_date, param_name):
    """
    分页拉取指定日期、端、参数名的数据。
    返回 (all_items列表, page_count)。
    """
    all_items = []
    page_no = 1

    while True:
        req = aop.api.UmengUappEventParamGetValueListRequest()
        req.appkey = appkey
        req.startDate = target_date
        req.endDate = target_date
        req.eventName = EVENT_NAME
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


def fetch_and_store_core_detail(start_date='2026-01-01', end_date=None):
    """逐日拉取 core_function_click 明细数据并入库"""
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 重建 core_detail 表（确保字段正确）
        cursor.execute("DROP TABLE IF EXISTS core_detail")
        cursor.execute("""
            CREATE TABLE core_detail (
                resource_amount BIGINT,
                resource_name   VARCHAR(255),
                item_name       VARCHAR(500),
                stat_date       DATE,
                port            VARCHAR(50)
            ) DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()

        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        dates_to_fetch = get_date_range(start_date, end_date)
        log.info(f"开始获取 {start_date} 到 {end_date} 的核心功能明细数据，共 {len(dates_to_fetch)} 天...")

        for target_date in dates_to_fetch:
            log.info(f"--- 获取日期: {target_date} ---")
            for port_name, appkey in APPS.items():

                # 检查该日期+端是否已有数据，避免重复入库
                cursor.execute("""
                    SELECT COUNT(*) FROM core_detail
                    WHERE stat_date = %s AND port = %s AND resource_name = %s
                """, (target_date, port_name, EVENT_NAME))
                existing = cursor.fetchone()[0]
                if existing > 0:
                    log.info(f"  [{port_name}] {target_date} 已有 {existing} 条数据，跳过。")
                    continue

                # 优先尝试 item_name，无数据则回退到 item-name
                all_items = []
                used_param = None
                page_count = 0
                for param_name in PARAM_CANDIDATES:
                    items, pages = fetch_pages(appkey, target_date, param_name)
                    if items:
                        all_items = items
                        used_param = param_name
                        page_count = pages
                        break

                if not all_items:
                    log.info(f"  [{port_name}] 暂无数据（已尝试：{', '.join(PARAM_CANDIDATES)}）。")
                    continue

                inserted = 0
                for item in all_items:
                    raw_name     = item.get("name", "")
                    count_val    = item.get("count", 0)
                    decoded_name = decode_url_encoded_str(raw_name)

                    cursor.execute("""
                        INSERT INTO core_detail
                            (resource_amount, resource_name, item_name, stat_date, port)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (count_val, EVENT_NAME, decoded_name, target_date, port_name))
                    inserted += 1

                log.info(f"  [{port_name}] 使用参数 [{used_param}]，共 {page_count} 页，入库 {inserted} 条数据。")

            # 每天提交一次，减少锁等待
            conn.commit()

        cursor.close()
        conn.close()
        log.info("所有核心功能明细数据已成功入库。")

    except Exception as e:
        log.error(f"代码执行异常：{str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    fetch_and_store_core_detail()
