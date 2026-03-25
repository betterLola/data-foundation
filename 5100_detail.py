# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import json
import pymysql
import traceback
import urllib.parse
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

# ── 日志 ─────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"5100_detail_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def decode_url_encoded_str(encoded_str):
    """通用URL解码函数，处理中文编码"""
    if not isinstance(encoded_str, str):
        return encoded_str
    try:
        return urllib.parse.unquote(encoded_str, encoding="utf-8")
    except Exception as e:
        log.warning(f"解码失败：{e}，返回原字符串")
        return encoded_str

def fetch_and_store_detail():
    """获取各端510100_items各子服务事件数据并存入数据库"""
    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 基础配置
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        # 获取昨天的日期
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        log.info(f"开始获取 {yesterday_date} 的 510100_items 数据...")

        resource_name = "510100_items"
        event_param_name = "service_name"

        for port_name, appkey in APPS.items():
            log.info(f"--- 正在拉取 [{port_name}] 端数据 ---")
            
            req = aop.api.UmengUappEventParamGetValueListRequest()
            req.appkey = appkey
            req.startDate = yesterday_date
            req.endDate = yesterday_date
            req.eventName = resource_name
            req.eventParamName = event_param_name

            resp = req.get_response(None)
            
            if resp and resp.get("success") is False:
                log.error(f"[{port_name}] 接口调用失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
                continue

            param_infos = resp.get("paramInfos", [])
            if not param_infos:
                log.info(f"[{port_name}] 暂无数据。")
                continue

            inserted_count = 0
            for item in param_infos:
                raw_name = item.get("name", "")
                count = item.get("count", 0)
                decoded_name = decode_url_encoded_str(raw_name)

                sql = """
                    INSERT INTO `5100_detail` (service_amount, resource_name, service_name, stat_date, port)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (count, resource_name, decoded_name, yesterday_date, port_name))
                inserted_count += 1

            log.info(f"[{port_name}] 成功入库 {inserted_count} 条数据。")

        conn.commit()
        cursor.close()
        conn.close()
        log.info("所有数据已成功入库。")

    except Exception as e:
        log.error(f"代码执行异常：{str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_detail()
