# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import json
import pymysql
import sys
import os
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
        UMENG_APP_APPKEYS  as PLATFORMS,
    )
except ImportError:
    API_KEY = "更换为友盟API_KEY"
    API_SECURITY = "更换为友盟API_SECRET"
    PLATFORMS = {
        "苹果": "更换为苹果端AppKey",
        "安卓": "更换为安卓端AppKey",
        "鸿蒙": "更换为鸿蒙端AppKey",
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
            os.path.join(LOG_DIR, f"fetch_retention_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

def fetch_and_store_retention():
    """获取各端留存数据并存入数据库"""
    try:
        log.info("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 基础配置
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        # 留存通常获取的是几周/几天前的，这里可以根据需要调整
        # 获取昨天的日期
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        log.info(f"开始获取 {yesterday_date} 的留存数据...")

        for port_name, appkey in PLATFORMS.items():
            log.info(f"--- 正在拉取 [{port_name}] 端留存数据 ---")
            
            req = aop.api.UmengUappGetRetentionsRequest()
            req.appkey = appkey
            req.startDate = yesterday_date
            req.endDate = yesterday_date
            req.periodType = 'daily'

            resp = req.get_response(None)
            
            if resp and resp.get("success") is False:
                log.error(f"[{port_name}] 接口调用失败：{resp.get('errorMsg')}")
                continue

            retention_info = resp.get("retentionInfo", [])
            for info in retention_info:
                # 解析留存数据并入库
                # 注意：不同接口返回结构不同，需根据友盟文档调整
                # 示例逻辑：
                date = info.get("date")
                total_install = info.get("totalInstall", 0)
                retention_rates = info.get("retentionRate", [])
                
                # 此处省略具体入库逻辑，需根据表结构设计
                log.info(f"[{port_name}] {date} 留存拉取成功")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        log.error(f"代码执行异常：{str(e)}")

if __name__ == "__main__":
    fetch_and_store_retention()
