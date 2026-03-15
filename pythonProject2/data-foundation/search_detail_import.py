# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
搜索词历史数据导入脚本
参考 resource_total_history.py，将 search_behavior 事件中 eventParamName=search_content
的 2026 年数据逐日入库到 search_detail 表中，resource_name 统一填 'search_behavior'。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import aop
import aop.api
import pymysql
import traceback
import urllib.parse
from datetime import datetime, timedelta

# 友盟开放平台API配置
API_KEY = "更换为友盟API_KEY"        # 友盟开放平台 API Key
API_SECURITY = "更换为友盟API_SECRET"  # 友盟开放平台 API Secret

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',           # 数据库地址
    'port': 3306,                  # 数据库端口，默认 3306
    'user': 'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset': 'utf8mb4'
}

# 各平台(端) AppKey 配置
APPS = {
    '安卓': '更换为安卓端AppKey',
    '苹果': '更换为苹果端AppKey',
    '鸿蒙': '更换为鸿蒙端AppKey',
}


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


def fetch_and_store_search_history():
    """逐日拉取 search_behavior / search_content 数据并入库"""
    try:
        print("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 确保 search_detail 表存在
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

        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        dates_to_fetch = get_date_range('2026-01-01', yesterday)
        print(f"开始获取 2026-01-01 到 {yesterday} 的搜索词数据，共 {len(dates_to_fetch)} 天...")

        for target_date in dates_to_fetch:
            print(f"\n--- 获取日期: {target_date} ---")
            for port_name, appkey in APPS.items():

                # 检查该日期+端是否已有数据，避免重复入库
                check_sql = """
                    SELECT COUNT(*) FROM search_detail
                    WHERE stat_date = %s AND port = %s AND resource_name = 'search_behavior'
                """
                cursor.execute(check_sql, (target_date, port_name))
                existing = cursor.fetchone()[0]
                if existing > 0:
                    print(f"  [{port_name}] {target_date} 已有 {existing} 条数据，跳过。")
                    continue

                # 初始化请求
                req = aop.api.UmengUappEventParamGetValueListRequest()
                req.appkey = appkey
                req.startDate = target_date
                req.endDate = target_date
                req.eventName = 'search_behavior'
                req.eventParamName = 'search_content'

                try:
                    resp = req.get_response(None)
                except Exception as e:
                    print(f"  [{port_name}] 接口调用异常：{str(e)}")
                    continue

                if resp and resp.get("success") is False:
                    print(f"  [{port_name}] 接口失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
                    continue

                param_infos = resp.get("paramInfos", []) if resp else []
                if not param_infos:
                    print(f"  [{port_name}] 暂无数据。")
                    continue

                inserted = 0
                for item in param_infos:
                    raw_keyword = item.get("name", "")
                    count_val = item.get("count", 0)
                    decoded_keyword = decode_url_encoded_str(raw_keyword)

                    cursor.execute("""
                        INSERT INTO search_detail
                            (search_amount, search_name, stat_date, port, resource_name)
                        VALUES (%s, %s, %s, %s, 'search_behavior')
                    """, (count_val, decoded_keyword, target_date, port_name))
                    inserted += 1

                print(f"  [{port_name}] 入库 {inserted} 条搜索词数据。")

            # 每天提交一次，减少锁等待
            conn.commit()

        cursor.close()
        conn.close()
        print("\n[OK] 所有历史搜索词数据已成功入库。")

    except Exception as e:
        print(f"\n代码执行异常：{str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    fetch_and_store_search_history()
