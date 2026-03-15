# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import pymysql
import traceback
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

# 需要统计的事件名称列表
EVENTS = [
    # 'mid_banner',
    # 'news_click',
    # 'person_banner_click',
    # 'top_banner_click',
    # 'Hometopic_click',
    'search_behavior'
]

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
        print("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        dates_to_fetch = get_date_range('2026-01-01', '2026-03-12')
        print(f"开始获取 {dates_to_fetch[0]} 到 {dates_to_fetch[-1]} 的历史数据...")

        for target_date in dates_to_fetch:
            print(f"\\n--- 获取日期: {target_date} ---")
            for port_name, appkey in APPS.items():
                for event_name in EVENTS:
                    # 检查是否已存在完全相同的行
                    check_sql = "SELECT 1 FROM resource_total WHERE resource_name = %s AND stat_date = %s AND port = %s"
                    cursor.execute(check_sql, (event_name, target_date, port_name))
                    if cursor.fetchone():
                        print(f"[{port_name}] 事件: {event_name} | 日期: {target_date} | 数据库中已存在，跳过。")
                        continue

                    # 初始化请求对象
                    req = aop.api.UmengUappEventGetDataRequest()
                    req.appkey = appkey
                    req.startDate = target_date
                    req.endDate = target_date
                    req.eventName = event_name

                    try:
                        resp = req.get_response(None)
                    except Exception as e:
                        print(f"[{port_name}] {event_name} 接口调用异常：{str(e)}")
                        continue

                    if resp and resp.get("success") is False:
                        print(f"[{port_name}] {event_name} 接口调用失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
                        continue

                    resource_amount = 0
                    if resp and "eventData" in resp:
                        event_data = resp["eventData"]
                        if event_data and len(event_data) > 0:
                            data_list = event_data[0].get("data", [])
                            if data_list and len(data_list) > 0:
                                resource_amount = data_list[0]
                    
                    print(f"[{port_name}] 事件: {event_name} | 日期: {target_date} | 数量: {resource_amount}")

                    sql = """
                        INSERT INTO resource_total (resource_amount, resource_name, stat_date, port)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(sql, (resource_amount, event_name, target_date, port_name))
            
            # 每天提交一次数据
            conn.commit()

        cursor.close()
        conn.close()
        print("\\n所有历史数据已成功入库。")

    except Exception as e:
        print(f"\\n代码执行异常：{str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_history_data()