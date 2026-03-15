# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import json
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
    'mid_banner',
    'news_click',
    'person_banner_click',
    'top_banner_click',
    'Hometopic_click'
]

def fetch_and_store_data():
    """获取各端事件数据并存入数据库"""
    try:
        # 连接数据库
        print("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 基础配置：设置网关域名和appinfo
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        # 获取昨天的日期
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"开始获取 {yesterday_date} 的数据...")
        print()

        for port_name, appkey in APPS.items():
            for event_name in EVENTS:
                # 初始化请求对象
                req = aop.api.UmengUappEventGetDataRequest()
                req.appkey = appkey
                req.startDate = yesterday_date
                req.endDate = yesterday_date
                req.eventName = event_name

                # 调用接口
                resp = req.get_response(None)
                
                # 校验响应是否成功
                if resp and resp.get("success") is False:
                    print(f"[{port_name}] {event_name} 接口调用失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
                    continue

                # 提取数据，处理数据不存在或为空的情况
                resource_amount = 0
                if resp and "eventData" in resp:
                    event_data = resp["eventData"]
                    if event_data and len(event_data) > 0:
                        data_list = event_data[0].get("data", [])
                        if data_list and len(data_list) > 0:
                            resource_amount = data_list[0]
                
                print(f"[{port_name}] 事件: {event_name} | 日期: {yesterday_date} | 数量: {resource_amount}")

                # 插入数据到数据库
                sql = """
                    INSERT INTO resource_total (resource_amount, resource_name, stat_date, port)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(sql, (resource_amount, event_name, yesterday_date, port_name))

        # 提交事务
        conn.commit()
        cursor.close()
        conn.close()
        print("\n所有数据已成功入库。")

    except Exception as e:
        print(f"\n代码执行异常：{str(e)}")
        # 打印完整异常栈方便排查
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_data()
