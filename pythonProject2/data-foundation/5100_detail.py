# -*- coding: utf-8 -*-
#!/usr/bin/env python
import aop
import aop.api
import json
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
    except Exception as e:
        print(f"解码失败：{e}，返回原字符串")
        return encoded_str

def fetch_and_store_detail():
    """获取各端510100_items各子服务事件数据并存入数据库"""
    try:
        print("正在连接数据库...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 确保表如果不存在则创建，以防万一
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS 5100_detail (
                id INT AUTO_INCREMENT PRIMARY KEY,
                service_amount BIGINT,
                resource_name VARCHAR(255),
                service_name VARCHAR(255),
                stat_date DATE,
                port VARCHAR(50)
            )
        ''')
        conn.commit()

        # 基础配置：设置网关域名和appinfo
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        # 获取昨天的日期
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"开始获取 {yesterday_date} 的 510100_items 数据...")
        print()

        resource_name = "510100_items"
        event_param_name = "service_name"

        for port_name, appkey in APPS.items():
            print(f"--- 正在拉取 [{port_name}] 端数据 ---")
            
            # 初始化请求对象
            req = aop.api.UmengUappEventParamGetValueListRequest()
            req.appkey = appkey
            req.startDate = yesterday_date
            req.endDate = yesterday_date
            req.eventName = resource_name
            req.eventParamName = event_param_name

            # 调用接口
            resp = req.get_response(None)
            
            # 校验响应是否成功
            if resp and resp.get("success") is False:
                print(f"[{port_name}] 接口调用失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
                continue

            param_infos = resp.get("paramInfos", [])
            if not param_infos:
                print(f"[{port_name}] 暂无数据或 paramInfos 为空。")
                continue

            # 处理数据并入库
            inserted_count = 0
            for item in param_infos:
                raw_name = item.get("name", "")
                count = item.get("count", 0)
                
                # 解码子服务名称
                decoded_name = decode_url_encoded_str(raw_name)

                sql = """
                    INSERT INTO 5100_detail (service_amount, resource_name, service_name, stat_date, port)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (count, resource_name, decoded_name, yesterday_date, port_name))
                inserted_count += 1

            print(f"[{port_name}] 成功入库 {inserted_count} 条子服务数据。")
            print()

        # 提交事务
        conn.commit()
        cursor.close()
        conn.close()
        print("所有数据已成功入库。")

    except Exception as e:
        print()
        print(f"代码执行异常：{str(e)}")
        # 打印完整异常栈方便排查
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_store_detail()
