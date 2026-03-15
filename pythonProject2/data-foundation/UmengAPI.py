# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
UmengAPI.py - 全平台日活数据获取与入库
========================================
通过友盟 Open API 获取以下平台的昨日日活数据并写入 MySQL：
- 原生APP：安卓、苹果、鸿蒙
- 小程序：微信小程序、支付宝小程序
"""
import aop
import aop.api
import json
import sys
from datetime import datetime, timedelta
import urllib.parse

# 设置输出编码为UTF-8（解决Windows控制台中文显示问题）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ====================== 1. 友盟API配置 ======================
# 友盟开放平台 API 认证信息，登录 https://developer.umeng.com/ 获取
API_KEY = "更换为友盟API_KEY"        # 友盟开放平台 API Key
API_SECURITY = "更换为友盟API_SECRET"  # 友盟开放平台 API Secret

# 原生APP各端的 AppKey（在友盟控制台应用列表中查看）
PLATFORM_APPKEYS = {
    "安卓": "更换为安卓端AppKey",
    "苹果": "更换为苹果端AppKey",
    "鸿蒙": "更换为鸿蒙端AppKey",
}

# 小程序的 DataSourceId（在友盟控制台小程序应用中查看）
MINI_PROGRAM_APPKEYS = {
    "微信小程序": "更换为微信小程序DataSourceId",
    "支付宝小程序": "更换为支付宝小程序DataSourceId",
}

# 昨日日期（用于获取昨日日活）
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ====================== 2. 通用工具函数 ======================
def decode_url_encoded_str(encoded_str):
    """通用URL解码函数，处理中文编码"""
    if not isinstance(encoded_str, str):
        return encoded_str
    try:
        return urllib.parse.unquote(encoded_str, encoding="utf-8")
    except Exception as e:
        print(f"解码失败：{e}，返回原字符串")
        return encoded_str


# ====================== 3. 核心功能：获取单个平台日活 ======================
def get_platform_dau(platform_name, appkey, date):
    """
    获取指定平台指定日期的日活数据（原生APP）

    :param platform_name: 平台名称（安卓/苹果/鸿蒙）
    :param appkey: 应用appkey
    :param date: 查询日期（格式：YYYY-MM-DD）
    :return: 日活数值，失败返回None
    """
    try:
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        req = aop.api.UmengUappGetActiveUsersRequest()
        resp = req.get_response(
            None,
            appkey=appkey,
            startDate=date,
            endDate=date,
            periodType="daily"
        )

        if resp.get("success") is False:
            print(f"【{platform_name}】接口调用失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
            return None

        active_user_info = resp.get("activeUserInfo", [])

        if not active_user_info:
            print(f"【{platform_name}】{date} 无数据")
            return 0

        for info in active_user_info:
            if info.get("date") == date:
                dau = info.get("value", 0)
                print(f"✅ 【{platform_name}】{date} 日活：{dau:,}")
                return dau

        print(f"【{platform_name}】未找到{date}的数据")
        return 0

    except aop.ApiError as e:
        print(f"【{platform_name}】API网关异常：{e}")
        return None
    except aop.AopError as e:
        print(f"【{platform_name}】客户端异常：{e}")
        return None
    except Exception as e:
        print(f"【{platform_name}】未知异常：{e}")
        import traceback
        traceback.print_exc()
        return None


def get_mini_program_dau(program_name, appkey, date):
    """
    获取指定小程序指定日期的日活数据

    :param program_name: 小程序名称（微信小程序/支付宝小程序）
    :param appkey: 数据源ID（dataSourceId）
    :param date: 查询日期（格式：YYYY-MM-DD）
    :return: 日活数值，失败返回None
    """
    try:
        aop.set_default_server('gateway.open.umeng.com')
        aop.set_default_appinfo(API_KEY, API_SECURITY)

        req = aop.api.UmengUminiGetOverviewRequest()
        resp = req.get_response(
            None,
            dataSourceId=appkey,
            fromDate=date,
            toDate=date,
            timeUnit="day",
            indicators="activeUser"
        )

        if not resp.get("success"):
            print(f"【{program_name}】接口调用失败：{resp.get('msg')}")
            return None

        data_list = resp.get("data", {}).get("data", [])

        if not data_list:
            print(f"【{program_name}】{date} 无数据")
            return 0

        for item in data_list:
            if item.get("dateTime") == date:
                dau = item.get("activeUser", 0)
                print(f"✅ 【{program_name}】{date} 日活：{dau:,}")
                return dau

        print(f"【{program_name}】未找到{date}的数据")
        return 0

    except aop.ApiError as e:
        print(f"【{program_name}】API网关异常：{e}")
        return None
    except aop.AopError as e:
        print(f"【{program_name}】客户端异常：{e}")
        return None
    except Exception as e:
        print(f"【{program_name}】未知异常：{e}")
        import traceback
        traceback.print_exc()
        return None


# ====================== 4. 获取所有平台日活数据 ======================
def get_all_platforms_dau(date=None):
    """
    获取所有平台的日活数据：安卓、苹果、鸿蒙APP + 微信小程序、支付宝小程序

    :param date: 查询日期，默认为昨日
    :return: 包含所有平台日活的字典
    """
    if date is None:
        date = YESTERDAY

    print("=" * 60)
    print(f"全平台日活数据获取")
    print(f"查询日期：{date}")
    print("=" * 60)

    results = {}

    print("\n【原生APP平台】")
    for platform_name, appkey in PLATFORM_APPKEYS.items():
        dau = get_platform_dau(platform_name, appkey, date)
        results[platform_name] = dau

    print("\n【小程序平台】")
    for program_name, appkey in MINI_PROGRAM_APPKEYS.items():
        dau = get_mini_program_dau(program_name, appkey, date)
        results[program_name] = dau

    app_total_dau = sum([results.get("安卓", 0) or 0,
                         results.get("苹果", 0) or 0,
                         results.get("鸿蒙", 0) or 0])

    all_values = [v for v in results.values() if v is not None and v > 0]
    platform_total_dau = sum(all_values) if all_values else 0

    result = {
        "date": date,
        "android_dau": results.get("安卓"),
        "ios_dau": results.get("苹果"),
        "harmony_dau": results.get("鸿蒙"),
        "app_dau": app_total_dau if app_total_dau > 0 else 0,
        "mini_program_dau": results.get("微信小程序"),
        "alipay_dau": results.get("支付宝小程序"),
        "platform_total_dau": platform_total_dau,
        "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    print("\n" + "=" * 60)
    print("【汇总结果】")
    print("=" * 60)

    print("\nAPP平台：")
    for platform_name in ["安卓", "苹果", "鸿蒙"]:
        dau = results.get(platform_name)
        if dau is not None:
            print(f"  ✅ {platform_name}APP日活: {dau:,}")
        else:
            print(f"  ❌ {platform_name}APP日活: 获取失败")

    if app_total_dau > 0:
        print(f"  📊 APP总日活: {app_total_dau:,}")

    print("\n小程序平台：")
    for program_name in ["微信小程序", "支付宝小程序"]:
        dau = results.get(program_name)
        if dau is not None:
            print(f"  ✅ {program_name}日活: {dau:,}")
        else:
            print(f"  ❌ {program_name}日活: 获取失败")

    if platform_total_dau > 0:
        print(f"\n🎯 全平台总日活: {platform_total_dau:,}")

    return result


# ====================== 5. 数据入库功能（MySQL） ======================
# MySQL数据库配置，请根据实际环境修改
DB_CONFIG = {
    'host': 'localhost',           # 数据库地址
    'port': 3306,                  # 数据库端口，默认 3306
    'user': 'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset': 'utf8mb4'
}

def save_to_database(data):
    """
    将日活数据入库到MySQL数据库
    采用安全策略：只更新友盟相关字段，不影响其他字段（如爬虫数据）

    :param data: 包含日活数据的字典
    """
    try:
        import pymysql

        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset=DB_CONFIG['charset']
        )
        cursor = conn.cursor()

        query_date = data.get("date")
        android_dau = data.get("android_dau") or 0
        ios_dau = data.get("ios_dau") or 0
        harmonyos_dau = data.get("harmony_dau") or 0
        app_dau = data.get("app_dau") or 0
        mini_program_dau = data.get("mini_program_dau") or 0
        alipay_dau = data.get("alipay_dau") or 0

        cursor.execute(
            "SELECT COUNT(*) FROM platform_daily_metrics WHERE stat_date = %s",
            (query_date,)
        )
        exists = cursor.fetchone()[0] > 0

        if exists:
            sql = """
                UPDATE platform_daily_metrics
                SET android_dau = %s,
                    ios_dau = %s,
                    harmonyos_dau = %s,
                    app_dau = %s,
                    mini_program_dau = %s,
                    alipay_dau = %s
                WHERE stat_date = %s
            """
            cursor.execute(sql, (android_dau, ios_dau, harmonyos_dau, app_dau,
                                mini_program_dau, alipay_dau, query_date))
            print(f"   更新已存在记录的友盟字段: {query_date}")
        else:
            sql = """
                INSERT INTO platform_daily_metrics
                    (stat_date, android_dau, ios_dau, harmonyos_dau, app_dau,
                     mini_program_dau, alipay_dau)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (query_date, android_dau, ios_dau, harmonyos_dau,
                                app_dau, mini_program_dau, alipay_dau))
            print(f"   新增记录: {query_date}")

        conn.commit()

        print(f"\n✅ 数据成功入库到MySQL")
        print(f"   数据库：{DB_CONFIG['database']}")
        print(f"   表名：platform_daily_metrics")
        print(f"   日期：{query_date}")
        print(f"   安卓日活：{android_dau:,}")
        print(f"   苹果日活：{ios_dau:,}")
        print(f"   鸿蒙日活：{harmonyos_dau:,}")
        print(f"   APP总日活：{app_dau:,}")
        print(f"   微信小程序日活：{mini_program_dau:,}")
        print(f"   支付宝小程序日活：{alipay_dau:,}")
        print(f"   ℹ️  注意：爬虫字段（new_register_users等）未被修改")

        cursor.close()
        conn.close()

    except ImportError:
        print(f"\n❌ 缺少 pymysql 模块，请安装：pip install pymysql")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ 入库失败：{e}")
        import traceback
        traceback.print_exc()


# ====================== 6. 主函数 ======================
def main():
    """主函数：获取全平台昨日日活（APP+小程序）→ 结构化输出 → 入库MySQL"""
    try:
        result = get_all_platforms_dau()

        print("\n" + "=" * 60)
        print("【JSON格式输出】")
        print("=" * 60)
        print(json.dumps(result, ensure_ascii=False, indent=2))

        save_to_database(result)

        return result

    except Exception as e:
        print(f"\n❌ 程序执行失败：{e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
