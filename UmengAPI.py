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
import logging
from datetime import datetime, timedelta
import urllib.parse

# 设置输出编码为UTF-8（解决Windows控制台中文显示问题）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ── 统一配置加载 ─────────────────────────────────────────────
try:
    from config import (
        DB_CONFIG,
        UMENG_API_KEY      as API_KEY,
        UMENG_API_SECRET   as API_SECURITY,
        UMENG_APP_APPKEYS  as PLATFORM_APPKEYS,
        UMENG_MINI_APPKEYS as MINI_PROGRAM_APPKEYS,
    )
except ImportError:
    # 友盟开放平台 API 认证信息
    API_KEY = "更换为友盟API_KEY"
    API_SECURITY = "更换为友盟API_SECRET"
    PLATFORM_APPKEYS = {
        "安卓": "更换为安卓端AppKey",
        "苹果": "更换为苹果端AppKey",
        "鸿蒙": "更换为鸿蒙端AppKey",
    }
    MINI_PROGRAM_APPKEYS = {
        "微信小程序": "更换为微信小程序DataSourceId",
        "支付宝小程序": "更换为支付宝小程序DataSourceId",
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
            os.path.join(LOG_DIR, f"umeng_api_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# 昨日日期
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ====================== 2. 通用工具函数 ======================
def decode_url_encoded_str(encoded_str):
    """通用URL解码函数，处理中文编码"""
    if not isinstance(encoded_str, str):
        return encoded_str
    try:
        return urllib.parse.unquote(encoded_str, encoding="utf-8")
    except Exception as e:
        log.warning(f"解码失败：{e}，返回原字符串")
        return encoded_str


# ====================== 3. 核心功能：获取单个平台日活 ======================
def get_platform_dau(platform_name, appkey, date):
    """获取指定平台指定日期的日活数据（原生APP）"""
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
            log.error(f"【{platform_name}】接口调用失败：{resp.get('errorMsg')}（错误码：{resp.get('errorCode')}）")
            return None

        active_user_info = resp.get("activeUserInfo", [])

        if not active_user_info:
            log.info(f"【{platform_name}】{date} 无数据")
            return 0

        for info in active_user_info:
            if info.get("date") == date:
                dau = info.get("value", 0)
                log.info(f"✅ 【{platform_name}】{date} 日活：{dau:,}")
                return dau

        log.info(f"【{platform_name}】未找到{date}的数据")
        return 0

    except Exception as e:
        log.error(f"【{platform_name}】异常：{e}")
        return None


def get_mini_program_dau(program_name, appkey, date):
    """获取指定小程序指定日期的日活数据"""
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
            log.error(f"【{program_name}】接口调用失败：{resp.get('msg')}")
            return None

        data_list = resp.get("data", {}).get("data", [])

        if not data_list:
            log.info(f"【{program_name}】{date} 无数据")
            return 0

        for item in data_list:
            if item.get("dateTime") == date:
                dau = item.get("activeUser", 0)
                log.info(f"✅ 【{program_name}】{date} 日活：{dau:,}")
                return dau

        log.info(f"【{program_name}】未找到{date}的数据")
        return 0

    except Exception as e:
        log.error(f"【{program_name}】异常：{e}")
        return None


# ====================== 4. 获取所有平台日活数据 ======================
def get_all_platforms_dau(date=None):
    """获取所有平台的日活数据"""
    if date is None:
        date = YESTERDAY

    log.info("=" * 60)
    log.info(f"全平台日活数据获取 | 查询日期：{date}")
    log.info("=" * 60)

    results = {}

    for platform_name, appkey in PLATFORM_APPKEYS.items():
        dau = get_platform_dau(platform_name, appkey, date)
        results[platform_name] = dau

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

    return result


# ====================== 5. 数据入库功能 ======================
def save_to_database(data):
    """将日活数据入库到MySQL数据库"""
    try:
        import pymysql

        conn = pymysql.connect(**DB_CONFIG)
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
            log.info(f"   更新记录: {query_date}")
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
            log.info(f"   新增记录: {query_date}")

        conn.commit()
        log.info(f"✅ 数据成功入库到MySQL")
        cursor.close()
        conn.close()

    except Exception as e:
        log.error(f"❌ 入库失败：{e}")


# ====================== 6. 主函数 ======================
def main():
    try:
        result = get_all_platforms_dau()
        save_to_database(result)
        return result
    except Exception as e:
        log.error(f"❌ 程序执行失败：{e}")
        return None


if __name__ == "__main__":
    main()
