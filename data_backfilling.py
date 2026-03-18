#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_backfilling.py
检查 daily 数据库近7天漏填数据并自动回填。

覆盖数据源：
  [1] 友盟 API      → platform_daily_metrics: android/ios/harmony/app/mini/alipay DAU
  [2] 友盟 API      → resource_total:         各端事件数据
  [3] 友盟 API      → 5100_detail:            510100_items 子服务明细
  [4] 智能前端爬虫  → platform_daily_metrics: smart_frontend_dau
  [5] 内网爬虫      → platform_daily_metrics: new_register_users / new_realname_users
"""

import os
import sys
import time
import glob
import random
import re
import json
import logging
import datetime
import traceback
import urllib.parse

import pymysql
import pandas as pd

# ── 输出编码（Windows 控制台中文）─────────────────────────────
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ════════════════════════════════════════════════════════════
# 常量配置（优先从 config.py 加载，参考 config.example.py）
# ════════════════════════════════════════════════════════════

try:
    from config import (
        DB_CONFIG,
        UMENG_API_KEY      as API_KEY,
        UMENG_API_SECRET   as API_SECURITY,
        UMENG_APP_APPKEYS  as PLATFORM_APPKEYS,
        UMENG_MINI_APPKEYS as MINI_PROGRAM_APPKEYS,
        FRONTEND_LOGIN_URL      as SMART_LOGIN_URL,
        FRONTEND_LOGIN_USERNAME as SMART_USERNAME,
        FRONTEND_LOGIN_PASSWORD as SMART_PASSWORD,
        FRONTEND_CHROME_PORT    as SMART_PORT,
        FRONTEND_CHROME_PROFILE as SMART_PROFILE,
        FRONTEND_DOWNLOAD_DIR   as SMART_DOWNLOAD_DIR,
        FRONTEND_DOWNLOAD_ROOT  as SMART_DOWNLOAD_ROOT,
        FRONTEND_DEBUG_DIR      as SMART_DEBUG_DIR,
        INTRANET_LOGIN_URL      as INTERNAL_URL,
        INTRANET_LOGIN_USERNAME as INTERNAL_USERNAME,
        INTRANET_LOGIN_PASSWORD as INTERNAL_PASSWORD,
        INTRANET_CHROME_PORT    as INTERNAL_PORT,
        INTRANET_CHROME_PROFILE as INTERNAL_PROFILE,
    )
except ImportError:
    # config.py 不存在时使用内联配置，请参考 config.example.py 创建 config.py
    DB_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '更换为自己的MySQL密码',
        'database': 'daily',
        'charset': 'utf8mb4',
    }

    # 友盟 API
    API_KEY      = "更换为友盟API_KEY"
    API_SECURITY = "更换为友盟API_SECRET"

    PLATFORM_APPKEYS = {
        '安卓': '更换为安卓端AppKey',
        '苹果': '更换为苹果端AppKey',
        '鸿蒙': '更换为鸿蒙端AppKey',
    }
    MINI_PROGRAM_APPKEYS = {
        '微信小程序':  '更换为微信小程序DataSourceId',
        '支付宝小程序':'更换为支付宝小程序DataSourceId',
    }

    # 智能前端爬虫
    SMART_LOGIN_URL    = '更换为智能前端系统登录地址'
    SMART_USERNAME     = '更换为系统账号'
    SMART_PASSWORD     = '更换为系统密码'
    SMART_PORT         = 9335
    SMART_PROFILE      = r'C:\Users\YOUR_USERNAME\AppData\Local\smart_spider_chrome'
    SMART_DOWNLOAD_DIR = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau'
    SMART_DOWNLOAD_ROOT= r'C:\Users\YOUR_USERNAME\Downloads'
    SMART_DEBUG_DIR    = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau\debug'

    # 内网爬虫
    INTERNAL_URL      = "更换为内网系统登录地址"
    INTERNAL_USERNAME = "更换为内网系统账号"
    INTERNAL_PASSWORD = "更换为内网系统密码"
    INTERNAL_PORT     = 9336
    INTERNAL_PROFILE  = r'C:\Users\YOUR_USERNAME\AppData\Local\internal_spider_chrome'

RESOURCE_EVENTS = [
    'mid_banner', 'news_click', 'person_banner_click',
    'top_banner_click', 'Hometopic_click',
]

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ── 日志 ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"backfill_{datetime.datetime.now().strftime('%Y%m%d')}.log"),
            encoding='utf-8'
        ),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# Step 1 — 缺失检查
# ════════════════════════════════════════════════════════════

def get_missing_dates() -> dict:
    """
    检查近 7 天（不含今日）各数据源的缺失情况。

    返回:
        {
          'umeng_dau':       [dates...],   # android/ios/harmony/mini/alipay 任一 NULL
          'smart_frontend':  [dates...],   # smart_frontend_dau 为 NULL
          'internal_network':[dates...],   # new_register_users 或 new_realname_users 为 NULL
          'resource_total':  [dates...],   # resource_total 表该日期无记录
          '5100_detail':     [dates...],   # 5100_detail 表该日期无记录
        }
    """
    today       = datetime.date.today()
    check_dates = [
        (today - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(1, 8)
    ]
    missing = {
        'umeng_dau':        [],
        'smart_frontend':   [],
        'internal_network': [],
        'resource_total':   [],
        '5100_detail':      [],
    }

    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        for d in check_dates:
            # ── platform_daily_metrics ──────────────────────────
            cursor.execute(
                "SELECT android_dau, ios_dau, harmonyos_dau, "
                "mini_program_dau, alipay_dau, "
                "smart_frontend_dau, new_register_users, new_realname_users "
                "FROM platform_daily_metrics WHERE stat_date = %s",
                (d,)
            )
            row = cursor.fetchone()
            if row is None:
                # 整行不存在，三类来源都缺失
                missing['umeng_dau'].append(d)
                missing['smart_frontend'].append(d)
                missing['internal_network'].append(d)
            else:
                android, ios, harmony, mini, alipay, sf, reg, real = row
                if any(v is None for v in [android, ios, harmony, mini, alipay]):
                    missing['umeng_dau'].append(d)
                if sf is None:
                    missing['smart_frontend'].append(d)
                if reg is None or real is None:
                    missing['internal_network'].append(d)

            # ── resource_total ──────────────────────────────────
            cursor.execute(
                "SELECT COUNT(*) FROM resource_total WHERE stat_date = %s", (d,)
            )
            if cursor.fetchone()[0] == 0:
                missing['resource_total'].append(d)

            # ── 5100_detail ─────────────────────────────────────
            cursor.execute(
                "SELECT COUNT(*) FROM `5100_detail` WHERE stat_date = %s", (d,)
            )
            if cursor.fetchone()[0] == 0:
                missing['5100_detail'].append(d)

        cursor.close()
    finally:
        conn.close()

    return missing


# ════════════════════════════════════════════════════════════
# Step 2 — 友盟 API 回填
# ════════════════════════════════════════════════════════════

def _umeng_init():
    import aop
    aop.set_default_server('gateway.open.umeng.com')
    aop.set_default_appinfo(API_KEY, API_SECURITY)
    return aop


def _decode(s):
    if not isinstance(s, str):
        return s
    try:
        return urllib.parse.unquote(s, encoding='utf-8')
    except Exception:
        return s


# ── 2-A: 平台 DAU ────────────────────────────────────────────

def backfill_umeng_dau(dates: list):
    """回填 android / ios / harmony / mini_program / alipay DAU"""
    if not dates:
        return
    log.info(f"[Umeng DAU] 回填 {len(dates)} 天: {dates}")

    import aop
    import aop.api
    _umeng_init()

    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        for d in dates:
            log.info(f"  处理日期: {d}")
            vals = {}

            # 原生 APP（安卓 / 苹果 / 鸿蒙）
            for name, appkey in PLATFORM_APPKEYS.items():
                try:
                    req = aop.api.UmengUappGetActiveUsersRequest()
                    req.appkey     = appkey
                    req.startDate  = d
                    req.endDate    = d
                    req.periodType = 'daily'
                    resp = req.get_response(None)
                    dau = 0
                    if resp.get('success') is not False:
                        for info in resp.get('activeUserInfo', []):
                            if info.get('date') == d:
                                dau = info.get('value', 0)
                                break
                    vals[name] = dau
                    log.info(f"    {name} DAU = {dau:,}")
                except Exception as e:
                    log.warning(f"    {name} 获取失败: {e}")
                    vals[name] = None

            # 小程序（微信 / 支付宝）
            for name, ds_id in MINI_PROGRAM_APPKEYS.items():
                try:
                    req = aop.api.UmengUminiGetOverviewRequest()
                    req.dataSourceId = ds_id
                    req.fromDate     = d
                    req.toDate       = d
                    req.timeUnit     = 'day'
                    req.indicators   = 'activeUser'
                    resp = req.get_response(None)
                    dau = 0
                    if resp.get('success'):
                        for item in resp.get('data', {}).get('data', []):
                            if item.get('dateTime') == d:
                                dau = item.get('activeUser', 0)
                                break
                    vals[name] = dau
                    log.info(f"    {name} DAU = {dau:,}")
                except Exception as e:
                    log.warning(f"    {name} 获取失败: {e}")
                    vals[name] = None

            android = vals.get('安卓') or 0
            ios     = vals.get('苹果') or 0
            harmony = vals.get('鸿蒙') or 0
            app_dau = android + ios + harmony
            mini    = vals.get('微信小程序') or 0
            alipay  = vals.get('支付宝小程序') or 0

            cursor.execute(
                "SELECT COUNT(*) FROM platform_daily_metrics WHERE stat_date = %s", (d,)
            )
            if cursor.fetchone()[0] > 0:
                cursor.execute(
                    "UPDATE platform_daily_metrics "
                    "SET android_dau=%s, ios_dau=%s, harmonyos_dau=%s, app_dau=%s, "
                    "mini_program_dau=%s, alipay_dau=%s "
                    "WHERE stat_date=%s",
                    (android, ios, harmony, app_dau, mini, alipay, d)
                )
            else:
                cursor.execute(
                    "INSERT INTO platform_daily_metrics "
                    "(stat_date, android_dau, ios_dau, harmonyos_dau, app_dau, "
                    "mini_program_dau, alipay_dau) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (d, android, ios, harmony, app_dau, mini, alipay)
                )
            conn.commit()
            log.info(f"  [{d}] Umeng DAU 入库完成")

        cursor.close()
    finally:
        conn.close()


# ── 2-B: resource_total ──────────────────────────────────────

def backfill_resource_total(dates: list):
    """回填 resource_total 表（各端事件数据）"""
    if not dates:
        return
    log.info(f"[resource_total] 回填 {len(dates)} 天: {dates}")

    import aop
    import aop.api
    _umeng_init()

    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        for d in dates:
            log.info(f"  处理日期: {d}")
            for port_name, appkey in PLATFORM_APPKEYS.items():
                for event_name in RESOURCE_EVENTS:
                    try:
                        req = aop.api.UmengUappEventGetDataRequest()
                        req.appkey    = appkey
                        req.startDate = d
                        req.endDate   = d
                        req.eventName = event_name
                        resp = req.get_response(None)
                        amount = 0
                        if resp and resp.get('success') is not False:
                            event_data = resp.get('eventData', [])
                            if event_data:
                                data_list = event_data[0].get('data', [])
                                if data_list:
                                    amount = data_list[0]
                        cursor.execute(
                            "INSERT INTO resource_total "
                            "(resource_amount, resource_name, stat_date, port) "
                            "VALUES (%s, %s, %s, %s)",
                            (amount, event_name, d, port_name)
                        )
                        log.info(f"    [{port_name}] {event_name} = {amount}")
                    except Exception as e:
                        log.warning(f"    [{port_name}] {event_name} 失败: {e}")
            conn.commit()
            log.info(f"  [{d}] resource_total 入库完成")

        cursor.close()
    finally:
        conn.close()


# ── 2-C: 5100_detail ─────────────────────────────────────────

def backfill_5100_detail(dates: list):
    """回填 5100_detail 表（510100_items 子服务明细）"""
    if not dates:
        return
    log.info(f"[5100_detail] 回填 {len(dates)} 天: {dates}")

    import aop
    import aop.api
    _umeng_init()

    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        for d in dates:
            log.info(f"  处理日期: {d}")
            for port_name, appkey in PLATFORM_APPKEYS.items():
                try:
                    req = aop.api.UmengUappEventParamGetValueListRequest()
                    req.appkey         = appkey
                    req.startDate      = d
                    req.endDate        = d
                    req.eventName      = '510100_items'
                    req.eventParamName = 'service_name'
                    resp = req.get_response(None)
                    if resp and resp.get('success') is False:
                        log.warning(
                            f"    [{port_name}] 接口失败: {resp.get('errorMsg')}"
                        )
                        continue
                    param_infos = resp.get('paramInfos', [])
                    for item in param_infos:
                        raw_name = item.get('name', '')
                        count    = item.get('count', 0)
                        decoded  = _decode(raw_name)
                        cursor.execute(
                            "INSERT INTO `5100_detail` "
                            "(service_amount, resource_name, service_name, stat_date, port) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            (count, '510100_items', decoded, d, port_name)
                        )
                    log.info(f"    [{port_name}] 入库 {len(param_infos)} 条")
                except Exception as e:
                    log.warning(f"    [{port_name}] 失败: {e}")
            conn.commit()
            log.info(f"  [{d}] 5100_detail 入库完成")

        cursor.close()
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# Step 3 — 智能前端日活回填（浏览器下载 Excel）
# ════════════════════════════════════════════════════════════

def _find_chrome_path():
    paths = [
        r'C:\Program Files\Google\Chrome\Application\chrome.exe',
        r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
        os.path.expandvars(r'%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe'),
    ]
    for p in paths:
        if os.path.exists(p):
            return p
    return None


def _kill_chrome_on_port(port: int):
    import subprocess
    try:
        res = subprocess.run(['netstat', '-ano'], capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if f':{port} ' in line and 'LISTENING' in line:
                pid = line.strip().split()[-1]
                if pid.isdigit():
                    subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True)
    except Exception:
        pass


def _inject_chrome_permissions(profile_dir: str, site_key: str):
    pref_dir  = os.path.join(profile_dir, 'Default')
    pref_path = os.path.join(pref_dir, 'Preferences')
    os.makedirs(pref_dir, exist_ok=True)
    prefs = {}
    if os.path.exists(pref_path):
        try:
            with open(pref_path, encoding='utf-8') as f:
                prefs = json.load(f)
        except Exception:
            pass
    exceptions = (prefs
                  .setdefault('profile', {})
                  .setdefault('content_settings', {})
                  .setdefault('exceptions', {}))
    for k in ('local_network', 'loopback_network'):
        exceptions.setdefault(k, {})[site_key] = {'setting': 1}
    try:
        with open(pref_path, 'w', encoding='utf-8') as f:
            json.dump(prefs, f, ensure_ascii=False)
    except Exception:
        pass


def _clean_smart_download():
    for pat in ('*.xlsx', '*.xls', '*.crdownload'):
        for f in glob.glob(os.path.join(SMART_DOWNLOAD_DIR, pat)):
            try:
                os.remove(f)
            except Exception:
                pass


def _create_smart_page():
    from DrissionPage import ChromiumPage, ChromiumOptions
    for d in (SMART_DOWNLOAD_DIR, SMART_DEBUG_DIR, SMART_PROFILE):
        os.makedirs(d, exist_ok=True)

    co = ChromiumOptions()
    bp = _find_chrome_path()
    if bp:
        co.set_browser_path(bp)
    co.set_user_agent(
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    )
    co.set_local_port(SMART_PORT)
    co.set_user_data_path(SMART_PROFILE)
    co.set_pref('profile.managed_default_content_settings.images', 2)
    co.set_pref('download.prompt_for_download', False)
    co.set_pref('download.default_directory', SMART_DOWNLOAD_DIR)
    co.set_pref('download.directory_upgrade', True)
    co.set_pref('safebrowsing.enabled', True)
    co.set_pref('profile.default_content_setting_values.notifications', 1)
    co.set_pref('profile.default_content_setting_values.automatic_downloads', 1)
    for arg in (
        '--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage',
        '--disable-popup-blocking', '--start-maximized', '--no-first-run',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=PrivateNetworkAccessChecks,'
        'BlockInsecurePrivateNetworkRequests,OptimizationGuideFetching',
    ):
        co.set_argument(arg)
    co.set_download_path(SMART_DOWNLOAD_DIR)

    for i in range(3):
        try:
            page = ChromiumPage(addr_or_opts=co)
            page.run_js(
                'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
            )
            try:
                page.set.download_path(SMART_DOWNLOAD_DIR)
            except Exception:
                pass
            page.set.timeouts(30)
            return page
        except Exception as e:
            log.warning(f'智能前端浏览器启动失败 ({i+1}/3): {e}')
            _kill_chrome_on_port(SMART_PORT)
            time.sleep(5)
            if i == 2:
                raise e


def _smart_dismiss_popups(page):
    for sel in (
        'xpath://button[.//span[contains(text(),"允许")]]',
        'css:.el-message-box__btns button.el-button--primary',
        'xpath://button[normalize-space(.)="确定"]',
        'xpath://button[.//span[contains(normalize-space(),"确定")]]',
    ):
        try:
            btn = page.ele(sel, timeout=1)
            btn.click()
            time.sleep(1)
        except Exception:
            pass


def _smart_login(page):
    log.info('智能前端: 打开登录页...')
    _smart_dismiss_popups(page)
    page.set.headers({
        'Referer': SMART_LOGIN_URL.split('/#')[0] + '/',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
    })
    page.get(SMART_LOGIN_URL)
    try:
        page.wait.doc_loaded()
    except Exception:
        time.sleep(5)
    time.sleep(8)
    _smart_dismiss_popups(page)

    # 强制注销旧会话
    try:
        for sel in (
            'xpath://span[contains(text(), "退出")]',
            'xpath://a[contains(text(), "退出")]',
            'css:.log-out', 'css:.logout',
        ):
            btn = page.ele(sel, timeout=3)
            if btn:
                btn.click()
                time.sleep(3)
                _smart_dismiss_popups(page)
                time.sleep(10)
                page.get(SMART_LOGIN_URL)
                time.sleep(5)
                break
    except Exception:
        pass

    try:
        user_input = page.ele(
            'xpath://input[contains(@placeholder,"账号") or '
            'contains(@placeholder,"账户") or contains(@placeholder,"手机号")]',
            timeout=15
        )
        if user_input:
            user_input.input(SMART_USERNAME, clear=True)
            time.sleep(random.uniform(0.8, 1.5))
            pwd_input = page.ele(
                'xpath://input[@type="password" or contains(@placeholder,"密码")]',
                timeout=15
            )
            pwd_input.input(SMART_PASSWORD, clear=True)
            time.sleep(random.uniform(0.5, 1.0))
            login_btn = page.ele(
                'xpath://button[contains(@class,"el-button--primary")]'
                '//span[contains(text(),"登录")]/..',
                timeout=15
            )
            login_btn.click()
    except Exception as e:
        log.warning(f'智能前端登录填写异常: {e}')

    time.sleep(5)
    _smart_dismiss_popups(page)
    time.sleep(25)

    # 激活"应用→概览"菜单
    try:
        app_menu = page.ele(
            'xpath://li[@role="menuitem" and .//span[text()="应用"]]', timeout=30
        )
        if app_menu:
            app_menu.click()
            time.sleep(30)
            overview_menu = page.ele(
                'xpath://li[@role="menuitem" and .//span[text()="概览"]]', timeout=30
            )
            if overview_menu:
                overview_menu.click()
                time.sleep(35)
    except Exception as e:
        log.warning(f'智能前端菜单切换异常: {e}')


def _smart_export_excel(page):
    log.info('智能前端: 准备导出 Excel (30天)...')
    time.sleep(15)
    _smart_dismiss_popups(page)

    frame = None
    for _ in range(20):
        try:
            frame = page.get_frame('tag:iframe')
            if frame:
                break
        except Exception:
            pass
        time.sleep(3)
    target = frame if frame else page

    # 等待图表数据加载
    deadline = time.time() + 90
    while time.time() < deadline:
        card_bodies = target.eles('css:div.el-card__body')
        if any(
            len(cb.text.strip()) > 15 and '无数据' not in cb.text
            for cb in card_bodies
        ):
            break
        time.sleep(5)

    # UV 卡片 → 切换列表视图
    try:
        uv_card = target.ele(
            'xpath://div[contains(@class,"el-card") and .//*[contains(text(),"UV")]]',
            timeout=20
        )
        if uv_card:
            uv_card.run_js('this.scrollIntoView({block:"center"});')
            time.sleep(3)
            tickets = uv_card.ele('css:i.el-icon-tickets', timeout=15)
            tickets.run_js('this.click();')
            log.info('已切换至列表视图')
    except Exception as e:
        log.error(f'UV 卡片操作失败: {e}')

    time.sleep(5)
    try:
        radio_30 = target.ele('css:input[value="30days"]', index=2, timeout=15)
        if radio_30:
            radio_30.run_js('this.click();')
            time.sleep(10)
        export_btn = target.ele('css:button[title="导出Excel"]', index=1, timeout=20)
        export_btn.run_js('this.click();')
        log.info('导出按钮已点击')
    except Exception as e:
        log.error(f'导出流程中断: {e}')
        return

    time.sleep(5)
    _smart_dismiss_popups(page)
    _smart_dismiss_popups(target)
    time.sleep(10)


def _smart_wait_download(not_before: float = None, timeout: int = 150) -> str:
    if not_before is None:
        not_before = time.time() - 30
    deadline = time.time() + timeout
    while time.time() < deadline:
        for search_dir in (SMART_DOWNLOAD_DIR, SMART_DOWNLOAD_ROOT):
            files = (
                glob.glob(os.path.join(search_dir, '*.xlsx'))
                + glob.glob(os.path.join(search_dir, '*.xls'))
            )
            tmp        = glob.glob(os.path.join(search_dir, '*.crdownload'))
            candidates = [f for f in files if os.path.getmtime(f) >= not_before]
            if candidates and not tmp:
                newest = max(candidates, key=os.path.getmtime)
                log.info(f'文件下载成功: {newest}')
                return newest
        time.sleep(3)
    raise TimeoutError('等待下载超时')


def _smart_parse_all_rows(file_path: str) -> dict:
    """
    解析 Excel 报表，返回 {date_str: dau} 字典（包含所有日期行）。
    date_str 格式为 YYYY-MM-DD。
    """
    log.info(f'智能前端: 解析 Excel: {file_path}')
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        log.error(f'Excel 读取失败: {e}')
        return {}

    log.info(f'报表列名: {df.columns.tolist()}')

    # 识别日期列
    date_col = None
    for col in df.columns:
        if any(k in str(col) for k in ['日期', '时间', 'Date', 'Time']):
            date_col = col
            break
    if not date_col:
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(10)
            if any(sample.str.contains(r'\d{1,2}[-/]\d{1,2}', regex=True)):
                date_col = col
                break
    if not date_col:
        date_col = df.columns[0]
        log.warning(f'日期列兜底使用第一列: {date_col}')

    df[date_col] = df[date_col].astype(str)
    num_cols     = df.select_dtypes(include=['number']).columns

    result = {}
    current_year = datetime.date.today().year
    for _, row in df.iterrows():
        date_text = str(row[date_col]).strip()

        # 尝试解析完整日期 YYYY-MM-DD 或 YYYY/MM/DD
        m = re.search(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})', date_text)
        if m:
            date_str = m.group(1).replace('/', '-')
        else:
            # 尝试 MM-DD / MM/DD，补全年份
            m = re.search(r'(\d{1,2}[-/]\d{1,2})', date_text)
            if m:
                raw      = m.group(1).replace('/', '-')
                date_str = f"{current_year}-{raw.zfill(5)}"
            else:
                continue

        if num_cols.empty:
            continue
        total = int(row[num_cols].fillna(0).sum())
        result[date_str] = total

    log.info(f'Excel 解析出 {len(result)} 行日期数据')
    return result


def backfill_smart_frontend_dau(dates: list):
    """通过浏览器下载 30 天 Excel，回填指定日期的智能前端日活"""
    if not dates:
        return
    log.info(f"[smart_frontend] 回填 {len(dates)} 天: {dates}")

    _clean_smart_download()
    _inject_chrome_permissions(SMART_PROFILE, f'{SMART_LOGIN_URL.split("/#")[0]}:443,*')
    _kill_chrome_on_port(SMART_PORT)

    start_time = time.time()
    page = _create_smart_page()
    try:
        _smart_login(page)
        _smart_export_excel(page)
        file_path = _smart_wait_download(not_before=start_time)
    finally:
        time.sleep(2)
        page.quit()
        log.info('智能前端浏览器已关闭')

    dau_map = _smart_parse_all_rows(file_path)

    conn = pymysql.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        for d in dates:
            dau = dau_map.get(d)
            if dau is None:
                log.warning(f"  [{d}] Excel 中未找到对应日期，跳过")
                continue
            cursor.execute(
                "INSERT INTO platform_daily_metrics (stat_date, smart_frontend_dau) "
                "VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE smart_frontend_dau = VALUES(smart_frontend_dau)",
                (d, dau)
            )
            log.info(f"  [{d}] smart_frontend_dau = {dau:,} 入库完成")
        conn.commit()
        cursor.close()
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# Step 4 — 内网爬虫回填（新增注册/实名）
# ════════════════════════════════════════════════════════════

class InternalBackfillSpider:
    """
    内网爬虫回填版：一次会话抓取所有可见日期行数据，
    支持同时回填多天缺失数据。
    表格按日期降序排列（最新在最上方），无需滑动，直接逐行提取。
    """

    def __init__(self):
        self.page = None

    # ── 浏览器初始化 ──────────────────────────────────────────

    def init_browser(self):
        from DrissionPage import ChromiumPage, ChromiumOptions
        co = ChromiumOptions()
        co.set_argument('--start-maximized')
        co.set_local_port(INTERNAL_PORT)
        co.set_user_data_path(INTERNAL_PROFILE)
        co.set_pref('profile.managed_default_content_settings.images', 2)
        co.set_argument('--disable-popup-blocking')
        co.set_argument('--no-sandbox')
        co.set_argument(
            '--disable-features=PrivateNetworkAccessChecks,'
            'BlockInsecurePrivateNetworkRequests'
        )
        log.info('内网爬虫: 正在启动浏览器...')
        self.page = ChromiumPage(addr_or_opts=co)
        self.page.run_js(
            'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        )
        self.page.set.timeouts(30)

    # ── 弹窗处理 ──────────────────────────────────────────────

    def _dismiss_popups(self):
        try:
            self.page.handle_alert(accept=True, timeout=1)
        except Exception:
            pass
        for sel in (
            'css:.el-message-box__btns button.el-button--primary',
            'xpath://button[.//span[contains(text(),"确定")]]',
            'xpath://button[.//span[contains(text(),"确认")]]',
            'xpath://button[text()="确定"]',
        ):
            try:
                btn = self.page.ele(sel, timeout=2)
                if btn:
                    btn.click()
                    time.sleep(1)
            except Exception:
                pass

    # ── 登录 ──────────────────────────────────────────────────

    def login(self):
        log.info(f'内网爬虫: 访问登录页: {INTERNAL_URL}')
        self.page.get(INTERNAL_URL)
        time.sleep(5)

        # 清除残留登录
        try:
            logout_btn = self.page.ele(
                'xpath://span[contains(text(), "退出")]', timeout=5
            )
            if logout_btn:
                logout_btn.click()
                time.sleep(3)
                self._dismiss_popups()
                time.sleep(5)
                self.page.get(INTERNAL_URL)
                time.sleep(5)
        except Exception:
            pass

        u_input = self.page.ele('#usernameTemp', timeout=15)
        if u_input:
            u_input.input(INTERNAL_USERNAME, clear=True)
            self.page.ele('#password', timeout=10).input(INTERNAL_PASSWORD, clear=True)
            self.page.ele('#loginBtn', timeout=10).click()
            time.sleep(5)
            try:
                self.page.handle_alert(accept=True, timeout=5)
            except Exception:
                pass
            self._dismiss_popups()
            time.sleep(5)
        else:
            log.warning('内网爬虫: 未找到登录框，可能已自动进入主页')

    # ── 菜单导航 ──────────────────────────────────────────────

    def navigate_to_data(self):
        try:
            self.page.handle_alert(accept=True, timeout=2)
        except Exception:
            pass
        self._dismiss_popups()
        for menu_name in ("数据平台", "运营指标统计", "活跃统计分析"):
            el = self.page.ele(f'text:{menu_name}', timeout=20)
            if el:
                el.click()
                time.sleep(5)
            else:
                raise Exception(f"菜单定位失败: {menu_name}")

    # ── 提取所有行数据 ────────────────────────────────────────

    def extract_all_rows(self) -> dict:
        """
        提取表格所有可见行，返回 {date_str: (reg_val, real_val)}。
        定位 is-scrolling-none 主表体，按列 class 名提取：
          el-table_1_column_6  = 新增注册用户
          el-table_1_column_14 = 新增实名用户
        第1行=昨日，第2行=前天，依此类推；
        优先读取日期列（el-table_1_column_2）校验，读取失败则按位置推算。
        """
        log.info('内网爬虫: 正在等待 iframe 加载...')
        time.sleep(10)

        frame  = self.page.get_frame('tag:iframe', timeout=15)
        target = frame if frame else self.page

        target.wait.ele_displayed('css:.el-table__body-wrapper', timeout=20)
        target.wait.ele_displayed('css:.el-table__row',    timeout=20)

        # 定位 is-scrolling-none 主表体（避免固定列重影）
        tbody_wrapper = target.ele('css:.el-table__body-wrapper.is-scrolling-none', timeout=10)
        if not tbody_wrapper:
            tbody_wrapper = target
            log.warning('内网爬虫: 未找到 is-scrolling-none，兜底使用整页')

        rows   = tbody_wrapper.eles('css:.el-table__row')
        result = {}

        for idx, row in enumerate(rows):
            # 按位置推算该行对应日期（row 0 = 昨天）
            inferred_date = (
                datetime.date.today() - datetime.timedelta(days=idx + 1)
            ).strftime('%Y-%m-%d')

            # 尝试从日期列（column_2）读取并验证
            row_date = inferred_date
            try:
                date_cell = row.ele('css:.el-table_1_column_2 .cell', timeout=2)
                if date_cell:
                    date_text = date_cell.text.strip()
                    m = re.search(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})', date_text)
                    if m:
                        row_date = m.group(1).replace('/', '-')
                        if row_date != inferred_date:
                            log.info(
                                f'  第{idx+1}行: 日期列={row_date}，位置推算={inferred_date}，'
                                f'以日期列为准'
                            )
            except Exception:
                log.info(f'  第{idx+1}行: 未能读取日期列，按位置推算={row_date}')

            # 提取新增实名（column_6）和新增注册（column_14）
            try:
                real_cell = row.ele('css:.el-table_1_column_6 .cell', timeout=2)
                reg_cell  = row.ele('css:.el-table_1_column_14 .cell', timeout=2)
                reg_text  = reg_cell.text.strip()  if reg_cell  else '0'
                real_text = real_cell.text.strip() if real_cell else '0'

                reg_val  = int(re.sub(r'[^\d]', '', reg_text))  if reg_text  else 0
                real_val = int(re.sub(r'[^\d]', '', real_text)) if real_text else 0

                result[row_date] = (reg_val, real_val)
                log.info(
                    f'  内网爬虫: {row_date} 注册={reg_val}, 实名={real_val}'
                )
            except Exception as e:
                log.warning(f'  第{idx+1}行({row_date}) 数据提取失败: {e}')

        log.info(f'内网爬虫: 共提取 {len(result)} 行数据')
        return result

    # ── 入库 ──────────────────────────────────────────────────

    def save_to_db(self, data_map: dict, dates: list):
        conn = pymysql.connect(**DB_CONFIG)
        try:
            cursor = conn.cursor()
            for d in dates:
                if d not in data_map:
                    log.warning(f'  [{d}] 内网表格未找到该日期，跳过')
                    continue
                reg_val, real_val = data_map[d]
                cursor.execute(
                    "INSERT INTO platform_daily_metrics "
                    "(stat_date, new_register_users, new_realname_users) "
                    "VALUES (%s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "new_register_users = VALUES(new_register_users), "
                    "new_realname_users = VALUES(new_realname_users)",
                    (d, reg_val, real_val)
                )
                log.info(
                    f'  [{d}] 注册={reg_val}, 实名={real_val} 入库完成'
                )
            conn.commit()
            cursor.close()
        finally:
            conn.close()

    # ── 带重试的入口 ──────────────────────────────────────────

    def run(self, dates: list):
        _inject_chrome_permissions(
            INTERNAL_PROFILE, f'{INTERNAL_URL.split("?")[0].rsplit("/", 1)[0]}:443,*'
        )
        for i in range(3):
            try:
                self.init_browser()
                self.login()
                self.navigate_to_data()
                data_map = self.extract_all_rows()
                self.save_to_db(data_map, dates)
                return
            except Exception as e:
                log.error(f'内网爬虫第 {i+1} 次失败: {e}')
                traceback.print_exc()
                if i == 2:
                    raise
                time.sleep(10)
            finally:
                if self.page:
                    try:
                        self.page.quit()
                    except Exception:
                        pass


def backfill_internal_network(dates: list):
    if not dates:
        return
    log.info(f"[internal_network] 回填 {len(dates)} 天: {dates}")
    InternalBackfillSpider().run(dates)


# ════════════════════════════════════════════════════════════
# 主入口
# ════════════════════════════════════════════════════════════

def main():
    log.info('=' * 65)
    log.info('数据回填任务启动')
    log.info(f'当前时间: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    log.info('=' * 65)

    # ── Step 1: 检测缺失 ─────────────────────────────────────
    log.info('\n【Step 1】检查近7天缺失数据...')
    missing = get_missing_dates()

    total = sum(len(v) for v in missing.values())
    if total == 0:
        log.info('近7天数据均已完整，无需回填！')
        return

    log.info('\n缺失汇总:')
    for source, dates in missing.items():
        if dates:
            log.info(f'  {source:20s}: {dates}')

    # ── Step 2: 友盟平台日活 ─────────────────────────────────
    log.info('\n【Step 2】回填友盟平台日活（安卓/苹果/鸿蒙/微信/支付宝）...')
    try:
        backfill_umeng_dau(missing['umeng_dau'])
    except Exception as e:
        log.error(f'友盟日活回填失败: {e}')
        traceback.print_exc()

    # ── Step 3: resource_total ───────────────────────────────
    log.info('\n【Step 3】回填 resource_total...')
    try:
        backfill_resource_total(missing['resource_total'])
    except Exception as e:
        log.error(f'resource_total 回填失败: {e}')
        traceback.print_exc()

    # ── Step 4: 5100_detail ──────────────────────────────────
    log.info('\n【Step 4】回填 5100_detail（510100_items 子服务明细）...')
    try:
        backfill_5100_detail(missing['5100_detail'])
    except Exception as e:
        log.error(f'5100_detail 回填失败: {e}')
        traceback.print_exc()

    # ── Step 5: 智能前端日活 ─────────────────────────────────
    log.info('\n【Step 5】回填智能前端日活...')
    try:
        backfill_smart_frontend_dau(missing['smart_frontend'])
    except Exception as e:
        log.error(f'智能前端日活回填失败: {e}')
        traceback.print_exc()

    # ── Step 6: 内网爬虫（注册/实名）───────────────────────────
    log.info('\n【Step 6】回填新增注册/实名用户数...')
    try:
        backfill_internal_network(missing['internal_network'])
    except Exception as e:
        log.error(f'内网爬虫回填失败: {e}')
        traceback.print_exc()

    # ── 完成汇报 ─────────────────────────────────────────────
    log.info('\n' + '=' * 65)
    log.info('数据回填任务完成')
    log.info('=' * 65)


if __name__ == '__main__':
    main()
