#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
smart_frontend_dau_spider.py
智能前端系统爬虫：通过导出 30 天 UV 报表获取昨日 DAU 数据。
"""

import os
import sys
import time
import glob
import random
import re
import json
import logging
from datetime import datetime, timedelta

import pymysql
import pandas as pd
from DrissionPage import ChromiumPage, ChromiumOptions

# ── 输出编码 ─────────────────────────────────────────────────
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ── 统一配置加载 ─────────────────────────────────────────────
try:
    from config import (
        DB_CONFIG,
        FRONTEND_LOGIN_URL      as LOGIN_URL,
        FRONTEND_LOGIN_USERNAME as USERNAME,
        FRONTEND_LOGIN_PASSWORD as PASSWORD,
        FRONTEND_DOWNLOAD_DIR   as DOWNLOAD_DIR,
        FRONTEND_DOWNLOAD_ROOT  as DOWNLOAD_ROOT,
        FRONTEND_DEBUG_DIR      as DEBUG_DIR,
        FRONTEND_CHROME_PROFILE as CHROME_PROFILE,
        FRONTEND_CHROME_PORT    as CHROME_PORT,
    )
except ImportError:
    LOGIN_URL    = '更换为智能前端系统登录地址'
    USERNAME     = '更换为系统账号'
    PASSWORD     = '更换为系统密码'
    DOWNLOAD_DIR  = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau'
    DOWNLOAD_ROOT = r'C:\Users\YOUR_USERNAME\Downloads'
    DEBUG_DIR     = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau\debug'
    CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\smart_spider_chrome'
    CHROME_PORT    = 9335
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
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"smart_spider_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── 工具函数 ─────────────────────────────────────────────────

def kill_chrome_on_port(port: int) -> None:
    """杀死占用指定调试端口的 Chrome 进程"""
    import subprocess
    try:
        res = subprocess.run(['netstat', '-ano'], capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if f':{port} ' in line and 'LISTENING' in line:
                pid = line.strip().split()[-1]
                if pid.isdigit():
                    subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True)
                    log.info(f"已清理占用端口 {port} 的进程 PID: {pid}")
    except: pass


def clear_chrome_lock():
    """清理 Chrome 锁文件及修复异常退出状态"""
    import subprocess
    ps_cmd = (
        f'Get-CimInstance Win32_Process -Filter "name = \'chrome.exe\'" | '
        f'Where-Object {{ $_.CommandLine -like "*{CHROME_PROFILE}*" }} | '
        f'ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }}'
    )
    try:
        subprocess.run(['powershell', '-Command', ps_cmd], capture_output=True, timeout=10)
    except: pass

    lock_file = os.path.join(CHROME_PROFILE, 'SingletonLock')
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
            log.info(f"清理锁文件: {lock_file}")
        except: pass

    for crash_file in [
        os.path.join(CHROME_PROFILE, 'Local State'),
        os.path.join(CHROME_PROFILE, 'Default', 'Preferences'),
    ]:
        if os.path.exists(crash_file):
            try:
                with open(crash_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                if '"exit_type":"Crashed"' in content or '"exited_cleanly":false' in content:
                    content = content.replace('"exit_type":"Crashed"', '"exit_type":"Normal"')
                    content = content.replace('"exited_cleanly":false', '"exited_cleanly":true')
                    with open(crash_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    log.info(f"已修复异常退出状态: {os.path.basename(crash_file)}")
            except: pass


def _dismiss_popups(page):
    """尝试点击页面上出现的‘确定’或‘允许’类弹窗按钮"""
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
        except: pass


def _screenshot(page, name):
    try:
        path = os.path.join(DEBUG_DIR, f"{datetime.now().strftime('%H%M%S')}_{name}")
        page.get_screenshot(path=path, full_page=True)
        log.info(f"已保存调试截图: {path}")
    except: pass


def run_spider():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    log.info(f"🚀 智能前端日活采集启动，目标日期: {yesterday}")

    # 1. 环境准备
    kill_chrome_on_port(CHROME_PORT)
    clear_chrome_lock()
    for d in (DOWNLOAD_DIR, DEBUG_DIR, CHROME_PROFILE):
        os.makedirs(d, exist_ok=True)
    
    # 清理旧 Excel
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")) + glob.glob(os.path.join(DOWNLOAD_DIR, "*.xls")):
        try: os.remove(f)
        except: pass

    # 2. 浏览器启动
    co = ChromiumOptions()
    co.set_local_port(CHROME_PORT)
    co.set_user_data_path(CHROME_PROFILE)
    co.set_pref('profile.managed_default_content_settings.images', 2)
    co.set_pref('download.default_directory', DOWNLOAD_DIR)
    co.set_argument('--start-maximized')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-blink-features=AutomationControlled')

    page = ChromiumPage(addr_or_opts=co)
    page.run_js('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')
    page.set.timeouts(30)

    try:
        # 3. 登录
        log.info(f"正在访问登录页: {LOGIN_URL}")
        page.get(LOGIN_URL)
        time.sleep(5)
        _dismiss_popups(page)

        u_input = page.ele('xpath://input[contains(@placeholder,"账号") or contains(@placeholder,"手机号")]', timeout=15)
        if u_input:
            u_input.input(USERNAME, clear=True)
            page.ele('xpath://input[@type="password"]', timeout=10).input(PASSWORD, clear=True)
            page.ele('xpath://button[contains(@class,"el-button--primary")]', timeout=10).click()
            log.info("已提交登录信息，等待页面跳转...")
            time.sleep(30) # 等待登录后的各种弹窗和加载
        else:
            log.info("未发现登录框，假设已处于登录状态")

        _dismiss_popups(page)

        # 4. 导航至“应用→概览”
        log.info("正在切换菜单至 应用 -> 概览...")
        try:
            app_menu = page.ele('xpath://li[.//span[text()="应用"]]', timeout=20)
            app_menu.click()
            time.sleep(5)
            overview_menu = page.ele('xpath://li[.//span[text()="概览"]]', timeout=20)
            overview_menu.click()
            time.sleep(20)
        except Exception as e:
            log.warning(f"菜单切换受阻: {e}，将尝试直接在当前页查找 iframe")

        # 5. 进入 iframe 定位 UV 卡片
        def get_target():
            for _ in range(10):
                try:
                    frame = page.get_frame('tag:iframe')
                    if frame:
                        log.info('已定位到数据 iframe')
                        return frame
                except: pass
                time.sleep(3)
            log.warning('未找到 iframe，尝试直接在主页面操作')
            return page

        target = get_target()

        # 等待数据加载
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                card_bodies = target.eles('css:div.el-card__body')
                if any(len(cb.text.strip()) > 15 and '无数据' not in cb.text for cb in card_bodies):
                    log.info('检测到图表数据已加载')
                    break
            except:
                target = get_target()
            time.sleep(5)

        # 切换到列表视图
        try:
            uv_card = target.ele('xpath://div[contains(@class,"el-card") and .//*[contains(text(),"UV")]]', timeout=20)
            uv_card.run_js('this.scrollIntoView({block:"center"});')
            time.sleep(2)
            tickets = uv_card.ele('css:i.el-icon-tickets', timeout=15)
            tickets.run_js('this.click();')
            log.info('已切换为列表视图')
        except Exception as e:
            log.error(f'UV 卡片操作失败: {e}')
            _screenshot(page, 'uv_card_failed.png')

        # 切换 30 天并导出
        try:
            radio_30 = target.ele('css:input[value="30days"]', index=2, timeout=15)
            if radio_30:
                radio_30.run_js('this.click();')
                log.info('已选择 30 天范围')
                time.sleep(10)
            
            export_btn = target.ele('css:button[title="导出Excel"]', index=1, timeout=20)
            if export_btn:
                export_btn.run_js('this.click();')
                log.info('点击导出 Excel，等待下载...')
            else:
                raise Exception('未找到导出按钮')
        except Exception as e:
            log.error(f'导出流程出错: {e}')
            _screenshot(page, 'export_failed.png')
            raise e

        # 6. 等待并解析文件
        time.sleep(15)
        _dismiss_popups(page)
        _dismiss_popups(target)
        
        start_wait = time.time()
        file_path = None
        while time.time() - start_wait < 120:
            files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")) + glob.glob(os.path.join(DOWNLOAD_DIR, "*.xls"))
            if files:
                file_path = max(files, key=os.path.getmtime)
                if not file_path.endswith('.crdownload'):
                    break
            time.sleep(5)
        
        if not file_path:
            raise Exception("下载超时，未获得 Excel 文件")

        log.info(f"开始解析报表: {file_path}")
        df = pd.read_excel(file_path)
        
        # 寻找日期和 UV 列
        date_col = None
        uv_col = None
        for col in df.columns:
            col_str = str(col)
            if '日期' in col_str or '时间' in col_str: date_col = col
            if 'UV' in col_str or '访客数' in col_str: uv_col = col
        
        if not date_col: date_col = df.columns[0]
        if not uv_col: uv_col = df.columns[1]

        df[date_col] = df[date_col].astype(str)
        
        yesterday_val = None
        for _, row in df.iterrows():
            if yesterday in str(row[date_col]):
                yesterday_val = int(row[uv_col])
                break
        
        if yesterday_val is not None:
            log.info(f"解析成功 -> {yesterday} DAU: {yesterday_val}")
            
            # 7. 入库
            conn = pymysql.connect(**DB_CONFIG)
            try:
                cursor = conn.cursor()
                sql = """
                    INSERT INTO platform_daily_metrics (stat_date, smart_frontend_dau)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE smart_frontend_dau = VALUES(smart_frontend_dau)
                """
                cursor.execute(sql, (yesterday, yesterday_val))
                conn.commit()
                log.info("数据已入库")
            finally:
                conn.close()
        else:
            log.error(f"在导出的报表中未找到昨日 ({yesterday}) 的数据")

    except Exception as e:
        log.error(f"采集过程发生严重异常: {e}")
        traceback.print_exc()
    finally:
        if page:
            try: page.quit()
            except: pass
        kill_chrome_on_port(CHROME_PORT)

if __name__ == "__main__":
    run_spider()
