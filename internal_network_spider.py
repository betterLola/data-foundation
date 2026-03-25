#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
internal_network_spider.py
内网系统爬虫：抓取每日新增注册用户数和新增实名用户数。
"""

import os
import sys
import time
import re
import json
import logging
from datetime import datetime, timedelta

import pymysql
from DrissionPage import ChromiumPage, ChromiumOptions

# ── 输出编码 ─────────────────────────────────────────────────
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ── 统一配置加载 ─────────────────────────────────────────────
try:
    from config import (
        DB_CONFIG,
        INTRANET_LOGIN_URL      as TARGET_URL,
        INTRANET_LOGIN_USERNAME as LOGIN_USERNAME,
        INTRANET_LOGIN_PASSWORD as LOGIN_PASSWORD,
        INTRANET_CHROME_PORT    as CHROME_PORT,
        INTRANET_CHROME_PROFILE as CHROME_PROFILE,
    )
except ImportError:
    TARGET_URL = "更换为内网系统登录地址"
    LOGIN_USERNAME = "更换为内网系统账号"
    LOGIN_PASSWORD = "更换为内网系统密码"
    CHROME_PORT = 9336
    CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\internal_spider_chrome'
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
            os.path.join(LOG_DIR, f"internal_spider_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── 工具函数 ─────────────────────────────────────────────────

def kill_chrome_on_port(port: int) -> None:
    """杀死占用指定端口的 Chrome 进程"""
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
    # 1. 杀死该 profile 相关的进程
    import subprocess
    ps_cmd = (
        f'Get-CimInstance Win32_Process -Filter "name = \'chrome.exe\'" | '
        f'Where-Object {{ $_.CommandLine -like "*{CHROME_PROFILE}*" }} | '
        f'ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }}'
    )
    try:
        subprocess.run(['powershell', '-Command', ps_cmd], capture_output=True, timeout=10)
    except: pass

    # 2. 清理锁文件
    lock_file = os.path.join(CHROME_PROFILE, 'SingletonLock')
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
            log.info(f"清理锁文件: {lock_file}")
        except: pass

    # 3. 修复崩溃状态
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


class InternalSpider:
    def __init__(self):
        self.page = None
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    def init_browser(self):
        kill_chrome_on_port(CHROME_PORT)
        clear_chrome_lock()
        time.sleep(3)

        co = ChromiumOptions()
        co.set_argument('--start-maximized')
        co.set_local_port(CHROME_PORT)
        co.set_user_data_path(CHROME_PROFILE)
        co.set_pref('profile.managed_default_content_settings.images', 2)
        co.set_argument('--disable-popup-blocking')
        co.set_argument('--no-sandbox')
        co.set_argument(
            '--disable-features=PrivateNetworkAccessChecks,'
            'BlockInsecurePrivateNetworkRequests'
        )
        log.info('正在启动浏览器...')
        self.page = ChromiumPage(addr_or_opts=co)
        self.page.run_js(
            'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        )
        self.page.set.timeouts(30)

    def _dismiss_popups(self):
        try:
            self.page.handle_alert(accept=True, timeout=1)
        except: pass
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
            except: pass

    def login(self):
        log.info(f'访问登录页: {TARGET_URL}')
        self.page.get(TARGET_URL)
        time.sleep(5)

        # 清除残留登录
        try:
            logout_btn = self.page.ele('xpath://span[contains(text(), "退出")]', timeout=5)
            if logout_btn:
                logout_btn.click()
                time.sleep(3)
                self._dismiss_popups()
                time.sleep(5)
                self.page.get(TARGET_URL)
                time.sleep(5)
        except: pass

        u_input = self.page.ele('#usernameTemp', timeout=15)
        if u_input:
            u_input.input(LOGIN_USERNAME, clear=True)
            self.page.ele('#password', timeout=10).input(LOGIN_PASSWORD, clear=True)
            self.page.ele('#loginBtn', timeout=10).click()
            time.sleep(5)
            try:
                self.page.handle_alert(accept=True, timeout=5)
            except: pass
            self._dismiss_popups()
            time.sleep(5)
        else:
            log.warning('未找到登录框，可能已自动进入主页')

    def navigate_to_data(self):
        try:
            self.page.handle_alert(accept=True, timeout=2)
        except: pass
        self._dismiss_popups()
        for menu_name in ("数据平台", "运营指标统计", "活跃统计分析"):
            el = self.page.ele(f'text:{menu_name}', timeout=20)
            if el:
                el.click()
                time.sleep(5)
            else:
                raise Exception(f"菜单定位失败: {menu_name}")

    def extract_data(self):
        log.info('正在等待数据表格加载...')
        time.sleep(10)

        frame = self.page.get_frame('tag:iframe', timeout=15)
        target = frame if frame else self.page

        target.wait.ele_displayed('css:.el-table__body-wrapper', timeout=20)
        target.wait.ele_displayed('css:.el-table__row', timeout=20)

        # 降序排列，第一行通常是昨天
        tbody_wrapper = target.ele('css:.el-table__body-wrapper.is-scrolling-none', timeout=10)
        if not tbody_wrapper:
            tbody_wrapper = target
            log.warning('未找到 is-scrolling-none 主表体，兜底使用整页查找')

        rows = tbody_wrapper.eles('css:.el-table__row')
        log.info(f"正在分析昨日数据 ({self.yesterday})...")
        
        found = False
        for idx, row in enumerate(rows):
            try:
                # 检查日期列（column_2）
                date_cell = row.ele('css:.el-table_1_column_2 .cell', timeout=2)
                if date_cell:
                    date_text = date_cell.text.strip()
                    if self.yesterday in date_text:
                        # 找到目标日期行
                        # column_6 = 新增注册用户，column_14 = 新增实名用户
                        reg_cell  = row.ele('css:.el-table_1_column_14 .cell', timeout=2)
                        real_cell = row.ele('css:.el-table_1_column_6 .cell', timeout=2)

                        reg_text = reg_cell.text.strip() if reg_cell else '0'
                        real_text = real_cell.text.strip() if real_cell else '0'

                        reg_val = int(re.sub(r'[^\d]', '', reg_text)) if reg_text else 0
                        real_val = int(re.sub(r'[^\d]', '', real_text)) if real_text else 0

                        log.info(f"提取结果 -> 注册={reg_val}, 实名={real_val}")
                        self.save_to_db(reg_val, real_val)
                        found = True
                        break
            except Exception as e:
                log.warning(f"分析行 {idx+1} 时出错: {e}")

        if not found:
            log.warning(f"在表格前几行中未找到昨日日期 ({self.yesterday})，尝试取第一行兜底...")
            row = rows[0]
            try:
                reg_cell  = row.ele('css:.el-table_1_column_14 .cell', timeout=2)
                real_cell = row.ele('css:.el-table_1_column_6 .cell', timeout=2)
                reg_val = int(re.sub(r'[^\d]', '', reg_cell.text.strip()))
                real_val = int(re.sub(r'[^\d]', '', real_cell.text.strip()))
                log.info(f"第一行兜底 -> 注册={reg_val}, 实名={real_val}")
                self.save_to_db(reg_val, real_val)
            except Exception as e:
                log.error(f"第一行兜底提取也失败了: {e}")

    def save_to_db(self, reg_val, real_val):
        conn = pymysql.connect(**DB_CONFIG)
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO platform_daily_metrics (stat_date, new_register_users, new_realname_users)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    new_register_users = VALUES(new_register_users),
                    new_realname_users = VALUES(new_realname_users)
            """
            cursor.execute(sql, (self.yesterday, reg_val, real_val))
            conn.commit()
            log.info("数据已入库")
        finally:
            conn.close()

    def run(self):
        try:
            self.init_browser()
            self.login()
            self.navigate_to_data()
            self.extract_data()
        except Exception as e:
            log.error(f"采集流程发生异常: {e}")
            traceback.print_exc()
        finally:
            if self.page:
                try: self.page.quit()
                except: pass
            kill_chrome_on_port(CHROME_PORT)

if __name__ == "__main__":
    InternalSpider().run()
