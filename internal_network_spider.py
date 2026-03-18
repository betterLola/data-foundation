#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
内网爬虫 - DrissionPage 稳控版
===========================
负责抓取：新增注册用户数、新增实名用户数
优势：无需 WebDriver 驱动，直接接管浏览器，抗干扰能力强。
"""

import os
import time
import re
import logging
import datetime
import pymysql
from DrissionPage import ChromiumPage, ChromiumOptions

# ── 常量与配置 ──────────────────────────────────────────────────
TARGET_URL = "更换为内网系统登录地址"  # 例：https://your-intranet.example.com/login
LOGIN_USERNAME = "更换为内网系统账号"
LOGIN_PASSWORD = "更换为内网系统密码"

DB_CONFIG = {
    'host': 'localhost',           # 数据库地址
    'port': 3306,                  # 数据库端口，默认 3306
    'user': 'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset':  'utf8mb4'
}

LOG_DIR = r"C:\Users\YOUR_USERNAME\logs"                            # 日志目录，请替换用户名
CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\internal_spider_chrome'  # Chrome专用Profile
CHROME_PORT = 9336  # Chrome远程调试端口，可自定义避免冲突

os.makedirs(LOG_DIR, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"internal_spider_{datetime.datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# ── 辅助函数 ──────────────────────────────────────────────────

def ensure_chrome_permissions():
    """在启动前预注入本地网络访问权限，抑制浏览器地址栏原生弹窗"""
    import json
    pref_dir = os.path.join(CHROME_PROFILE, 'Default')
    pref_path = os.path.join(pref_dir, 'Preferences')
    site_key = 'https://your-intranet.example.com:443,*'  # 请替换为实际内网系统域名
    
    os.makedirs(pref_dir, exist_ok=True)
    prefs = {}
    if os.path.exists(pref_path):
        try:
            with open(pref_path, encoding='utf-8') as f:
                prefs = json.load(f)
        except: pass
    
    exceptions = prefs.setdefault('profile', {}).setdefault('content_settings', {}).setdefault('exceptions', {})
    for k in ('local_network', 'loopback_network'):
        exceptions.setdefault(k, {})[site_key] = {'setting': 1}
    
    try:
        with open(pref_path, 'w', encoding='utf-8') as f:
            json.dump(prefs, f, ensure_ascii=False)
        log.info("已成功预注入浏览器原生权限")
    except: pass

# ── 核心逻辑 ──────────────────────────────────────────────────

class InternalSpider:
    def __init__(self):
        self.page = None

    def init_browser(self):
        co = ChromiumOptions()
        co.set_argument('--start-maximized')
        co.set_local_port(CHROME_PORT)
        co.set_user_data_path(CHROME_PROFILE)
        
        # 稳控参数
        co.set_pref('profile.managed_default_content_settings.images', 2)
        co.set_argument('--disable-popup-blocking')
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-features=PrivateNetworkAccessChecks,BlockInsecurePrivateNetworkRequests')
        
        log.info("正在启动浏览器...")
        self.page = ChromiumPage(addr_or_opts=co)
        self.page.run_js('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')
        self.page.set.timeouts(30)

    def login(self):
        log.info(f"访问登录页: {TARGET_URL}")
        self.page.get(TARGET_URL)
        time.sleep(5)

        # 处理残留登录
        try:
            logout_btn = self.page.ele('xpath://span[contains(text(), "退出")]', timeout=5)
            if logout_btn:
                log.info("检测到已处于登录状态，执行注销重登...")
                logout_btn.click()
                time.sleep(3)
                # 处理注销确认弹窗（可能是 Alert 也可能是页面弹窗）
                self._dismiss_popups() 
                time.sleep(5)
                self.page.get(TARGET_URL)
                time.sleep(5)
        except: pass

        log.info("执行登录表单填写...")
        u_input = self.page.ele('#usernameTemp', timeout=15)
        if u_input:
            u_input.input(LOGIN_USERNAME, clear=True)
            self.page.ele('#password', timeout=10).input(LOGIN_PASSWORD, clear=True)
            self.page.ele('#loginBtn', timeout=10).click()
            
            # 【核心修正】处理浏览器原生弹窗 (Alert/Confirm)
            # 参照老版本 handle_alert 逻辑
            time.sleep(5)
            try:
                # DrissionPage 处理 Alert 的方法
                if self.page.handle_alert(accept=True, timeout=5):
                    log.info("已通过 handle_alert 接受浏览器原生弹窗")
            except: pass
            
            # 同时保留页面级别弹窗清理
            self._dismiss_popups()
            
            time.sleep(5)
            log.info("登录跳转完成")
        else:
            log.warning("未找到登录框，可能已自动进入主页")

    def _dismiss_popups(self):
        """处理页面级别 (DOM) 的确认弹窗"""
        # 先尝试处理一次浏览器原生 Alert
        try: self.page.handle_alert(accept=True, timeout=1)
        except: pass
        
        selectors = [
            'css:.el-message-box__btns button.el-button--primary',
            'xpath://button[.//span[contains(text(),"确定")]]',
            'xpath://button[.//span[contains(text(),"确认")]]',
            'xpath://button[text()="确定"]',
        ]
        for sel in selectors:
            try:
                btn = self.page.ele(sel, timeout=2)
                if btn:
                    btn.click()
                    log.info(f"已点击页面弹窗按钮: {sel}")
                    time.sleep(1)
            except: pass

    def navigate_to_data(self):
        # 导航前清理所有类型的弹窗
        try: self.page.handle_alert(accept=True, timeout=2)
        except: pass
        self._dismiss_popups()
        menus = ["数据平台", "运营指标统计", "活跃统计分析"]
        for menu_name in menus:
            log.info(f"切换菜单: {menu_name}")
            el = self.page.ele(f'text:{menu_name}', timeout=20)
            if el:
                el.click()
                time.sleep(5)
            else:
                raise Exception(f"菜单定位失败: {menu_name}")

    def extract_data(self):
        log.info("正在等待报表 iframe 加载...")
        time.sleep(10)
        frame = self.page.get_frame('tag:iframe', timeout=15)
        target = frame if frame else self.page
        
        target.wait.ele_displayed('css:.el-table__body-wrapper', timeout=20)
        target.wait.ele_displayed('css:.el-table__row', timeout=20)

        # 定位 is-scrolling-none 区域，避免固定列重影行
        tbody_wrapper = target.ele('css:.el-table__body-wrapper.is-scrolling-none', timeout=10)
        if not tbody_wrapper:
            tbody_wrapper = target
            log.warning("未找到 is-scrolling-none，兜底使用整页")

        # 第一行 = 昨日数据
        row = tbody_wrapper.ele('css:.el-table__row', timeout=10)
        if not row:
            raise Exception("未找到数据行")

        # 按列 class 名提取：column_6=新增实名用户，column_14=新增注册用户
        reg_text = ''
        real_text = ''
        try:
            real_cell = row.ele('css:.el-table_1_column_6 .cell', timeout=3)
            real_text = real_cell.text.strip() if real_cell else ''
        except Exception as e:
            log.warning(f"column_6 新增实名用户提取失败: {e}")

        try:
            reg_cell = row.ele('css:.el-table_1_column_14 .cell', timeout=3)
            reg_text = reg_cell.text.strip() if reg_cell else ''
        except Exception as e:
            log.warning(f"column_14 新增注册用户提取失败: {e}")

        log.info(f"提取结果 -> 注册: {reg_text}, 实名: {real_text}")

        reg_val = int(re.sub(r'[^\d]', '', reg_text)) if reg_text else 0
        real_val = int(re.sub(r'[^\d]', '', real_text)) if real_text else 0
        return reg_val, real_val

    def save_to_db(self, reg_val, real_val):
        log.info(f"同步数据库 (日期: {YESTERDAY})")
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO platform_daily_metrics 
                        (stat_date, new_register_users, new_realname_users)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        new_register_users = VALUES(new_register_users),
                        new_realname_users = VALUES(new_realname_users)
                """
                cur.execute(sql, (YESTERDAY, reg_val, real_val))
            conn.commit()
            log.info(f"✅ 写入成功：注册={reg_val}, 实名={real_val}")
        finally:
            conn.close()

    def run(self):
        ensure_chrome_permissions()
        max_retry = 2
        for i in range(max_retry + 1):
            try:
                self.init_browser()
                self.login()
                self.navigate_to_data()
                reg, real = self.extract_data()
                self.save_to_db(reg, real)
                return
            except Exception as e:
                log.error(f"第 {i+1} 次失败: {e}")
                if i == max_retry: raise
                time.sleep(10)
            finally:
                if self.page: self.page.quit()

if __name__ == "__main__":
    InternalSpider().run()
