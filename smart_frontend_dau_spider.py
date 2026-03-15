#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
smart_frontend_dau_spider.py
抓取「智能前端日活」并写入 platform_daily_metrics.smart_frontend_dau
优化策略：模拟真人行为、隐藏爬虫指纹、智能等待、非隐私模式自愈。
"""

import os
import time
import glob
import random
import datetime
import logging

import pymysql
import pandas as pd
from DrissionPage import ChromiumPage, ChromiumOptions

# ── 日志 ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── 常量 ──────────────────────────────────────────────────────
LOGIN_URL    = '更换为智能前端系统登录地址'  # 例：https://your-frontend.example.com/account-center/#/login
USERNAME     = '更换为系统账号'
PASSWORD     = '更换为系统密码'
DOWNLOAD_DIR  = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau'  # 下载目录，请替换用户名
DOWNLOAD_ROOT = r'C:\Users\YOUR_USERNAME\Downloads'                     # Chrome默认下载目录
DEBUG_DIR     = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau\debug'  # 调试截图目录
CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\smart_spider_chrome'  # Chrome专用Profile
CHROME_PORT    = 9335  # Chrome远程调试端口，可自定义

DB_CONFIG = {
    'host':     'localhost',           # 数据库地址
    'port':     3306,                  # 数据库端口，默认 3306
    'user':     'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset':  'utf8mb4',
}

YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)


# ── 浏览器初始化 ───────────────────────────────────────────────
def find_chrome_path():
    paths = [
        r'C:\Program Files\Google\Chrome\Application\chrome.exe',
        r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
        os.path.expandvars(r'%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe'),
    ]
    for p in paths:
        if os.path.exists(p): return p
    return None

def create_page() -> ChromiumPage:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    os.makedirs(CHROME_PROFILE, exist_ok=True)

    co = ChromiumOptions()
    
    # 【优化】显式指定浏览器路径
    browser_path = find_chrome_path()
    if browser_path: co.set_browser_path(browser_path)

    # 【优化】配置真人级 User-Agent
    co.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    
    co.set_local_port(CHROME_PORT)
    co.set_user_data_path(CHROME_PROFILE)
    
    # 【优化】禁用图片加载（提升速度 + 减少检测）
    co.set_pref('profile.managed_default_content_settings.images', 2)
    
    # 抑制系统下载弹窗
    co.set_pref('download.prompt_for_download', False)
    co.set_pref('download.default_directory', DOWNLOAD_DIR)
    co.set_pref('download.directory_upgrade', True)
    co.set_pref('safebrowsing.enabled', True)
    
    # 自动允许下载、通知
    co.set_pref('profile.default_content_setting_values.notifications', 1)
    co.set_pref('profile.default_content_setting_values.automatic_downloads', 1)
    
    # 稳控参数
    co.set_argument('--disable-gpu')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-dev-shm-usage')
    co.set_argument('--disable-popup-blocking')
    co.set_argument('--start-maximized')
    co.set_argument('--no-first-run')
    co.set_argument('--disable-blink-features=AutomationControlled')
    # 绕过本地网络访问限制
    co.set_argument('--disable-features=PrivateNetworkAccessChecks,BlockInsecurePrivateNetworkRequests,OptimizationGuideFetching')
    
    co.set_download_path(DOWNLOAD_DIR)

    max_retries = 3
    for i in range(max_retries):
        try:
            log.info(f'正在启动浏览器 (第{i+1}次)...')
            page = ChromiumPage(addr_or_opts=co)
            
            # 【关键优化】隐藏 navigator.webdriver 特征
            page.run_js('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')
            
            try: page.set.download_path(DOWNLOAD_DIR)
            except: pass
            
            page.set.timeouts(30)
            log.info('浏览器启动成功并已注入防检测脚本！')
            return page
        except Exception as e:
            log.warning(f'浏览器启动失败: {e}')
            kill_chrome_on_port(CHROME_PORT)
            time.sleep(5)
            if i == max_retries - 1: raise e

# ── 模拟真人交互 ──────────────────────────────────────────────
def human_move(page: ChromiumPage):
    """模拟真人随机交互：微量滚动、点击空白处。"""
    try:
        # 随机点击页面空白处
        page.ele('xpath://body').click()
        time.sleep(random.uniform(0.5, 1.5))
        # 随机滚动
        page.scroll.to(0, random.randint(100, 400))
        time.sleep(random.uniform(0.5, 1.0))
        page.scroll.to_top()
    except:
        pass

# ── 登录 ──────────────────────────────────────────────────────
def login(page: ChromiumPage) -> None:
    log.info('打开登录页 ...')
    _dismiss_popups(page)
    
    # 配置真人级 Header
    page.set.headers({
        'Referer': 'https://tfsmy.chengdu.gov.cn/',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
    })
    
    page.get(LOGIN_URL)
    # 修正 API 错误：使用 doc_loaded() 或直接 sleep
    try: page.wait.doc_loaded()
    except: time.sleep(5)
    
    time.sleep(8)
    _dismiss_popups(page)

    # 【核心优化】由于非隐私模式受以前登录影响，必须先尝试点击退出
    log.info('正在检查并强制注销旧会话 ...')
    try:
        # 寻找包含“退出”文字的按钮或链接
        logout_selectors = [
            'xpath://span[contains(text(), "退出")]',
            'xpath://a[contains(text(), "退出")]',
            'css:.log-out',
            'css:.logout'
        ]
        found_logout = False
        for sel in logout_selectors:
            btn = page.ele(sel, timeout=3)
            if btn:
                log.info(f'探测到旧会话，点击退出：{sel}')
                btn.click()
                found_logout = True
                time.sleep(3)
                # 处理注销确认弹窗
                _dismiss_popups(page)
                time.sleep(10) # 等待注销跳转完成
                # 再次刷新到登录页
                page.get(LOGIN_URL)
                time.sleep(5)
                break
        if not found_logout:
            log.info('未发现已登录特征，直接进入登录流程')
    except:
        pass

    log.info('开始填写账号密码 ...')
    try:
        # 探测输入框
        user_input = page.ele('xpath://input[contains(@placeholder, "账号") or contains(@placeholder, "账户") or contains(@placeholder, "手机号")]', timeout=15)
        if user_input:
            human_move(page)
            user_input.input(USERNAME, clear=True)
            time.sleep(random.uniform(0.8, 1.5))
            
            pwd_input = page.ele('xpath://input[@type="password" or contains(@placeholder, "密码")]', timeout=15)
            pwd_input.input(PASSWORD, clear=True)
            time.sleep(random.uniform(0.5, 1.0))
            
            login_btn = page.ele('xpath://button[contains(@class,"el-button--primary")]//span[contains(text(), "登录")]/..', timeout=15)
            login_btn.click()
            log.info('已提交登录表单')
        else:
            log.warning('未找到账号输入框，可能页面结构异常或加载失败')
    except Exception as e:
        log.warning(f'登录填写异常: {e}')

    # 处理「强制下线」弹窗
    time.sleep(5)
    _dismiss_popups(page)

    log.info('等待页面跳转 ...')
    time.sleep(25)
    log.info(f'当前 URL：{page.url}')

    # 【优化】精准定位并激活“应用”菜单
    log.info('执行菜单激活逻辑：点击“应用” -> 强制等待 -> 点击“概览”')
    try:
        # 使用提供的 li 标签精准特征
        app_menu = page.ele('xpath://li[@role="menuitem" and .//span[text()="应用"]]', timeout=30)
        if app_menu:
            app_menu.click()
            log.info('已点击“应用”菜单，执行激活等待 ...')
            time.sleep(30) # 保持用户要求的时长以激活图表
            
            overview_menu = page.ele('xpath://li[@role="menuitem" and .//span[text()="概览"]]', timeout=30)
            if overview_menu:
                overview_menu.click()
                log.info('已点击“概览”菜单，等待数据最终渲染 ...')
                time.sleep(35)
            else:
                log.warning('未找到“概览”菜单，尝试直接进入后续操作')
        else:
            log.warning('未找到“应用”菜单')
    except Exception as e:
        log.warning(f'菜单切换逻辑异常: {e}')


# ── 辅助函数 ──────────────────────────────────────────────────
def _screenshot(page: ChromiumPage, name: str) -> None:
    try:
        page.get_screenshot(path=DEBUG_DIR, name=name)
        log.info(f'[截图] → {name}')
    except: pass

def _dismiss_popups(page: ChromiumPage) -> None:
    """清理页面上各种可能的阻碍弹窗。"""
    selectors = [
        'xpath://button[.//span[contains(text(),"允许")]]',
        'css:.el-message-box__btns button.el-button--primary', # 通用 ElementUI 确定
        'xpath://button[normalize-space(.)="确定"]',
        'xpath://button[.//span[contains(normalize-space(),"确定")]]',
    ]
    for sel in selectors:
        try:
            btn = page.ele(sel, timeout=1)
            btn.click()
            log.info(f'[清理弹窗] {sel}')
            time.sleep(1)
        except: pass


# ── 导出 Excel ────────────────────────────────────────────────
def export_excel(page: ChromiumPage) -> None:
    log.info('准备进入 iframe 报表环境 ...')
    time.sleep(15)
    _dismiss_popups(page)

    frame = None
    for i in range(20):
        try:
            frame = page.get_frame('tag:iframe')
            if frame:
                log.info(f'已获取报表 iframe')
                break
        except: pass
        time.sleep(3)
    
    target = frame if frame else page

    # 【优化】智能探测数据卡片是否真正加载
    deadline = time.time() + 90
    log.info('正在观测图表数据是否加载成功...')
    while time.time() < deadline:
        card_bodies = target.eles('css:div.el-card__body')
        if any(len(cb.text.strip()) > 15 and '无数据' not in cb.text for cb in card_bodies):
            log.info('图表数据已加载完成！')
            break
        time.sleep(5)

    log.info('定位 UV 数据区并切换到列表模式 ...')
    try:
        uv_card = target.ele('xpath://div[contains(@class,"el-card") and .//*[contains(text(),"UV")]]', timeout=20)
        if uv_card:
            uv_card.run_js('this.scrollIntoView({block:"center"});')
            time.sleep(3)
            # 点击“列表视图”小图标
            tickets = uv_card.ele('css:i.el-icon-tickets', timeout=15)
            tickets.run_js('this.click();')
            log.info('已切换至列表视图')
    except Exception as e:
        log.error(f'UV 卡片操作失败: {e}')

    time.sleep(5)
    log.info('选择 30 天范围并触发导出 ...')
    try:
        # 选择 30 天（index=2 对应 UV 卡片内的 radio）
        radio_30 = target.ele('css:input[value="30days"]', index=2, timeout=15)
        if radio_30:
            radio_30.run_js('this.click();')
            time.sleep(10)
        
        # 点击导出按钮
        export_btn = target.ele('css:button[title="导出Excel"]', index=1, timeout=20)
        export_btn.run_js('this.click();')
        log.info('导出按钮已点击，正在处理下载确认 ...')
    except Exception as e:
        log.error(f'导出流程中断: {e}')
        return

    time.sleep(5)
    _dismiss_popups(page)
    _dismiss_popups(target)
    log.info('等待文件下传中 ...')
    time.sleep(10)


# ── 等待下载 ──────────────────────────────────────────────────
def wait_for_download(timeout: int = 150, not_before: float = None) -> str:
    if not_before is None: not_before = time.time() - 30
    log.info(f'等待 Excel 文件生成（最长 {timeout} 秒）...')
    deadline = time.time() + timeout
    while time.time() < deadline:
        for search_dir in [DOWNLOAD_DIR, DOWNLOAD_ROOT]:
            files = glob.glob(os.path.join(search_dir, '*.xlsx')) + glob.glob(os.path.join(search_dir, '*.xls'))
            tmp = glob.glob(os.path.join(search_dir, '*.crdownload'))
            # 只取本次运行之后生成的文件
            candidates = [f for f in files if os.path.getmtime(f) >= not_before]
            if candidates and not tmp:
                newest = max(candidates, key=os.path.getmtime)
                log.info(f'文件下载成功：{newest}')
                return newest
        time.sleep(3)
    raise TimeoutError('等待下载超时')


# ── 解析 Excel → 昨日日活合计 ─────────────────────────────────
def parse_dau(file_path: str) -> int:
    log.info(f'正在解析 Excel 报表：{file_path}')
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        log.error(f'Excel 读取失败: {e}')
        return 0

    log.info(f'报表列名：{df.columns.tolist()}')
    
    # 生成多格式目标日期
    y_str = YESTERDAY.strftime('%Y-%m-%d')
    y_mmdd = YESTERDAY.strftime('%m-%d')
    y_slash = YESTERDAY.strftime('%Y/%m/%d')
    log.info(f'匹配目标：{y_str} / {y_mmdd} / {y_slash}')

    # 【优化】增强日期列识别逻辑
    date_col = None
    # 策略 1: 检查列名关键词
    for col in df.columns:
        if any(k in str(col) for k in ['日期', '时间', 'Date', 'Time']):
            date_col = col
            log.info(f'已锁定日期列 (关键词): "{col}"')
            break
    
    # 策略 2: 探测前 10 行内容特征
    if not date_col:
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(10)
            if any(sample.str.contains(r'\d{1,2}[-/]\d{1,2}', regex=True)):
                date_col = col
                log.info(f'已锁定日期列 (内容特征): "{col}"')
                break
    
    # 兜底策略
    if not date_col:
        date_col = df.columns[0]
        log.warning(f'无法自动识别日期列，兜底尝试第一列: "{date_col}"')

    # 数据行过滤
    df[date_col] = df[date_col].astype(str)
    mask = (df[date_col].str.contains(y_str, na=False) | 
            df[date_col].str.contains(y_mmdd, na=False) |
            df[date_col].str.contains(y_slash, na=False))
    
    subset = df[mask]
    if subset.empty:
        log.warning('报表中未匹配到昨日日期行！')
        log.info(f'日期列头部样本：\n{df[date_col].head(5).tolist()}')
        return 0

    # 加总所有数值型列（通常为 UV 统计列）
    num_cols = subset.select_dtypes(include=['number']).columns
    if num_cols.empty:
        log.warning('未在匹配行中找到任何数值列')
        return 0
        
    total = int(subset[num_cols].sum().sum())
    log.info(f'解析成功，昨日 UV 合计：{total}')
    return total


# ── 写入数据库 ─────────────────────────────────────────────────
def write_db(dau: int) -> None:
    log.info(f'执行入库：date={YESTERDAY}, smart_frontend_dau={dau}')
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO platform_daily_metrics (stat_date, smart_frontend_dau)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE smart_frontend_dau = VALUES(smart_frontend_dau)
            """
            cur.execute(sql, (YESTERDAY, dau))
        conn.commit()
        log.info('数据库同步成功')
    finally:
        conn.close()


# ── 环境自愈辅助 ──────────────────────────────────────────────
def ensure_chrome_permissions() -> None:
    import json
    default_dir = os.path.join(CHROME_PROFILE, 'Default')
    pref_path   = os.path.join(default_dir, 'Preferences')
    site_key    = 'https://your-frontend.example.com:443,*'  # 请替换为实际前端系统域名
    os.makedirs(default_dir, exist_ok=True)
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
    except: pass

def kill_chrome_on_port(port: int) -> None:
    import subprocess
    try:
        res = subprocess.run(['netstat', '-ano'], capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if f':{port} ' in line and 'LISTENING' in line:
                pid = line.strip().split()[-1]
                if pid.isdigit():
                    subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True)
    except: pass
    try:
        if os.path.exists(os.path.join(CHROME_PROFILE, 'Default', 'Session Storage', 'LOCK')):
            subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe', '/FI', 'WINDOWTITLE eq *'], capture_output=True)
    except: pass

def clean_download_dir() -> None:
    for p in ('*.xlsx', '*.xls', '*.crdownload'):
        for f in glob.glob(os.path.join(DOWNLOAD_DIR, p)):
            try: os.remove(f)
            except: pass


# ── 主流程 ────────────────────────────────────────────────────
def main() -> None:
    log.info(f'===== 智能前端爬虫启动 | 目标日期：{YESTERDAY} =====')
    clean_download_dir()
    ensure_chrome_permissions()
    kill_chrome_on_port(CHROME_PORT)
    
    start_run_time = time.time()
    page = create_page()
    try:
        login(page)
        export_excel(page)
        file_path = wait_for_download(not_before=start_run_time)
    finally:
        # 为了稳定，稍微多留 2 秒让写盘完成
        time.sleep(2)
        page.quit()
        log.info('浏览器已关闭')

    dau = parse_dau(file_path)
    write_db(dau)
    log.info('===== 全部任务顺利完成 =====')

if __name__ == '__main__':
    main()
