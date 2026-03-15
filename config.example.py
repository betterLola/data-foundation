# -*- coding: utf-8 -*-
"""
config.example.py - 项目统一配置模板
======================================
使用方法：
  1. 复制本文件为 config.py
     cp config.example.py config.py
  2. 填写 config.py 中的真实配置值
  3. config.py 已在 .gitignore 中被排除，不会上传到 Git

所有脚本优先从 config.py 读取配置，若 config.py 不存在则使用各脚本内的内联配置。
"""

# ==============================================================
# 1. MySQL 数据库配置
# ==============================================================
DB_CONFIG = {
    'host':     'localhost',           # 数据库主机地址（通常为 localhost）
    'port':     3306,                  # 端口，默认 3306
    'user':     'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',  # ← 替换为实际密码
    'database': 'daily',              # 目标数据库名
    'charset':  'utf8mb4',
}

# ==============================================================
# 2. 友盟 Open API 配置
#    获取方式：https://developer.umeng.com/ → 账号设置 → API Key
# ==============================================================
UMENG_API_KEY      = "更换为友盟API_KEY"       # ← 替换为实际 API Key
UMENG_API_SECRET   = "更换为友盟API_SECRET"    # ← 替换为实际 API Secret

# 原生 APP AppKey（友盟控制台 → 对应应用 → AppKey）
UMENG_APP_APPKEYS = {
    "安卓":  "更换为安卓端AppKey",              # ← 替换
    "苹果":  "更换为苹果端AppKey",              # ← 替换
    "鸿蒙":  "更换为鸿蒙端AppKey",              # ← 替换
}

# 小程序 DataSourceId（友盟控制台 → 对应小程序 → DataSourceId）
UMENG_MINI_APPKEYS = {
    "微信小程序":   "更换为微信小程序DataSourceId",  # ← 替换
    "支付宝小程序": "更换为支付宝小程序DataSourceId", # ← 替换
}

# 留存率查询平台（key 对应友盟平台标识，value 对应 AppKey）
UMENG_RETENTION_PLATFORMS = {
    "ios":     "更换为iOS端AppKey",     # ← 替换
    "android": "更换为安卓端AppKey",    # ← 替换
    "harmony": "更换为鸿蒙端AppKey",    # ← 替换
}

# ==============================================================
# 3. 内网系统爬虫配置（internal_network_spider.py）
#    用于爬取内网平台的新增注册/实名用户数
# ==============================================================
INTRANET_LOGIN_URL      = "更换为内网系统登录地址"  # ← 替换，例：https://intranet.example.com/login
INTRANET_LOGIN_USERNAME = "更换为内网系统账号"       # ← 替换
INTRANET_LOGIN_PASSWORD = "更换为内网系统密码"       # ← 替换

# Chrome 调试端口（避免与其他 Chrome 实例冲突，可自定义）
INTRANET_CHROME_PORT    = 9336
# Chrome 专用 Profile 目录（用于持久化登录状态）
INTRANET_CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\internal_spider_chrome'
# 日志输出目录
INTRANET_LOG_DIR        = r'C:\Users\YOUR_USERNAME\logs'

# ==============================================================
# 4. 智能前端系统爬虫配置（smart_frontend_dau_spider.py）
#    用于爬取智能前端平台的日活 UV 数据
# ==============================================================
FRONTEND_LOGIN_URL      = '更换为智能前端系统登录地址'  # ← 替换
FRONTEND_LOGIN_USERNAME = '更换为系统账号'               # ← 替换
FRONTEND_LOGIN_PASSWORD = '更换为系统密码'               # ← 替换

# Chrome 调试端口（与内网爬虫使用不同端口）
FRONTEND_CHROME_PORT    = 9335
FRONTEND_CHROME_PROFILE = r'C:\Users\YOUR_USERNAME\AppData\Local\smart_spider_chrome'
FRONTEND_DOWNLOAD_DIR   = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau'
FRONTEND_DOWNLOAD_ROOT  = r'C:\Users\YOUR_USERNAME\Downloads'
FRONTEND_DEBUG_DIR      = r'C:\Users\YOUR_USERNAME\Downloads\smart_frontend_dau\debug'

# ==============================================================
# 5. 告警配置（可选，用于脚本失败时推送通知）
# ==============================================================
# 钉钉机器人 Webhook（在钉钉群 → 智能群助手 → 自定义机器人中获取）
DINGTALK_WEBHOOK = ""   # ← 填入 webhook URL，留空则不发送钉钉告警

# 邮件告警配置（留空则不发送邮件）
ALERT_EMAIL_SMTP_HOST    = ""      # 例：smtp.qq.com
ALERT_EMAIL_SMTP_PORT    = 465
ALERT_EMAIL_SENDER       = ""      # 发件人邮箱
ALERT_EMAIL_PASSWORD     = ""      # 邮箱授权码（非登录密码）
ALERT_EMAIL_RECEIVERS    = []      # 收件人列表，例：["ops@example.com"]

# ==============================================================
# 6. 数据质量监控阈值（main.py 末尾自动运行）
# ==============================================================
# 日活波动告警阈值（与前日相比，跌幅超过该比例则告警）
DAU_DROP_THRESHOLD = 0.5    # 默认 50%
# 最低合理日活（低于此值视为数据异常）
DAU_MIN_VALID      = 100
