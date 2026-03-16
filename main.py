import os
import logging
import subprocess
import sys
import json
import pymysql
import traceback
from datetime import datetime, timedelta

# 回填模块（检查并补填近7天漏填数据）
import data_backfilling

# ── 尝试从统一配置文件加载（推荐方式） ─────────────────────────
try:
    from config import DB_CONFIG, DINGTALK_WEBHOOK, DAU_DROP_THRESHOLD, DAU_MIN_VALID
except ImportError:
    # config.py 不存在时使用内联配置，请参考 config.example.py 创建 config.py
    DB_CONFIG = {
        'host': 'localhost',
        'port': 3306,                  # 数据库端口，默认 3306
        'user': 'root',
        'password': '更换为自己的MySQL密码',
        'database': 'daily',
        'charset': 'utf8mb4'
    }
    DINGTALK_WEBHOOK  = ""    # 钉钉机器人 Webhook，留空则不发送告警
    DAU_DROP_THRESHOLD = 0.5  # 日活跌幅告警阈值（默认 50%）
    DAU_MIN_VALID      = 100  # 最低合理日活（低于此值视为异常）

# ── 日志配置 ─────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"main_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

def run_backfill():
    """
    调用 data_backfilling 模块，检查并回填近7天漏填数据。
    覆盖范围：
      - platform_daily_metrics: android/ios/harmony/app/mini/alipay DAU（友盟API）
      - platform_daily_metrics: smart_frontend_dau（智能前端爬虫）
      - platform_daily_metrics: new_register_users / new_realname_users（内网爬虫）
      - resource_total: 各端事件数据（友盟API）
      - 5100_detail: 510100_items 子服务明细（友盟API）
    每个数据源独立容错，单项失败不影响其余项。
    """
    print("==================================================")
    print("🔁 开始执行近7天数据回填检查")
    print("==================================================")
    try:
        data_backfilling.main()
        print("✅ 数据回填检查执行完成\n")
    except Exception as e:
        print(f"❌ 数据回填执行异常: {e}\n")
        traceback.print_exc()


def run_script(script_name):
    """
    使用当前 Python 解释器执行指定的脚本文件
    """
    print(f"==================================================")
    print(f"🚀 开始执行脚本: {script_name}")
    print(f"==================================================")
    try:
        # 使用当前运行的 python 解释器执行脚本
        # 增加环境变量配置，确保字符集正确
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        result = subprocess.run(
            [sys.executable, script_name], 
            check=True, 
            capture_output=True, 
            text=True, 
            encoding='utf-8', 
            errors='replace',
            env=env
        )
        print(result.stdout)
        print(f"✅ {script_name} 执行成功！\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_name} 执行失败！退出码: {e.returncode}")
        print(f"标准输出:\n{e.stdout}")
        print(f"标准错误:\n{e.stderr}\n")
        return False
    except Exception as e:
        print(f"❌ {script_name} 发生未知错误: {e}\n")
        return False

def update_total_service_times(target_date_str):
    """
    计算并更新昨日的累计服务总次数 (total_service_times)
    逻辑：前天的累计总次数 + 昨天的单日服务总次数 = 昨天的累计总次数
    """
    print("==================================================")
    print(f"🔄 开始计算并更新 {target_date_str} 的 total_service_times")
    print("==================================================")
    
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        previous_date = target_date - timedelta(days=1)
        
        # 1. 获取昨天 (target_date) 5100_detail 中的所有服务次数总和
        cursor.execute("SELECT SUM(service_amount) FROM 5100_detail WHERE stat_date = %s", (target_date,))
        yesterday_sum_result = cursor.fetchone()
        yesterday_service_amount_sum = int(yesterday_sum_result[0]) if yesterday_sum_result and yesterday_sum_result[0] is not None else 0
        
        print(f"[{target_date_str}] 昨日本地 5100 服务总次数: {yesterday_service_amount_sum}")
        
        # 2. 获取前天 (previous_date) platform_daily_metrics 的累计总次数
        cursor.execute("SELECT total_service_times FROM platform_daily_metrics WHERE stat_date = %s", (previous_date,))
        previous_total_result = cursor.fetchone()
        
        if not previous_total_result:
             print(f"⚠️ 警告: 数据库中找不到前天 ({previous_date}) 的累计记录，无法进行累加！")
             print(f"ℹ️ 建议：请手动初始化前天或更早日期的 total_service_times 基础值。")
             return
             
        previous_total_service_times = int(previous_total_result[0]) if previous_total_result[0] is not None else 0
        print(f"[{previous_date}] 前天累计总次数: {previous_total_service_times}")
        
        # 3. 计算昨日新的累计总次数
        new_total_service_times = previous_total_service_times + yesterday_service_amount_sum
        print(f"➡️  计算得出 [{target_date_str}] 新累计总次数: {new_total_service_times}")
        
        # 4. 写入数据库
        cursor.execute("SELECT 1 FROM platform_daily_metrics WHERE stat_date = %s", (target_date,))
        if cursor.fetchone():
            update_sql = "UPDATE platform_daily_metrics SET total_service_times = %s WHERE stat_date = %s"
            cursor.execute(update_sql, (new_total_service_times, target_date))
            print(f"✅ 已更新 {target_date_str} 的 total_service_times。")
        else:
            insert_sql = "INSERT INTO platform_daily_metrics (stat_date, total_service_times) VALUES (%s, %s)"
            cursor.execute(insert_sql, (target_date, new_total_service_times))
            print(f"✅ 已插入 {target_date_str} 的新记录及累计次数。")

        conn.commit()
    except Exception as e:
        print(f"❌ 更新 total_service_times 时发生错误: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def update_daily_aggregates(target_date_str):
    """
    计算并更新昨日的综合指标：
    1. platform_dau = 各端日活总和
    2. total_register_users = 前日累计注册 + 昨日新增注册
    3. total_realname_users = 前日累计实名 + 昨日新增实名
    """
    print("==================================================")
    print(f"🔄 开始计算并更新 {target_date_str} 的汇总指标 (DAU/累计用户)")
    print("==================================================")
    
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        previous_date = target_date - timedelta(days=1)
        
        # --- A. 获取昨日所有基础日活数据 ---
        cursor.execute("""
            SELECT android_dau, ios_dau, harmonyos_dau, alipay_dau, mini_program_dau, smart_frontend_dau, 
                   new_register_users, new_realname_users
            FROM platform_daily_metrics 
            WHERE stat_date = %s
        """, (target_date,))
        today_data = cursor.fetchone()
        
        if not today_data:
            print(f"⚠️ 警告: 找不到 {target_date_str} 的基础记录，无法计算汇总指标。")
            return

        # 计算 platform_dau
        platform_dau = (
            (today_data.get('android_dau') or 0) +
            (today_data.get('ios_dau') or 0) +
            (today_data.get('harmonyos_dau') or 0) +
            (today_data.get('alipay_dau') or 0) +
            (today_data.get('mini_program_dau') or 0) +
            (today_data.get('smart_frontend_dau') or 0)
        )
        print(f"[{target_date_str}] 计算得出全平台总日活 (platform_dau): {platform_dau}")

        # --- B. 获取前日累计数据 ---
        cursor.execute("""
            SELECT total_register_users, total_realname_users 
            FROM platform_daily_metrics 
            WHERE stat_date = %s
        """, (previous_date,))
        prev_data = cursor.fetchone()
        
        if not prev_data:
            print(f"⚠️ 警告: 找不到前天 ({previous_date}) 的记录，将仅使用昨日新增值作为初始累计。")
            prev_total_reg = 0
            prev_total_real = 0
        else:
            prev_total_reg = prev_data.get('total_register_users') or 0
            prev_total_real = prev_data.get('total_realname_users') or 0

        # 计算昨日新的累计值
        new_total_reg = prev_total_reg + (today_data.get('new_register_users') or 0)
        new_total_real = prev_total_real + (today_data.get('new_realname_users') or 0)
        
        print(f"[{target_date_str}] 计算得出累计注册: {new_total_reg} (增量: {today_data.get('new_register_users') or 0})")
        print(f"[{target_date_str}] 计算得出累计实名: {new_total_real} (增量: {today_data.get('new_realname_users') or 0})")

        # --- C. 更新数据库 ---
        update_sql = """
            UPDATE platform_daily_metrics 
            SET platform_dau = %s,
                total_register_users = %s,
                total_realname_users = %s
            WHERE stat_date = %s
        """
        cursor.execute(update_sql, (platform_dau, new_total_reg, new_total_real, target_date))
        conn.commit()
        print(f"✅ 已成功更新 {target_date_str} 的汇总指标字段。")

    except Exception as e:
        print(f"❌ 计算更新汇总指标时发生错误: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def deduplicate_all_tables():
    """
    遍历数据库中的所有表，如果存在完全重复的行（所有列都相同），则仅保留一条。
    """
    print("==================================================")
    print(f"🧹 开始执行全库表去重清洗")
    print("==================================================")
    
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 获取所有表名
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            print(f"正在检查表: {table} ...", end=" ")
            
            # 使用临时表去重法：
            # 1. 创建结构相同的临时表并填充去重后的数据
            # 2. 清空原表
            # 3. 将去重数据导回
            
            # 注意：此操作会处理“完全重复”的行。
            # 如果表有自增主键且主键值不同，则不被视为完全重复。
            # 但对于只有业务字段的表（如 5100_detail 或 retention），效果显著。
            
            cursor.execute(f"CREATE TEMPORARY TABLE temp_dedup AS SELECT DISTINCT * FROM `{table}`")
            
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            old_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM temp_dedup")
            new_count = cursor.fetchone()[0]
            
            diff = old_count - new_count
            if diff > 0:
                cursor.execute(f"TRUNCATE TABLE `{table}`")
                cursor.execute(f"INSERT INTO `{table}` SELECT * FROM temp_dedup")
                print(f"✅ 已清理 {diff} 条重复数据。")
            else:
                print(f"🆗 无重复行。")
            
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_dedup")
            
        conn.commit()
        print("✨ 全库去重清洗完成。")
    except Exception as e:
        print(f"❌ 去重清洗时发生错误: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def send_dingtalk_alert(title: str, content: str) -> None:
    """
    向钉钉机器人发送告警消息。
    需要在 config.py 中配置 DINGTALK_WEBHOOK。
    """
    if not DINGTALK_WEBHOOK:
        return
    try:
        import urllib.request
        payload = json.dumps({
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n\n{content}"
            }
        }).encode("utf-8")
        req = urllib.request.Request(
            DINGTALK_WEBHOOK,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=5)
        log.info("钉钉告警已发送")
    except Exception as e:
        log.warning(f"钉钉告警发送失败: {e}")


def run_data_quality_check(target_date_str: str) -> list:
    """
    数据质量检查：
    1. 核心字段空值检测
    2. 日活异常跌幅告警（与前日相比跌幅超过阈值）
    3. 日活低于合理最小值告警

    :param target_date_str: 目标日期字符串（YYYY-MM-DD）
    :return: 告警信息列表（空则表示质量正常）
    """
    log.info("=" * 50)
    log.info("🔍 开始执行数据质量检查")
    log.info("=" * 50)

    alerts = []
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        previous_date = target_date - timedelta(days=1)

        # 1. 读取今日和昨日数据
        cursor.execute(
            "SELECT * FROM platform_daily_metrics WHERE stat_date IN (%s, %s)",
            (target_date, previous_date)
        )
        rows = {str(row["stat_date"]): row for row in cursor.fetchall()}

        today = rows.get(target_date_str)
        yesterday = rows.get(str(previous_date))

        if not today:
            alerts.append(f"❌ [{target_date_str}] platform_daily_metrics 中无任何记录，数据采集可能全部失败！")
            return alerts

        # 2. 核心字段空值检测
        critical_fields = [
            "android_dau", "ios_dau", "harmonyos_dau",
            "new_register_users", "platform_dau"
        ]
        null_fields = [f for f in critical_fields if today.get(f) is None]
        if null_fields:
            alerts.append(f"⚠️ [{target_date_str}] 核心字段为空：{', '.join(null_fields)}")
            log.warning(f"核心字段空值：{null_fields}")

        # 3. 日活异常跌幅检测
        if yesterday:
            dau_fields = ["android_dau", "ios_dau", "harmonyos_dau", "platform_dau"]
            for field in dau_fields:
                prev_val = yesterday.get(field) or 0
                curr_val = today.get(field) or 0

                if prev_val > 0 and curr_val < DAU_MIN_VALID:
                    alerts.append(
                        f"⚠️ [{target_date_str}] {field}={curr_val}，低于最低合理值 {DAU_MIN_VALID}")

                if prev_val > 0 and curr_val > 0:
                    drop_rate = (prev_val - curr_val) / prev_val
                    if drop_rate > DAU_DROP_THRESHOLD:
                        alerts.append(
                            f"📉 [{target_date_str}] {field} 较前日跌幅 {drop_rate:.1%}"
                            f"（{prev_val:,} → {curr_val:,}），超过告警阈值 {DAU_DROP_THRESHOLD:.0%}"
                        )
                        log.warning(f"{field} 异常跌幅：{drop_rate:.1%}")

        if not alerts:
            log.info(f"✅ [{target_date_str}] 数据质量检查通过，无异常")
        else:
            for msg in alerts:
                log.warning(msg)

    except Exception as e:
        log.error(f"数据质量检查异常: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

    return alerts


def main():
    # 获取昨天的日期字符串作为目标日期
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    log.info(f"🌟 统一数据采集主流程启动")
    log.info(f"📅 执行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"📅 数据目标日期：{target_date}")
    print(f"🌟 统一数据采集主流程启动 🌟")
    print(f"📅 执行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 数据目标日期：{target_date}")

    # 待执行的脚本列表（按逻辑依赖顺序排列）
    scripts_to_run = [
        "UmengAPI.py",                  # 友盟核心日活数据 (android_dau, ios_dau, harmonyos_dau, app_dau 等)
        "5100_detail.py",               # 成都所有事项 510100_items 细分服务次数
        "internal_network_spider.py",   # 内网数据 (new_register_users, new_realname_users)
        "smart_frontend_dau_spider.py", # 智能前端平台日活爬虫
        "fetch_retention.py",           # 留存率数据获取
        "resource_total.py",            # 资源大盘总计数据 (昨日快照)
    ]

    all_success = True
    failed_scripts = []

    # 依次执行各采集模块
    for script in scripts_to_run:
        if not os.path.exists(script):
            log.warning(f"找不到模块文件 {script}，跳过执行。")
            print(f"⚠️ 报错：找不到模块文件 {script}，跳过执行。")
            all_success = False
            failed_scripts.append(script)
            continue

        success = run_script(script)
        if not success:
            all_success = False
            failed_scripts.append(script)
            log.warning(f"{script} 运行遇到问题，已跳过。")
            print(f"⚠️ {script} 运行遇到问题，已跳过。")

    # 计算并更新昨日累计服务总次数
    print("\n--- 正在执行最后的逻辑汇总 ---")
    update_total_service_times(target_date)

    # 计算并更新 platform_dau, total_register_users, total_realname_users
    update_daily_aggregates(target_date)

    # 全库去重逻辑
    deduplicate_all_tables()

    # 近7天数据回填：修复因任务失败/网络异常导致的历史数据缺口
    # 在当日采集和聚合完成后执行，确保补填数据与当日流程不冲突
    print("\n--- 正在执行近7天数据回填检查 ---")
    run_backfill()

    # ── 数据质量检查 ─────────────────────────────────────────
    quality_alerts = run_data_quality_check(target_date)

    # ── 最终结果汇总与告警 ───────────────────────────────────
    print("\n" + "="*50)
    if all_success:
        log.info(f"🎉 任务圆满完成！{target_date} 的所有字段数据已成功更新至 daily 数据库。")
        print(f"🎉 任务圆满完成！{target_date} 的所有字段数据已成功更新至 daily 数据库。")
    else:
        msg = f"🛑 任务执行完毕，但以下模块存在异常：{', '.join(failed_scripts)}"
        log.error(msg)
        print(msg)
        for fs in failed_scripts:
            print(f"   - {fs} (失败)")
        print(f"请检查上述脚本日志进行人工补数。")

        # 脚本失败时发送钉钉告警
        alert_content = (
            f"**执行日期**：{target_date}  \n"
            f"**失败模块**：{', '.join(failed_scripts)}  \n"
            f"**执行时间**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n"
            f"请检查日志目录 `logs/` 进行排查。"
        )
        send_dingtalk_alert("⚠️ 数据采集任务失败告警", alert_content)

    # 数据质量问题也发送告警
    if quality_alerts:
        quality_content = "  \n".join(quality_alerts)
        send_dingtalk_alert("📊 数据质量异常告警", f"**目标日期**：{target_date}  \n\n{quality_content}")

    print("="*50)
    log.info(f"主流程结束，日志已写入 logs/main_{datetime.now().strftime('%Y%m%d')}.log")

if __name__ == "__main__":
    main()
