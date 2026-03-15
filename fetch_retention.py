# -*- coding: utf-8 -*-
import pymysql
import aop
import aop.api
from datetime import datetime, timedelta

# 数据库配置（不包含 database，防止库未创建报错）
DB_CONFIG = {
    'host': 'localhost',           # 数据库地址
    'port': 3306,                  # 数据库端口，默认 3306
    'user': 'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'charset': 'utf8mb4'
}
DB_NAME = 'daily'

# 友盟配置
API_KEY = "更换为友盟API_KEY"        # 友盟开放平台 API Key
API_SECURITY = "更换为友盟API_SECRET"  # 友盟开放平台 API Secret

PLATFORMS = {
    "ios": "更换为iOS端AppKey",
    "android": "更换为安卓端AppKey",
    "harmony": "更换为鸿蒙端AppKey",
}

def init_db():
    # 1. 连接 MySQL 并创建数据库（如果不存在）
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4;")
    conn.commit()
    
    # 2. 切换到目标数据库
    cursor.execute(f"USE `{DB_NAME}`;")
    
    # 3. 创建数据表 (包含 id, 平台, 统计日期, 次留率)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `app_retention` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `platform` VARCHAR(20) NOT NULL COMMENT '平台名称(ios/android/harmony)',
        `stat_date` DATE NOT NULL COMMENT '统计日期',
        `day_1_retention` DECIMAL(5, 2) COMMENT '次日留存率(%)',
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY `uk_platform_date` (`platform`, `stat_date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='App次日留存率统计表';
    """
    cursor.execute(create_table_sql)
    conn.commit()
    return conn, cursor

def fetch_and_save_retention():
    # 设置网关和密钥
    aop.set_default_server('gateway.open.umeng.com')
    aop.set_default_appinfo(API_KEY, API_SECURITY)
    
    # 获取最近14天的数据
    # 次留是指某天新增的用户在次日的活跃情况，因此查询前天到15天前的更可靠
    end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
    
    print(f"开始获取 {start_date} 至 {end_date} 的次日留存数据...")

    try:
        conn, cursor = init_db()
        print(f"成功连接数据库并初始化表结构。")
    except Exception as e:
        print(f"数据库连接失败，请检查MySQL服务是否启动及账号密码：{e}")
        return
        
    insert_sql = """
    INSERT INTO `app_retention` (`platform`, `stat_date`, `day_1_retention`)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE `day_1_retention` = VALUES(`day_1_retention`);
    """
    
    for platform_name, appkey in PLATFORMS.items():
        print(f"\\n正在获取 [{platform_name}] 留存数据...")
        req = aop.api.UmengUappGetRetentionsRequest()
        try:
            resp = req.get_response(None, appkey=appkey, startDate=start_date, endDate=end_date)
            retention_info_list = resp.get('retentionInfo', [])
            
            if not retention_info_list:
                print(f"  - 未获取到数据或接口返回空")
                continue
                
            for info in retention_info_list:
                stat_date = info.get('date')
                rates = info.get('retentionRate', [])
                if rates and len(rates) > 0:
                    day_1_retention = rates[0]
                    cursor.execute(insert_sql, (platform_name, stat_date, day_1_retention))
                    print(f"  - 日期: {stat_date}, 次留: {day_1_retention}% 已更新入库")
                else:
                    print(f"  - 日期: {stat_date}, 暂无次留数据")
                    
        except Exception as e:
            print(f"  ! 获取 [{platform_name}] 数据失败: {e}")
            
    conn.commit()
    cursor.close()
    conn.close()
    print("\\n所有数据拉取与入库完成！")

if __name__ == "__main__":
    fetch_and_save_retention()
