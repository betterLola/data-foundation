import pandas as pd
import pymysql
import math

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',           # 数据库地址
    'port': 3306,                  # 数据库端口，默认 3306
    'user': 'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset': 'utf8mb4'
}

def clean_val(val):
    """处理 NaN 或 NaT 的值，转换为 None 以适配 MySQL"""
    if pd.isna(val):
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def import_data():
    excel_path = 'platform_mau.xlsx'
    
    print(f"正在读取 Excel 文件: {excel_path} ...")
    df = pd.read_excel(excel_path)
    
    # 打印原始列名，帮助确认是否与表字段一致
    print("Excel 表头列名:", df.columns.tolist())
    
    # === 重点处理 date_month 字段 ===
    date_col = 'date_month' 
    if date_col not in df.columns:
        date_col = df.columns[0]
        print(f"警告：未找到 'date_month' 列，尝试使用第一列 '{date_col}' 作为日期列。")

    def parse_date(val):
        """混合处理 Excel 日期序列号和中文日期"""
        if pd.isna(val):
            return None
            
        val_str = str(val).strip()
        
        # 1. 尝试处理 Excel 序列号 (如 44927)
        if val_str.isdigit():
            try:
                # Excel的基准日期是1899-12-30
                dt = pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val_str), unit='D')
                return dt
            except:
                pass
                
        # 2. 处理包含中文的格式 (如 "2022年1月")
        if '年' in val_str:
            val_str = val_str.replace('年', '-').replace('月', '').replace('日', '')
            
        # 3. 尝试转为 datetime
        try:
            return pd.to_datetime(val_str)
        except:
            return None

    # 应用转换函数，然后格式化为 YYYY-MM-01 (因为数据库是 DATE 类型，必须包含日)
    df[date_col] = df[date_col].apply(parse_date).dt.strftime('%Y-%m-01')
    
    print("日期处理后的数据预览:")
    print(df.head())

    print("正在连接数据库...")
    connection = pymysql.connect(**DB_CONFIG)
    
    try:
        with connection.cursor() as cursor:
            # 构建插入 SQL
            sql = """
            INSERT INTO platform_mau 
            (date_month, mau, mau_percent, total_register_users, dau, 
             monthly_avg_total_register_users, dau_percent, retention_percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            insert_count = 0
            for index, row in df.iterrows():
                # 提取数据，如果 Excel 的列名和数据库字段不一致，请修改这里的 key
                # 这里假设 Excel 的列名与数据库的字段名完全一致
                date_month_val = clean_val(row.get(date_col))
                mau = clean_val(row.get('mau'))
                mau_percent = clean_val(row.get('mau_percent'))
                total_register_users = clean_val(row.get('total_register_users'))
                dau = clean_val(row.get('dau'))
                monthly_avg_total_register_users = clean_val(row.get('monthly_avg_total_register_users'))
                dau_percent = clean_val(row.get('dau_percent'))
                retention_percent = clean_val(row.get('retention_percent'))
                
                cursor.execute(sql, (
                    date_month_val, mau, mau_percent, total_register_users, dau, 
                    monthly_avg_total_register_users, dau_percent, retention_percent
                ))
                insert_count += 1
                
        # 提交事务
        connection.commit()
        print(f"成功导入 {insert_count} 条数据到 platform_mau 表中！")
        
    except Exception as e:
        connection.rollback()
        print("导入失败，已回滚。错误信息:", e)
    finally:
        connection.close()

if __name__ == "__main__":
    import_data()
