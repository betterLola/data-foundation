import os
import pandas as pd
import pymysql
from datetime import datetime
import glob

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',           # 数据库地址
    'port': 3306,                  # 数据库端口，默认 3306
    'user': 'root',                # 数据库用户名
    'password': '更换为自己的MySQL密码',
    'database': 'daily',
    'charset': 'utf8mb4'
}

def create_table(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS 5100_detail (
            id INT AUTO_INCREMENT PRIMARY KEY,
            service_amount BIGINT,
            resource_name VARCHAR(255),
            service_name VARCHAR(255),
            stat_date DATE,
            port VARCHAR(50)
        )
    ''')

def get_port_from_filename(filename):
    if 'Android' in filename:
        return '安卓'
    elif 'iPhone' in filename:
        return '苹果'
    elif 'Harmony' in filename:
        return '鸿蒙'
    return '未知'

def process_file(filepath, cursor):
    filename = os.path.basename(filepath)
    port = get_port_from_filename(filename)
    resource_name = '510100_items'
    
    print(f"Processing {filename} (Port: {port})...")
    
    # Try different encodings
    df = None
    for enc in ['utf-8-sig', 'gbk', 'gb2312', 'utf-8']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            break
        except Exception:
            pass
            
    if df is None:
        print(f"Failed to read {filename} with known encodings.")
        return
        
    # Standardize column names based on position
    # The columns are expected to be: 日期, 参数值, 消息数量, 占比
    if len(df.columns) < 3:
        print(f"File {filename} has unexpected number of columns: {len(df.columns)}")
        return
        
    # Convert dates
    # Assuming first column is date, second is parameter value, third is message amount
    date_col = df.columns[0]
    param_col = df.columns[1]
    amount_col = df.columns[2]
    
    # Drop rows with NaN in essential columns
    df = df.dropna(subset=[date_col, param_col, amount_col])
    
    # Parse dates safely
    df['parsed_date'] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False, errors='coerce')
    df = df.dropna(subset=['parsed_date'])
    
    # Prepare data for insertion
    insert_query = '''
        INSERT INTO 5100_detail (service_amount, resource_name, service_name, stat_date, port)
        VALUES (%s, %s, %s, %s, %s)
    '''
    
    # Batch insert
    batch_size = 5000
    data_to_insert = []
    
    for _, row in df.iterrows():
        try:
            amount = int(row[amount_col])
            service_name = str(row[param_col]).strip()
            stat_date = row['parsed_date'].strftime('%Y-%m-%d')
            
            data_to_insert.append((amount, resource_name, service_name, stat_date, port))
            
            if len(data_to_insert) >= batch_size:
                cursor.executemany(insert_query, data_to_insert)
                data_to_insert = []
        except Exception as e:
            # print(f"Error parsing row {row}: {e}")
            pass
            
    if data_to_insert:
        cursor.executemany(insert_query, data_to_insert)

def main():
    data_dir = r".\历史数据\成都所有事项"  # 请替换为实际的历史数据目录路径
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    if not csv_files:
        print("No CSV files found in the directory.")
        return

    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            create_table(cursor)
            
            for file in csv_files:
                process_file(file, cursor)
                conn.commit()  # commit after each file
                
        print("Data import completed successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
