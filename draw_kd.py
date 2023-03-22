import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 連接到 SQLite 資料庫
conn = sqlite3.connect('stock.db')

by_date_sql_command = "SELECT * FROM stocks WHERE symbol=='2002' AND date < '2021-01-01';"
by_month_sql_command = """
SELECT * FROM stocks
WHERE symbol='2002'
AND (strftime('%d', date) = '01' OR strftime('%d', date) = '04')
AND date < '2023-01-01';
"""

# 從 SQLite 資料庫讀取資料到 pandas dataframe
df = pd.read_sql_query(by_month_sql_command, conn)

# 將日期欄位轉換為 pandas datetime 格式
df['date'] = pd.to_datetime(df['date'])

df.sort_values(by=['date'])

# 計算 KD 值
high_list = df['high'].rolling(window=9, min_periods=9).max()
low_list = df['low'].rolling(window=9, min_periods=9).min()
rsv = (df['close'] - low_list) / (high_list - low_list) * 100
df['k'] = rsv.ewm(com=2).mean()
df['d'] = df['k'].ewm(com=2).mean()
df = df.dropna()  # 刪除包含 NaN 值的行

# 繪製 KD 線圖
plt.figure(figsize=(20,15))
plt.plot(df['date'], df['k'], label='K')
plt.plot(df['date'], df['d'], label='D')
plt.legend()
plt.title('KD Line Chart')
plt.xlabel('Date')
plt.ylabel('KD Value')
plt.yticks([_ for _ in range(0, 100, 5)])
plt.grid(color='r', linestyle='--', linewidth=1)
plt.show()
