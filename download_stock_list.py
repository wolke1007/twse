import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import urllib.parse
from sqlalchemy import create_engine


# 從臺灣證券交易所頁面進行爬蟲取得列表
url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
res = requests.get(url)

soup = BeautifulSoup(res.text, "lxml")
tr = soup.findAll('tr')

tds = []
for raw in tr:
     data = [td.get_text() for td in raw.findAll("td")]
     if len(data) == 7:
         tds.append(data)

stock_list = pd.DataFrame(tds[1:], columns=tds[0])

# 拆分 "有價證券代號及名稱" 成 "有價證券代號" 和 "有價證券名稱"
stock_list[['有價證券代號', '有價證券名稱']] = stock_list['有價證券代號及名稱 '].str.split('　', expand=True)

# 重命名 DataFrame 欄位以匹配資料庫中的欄位名稱
stock_list.rename(columns={
    '有價證券代號': 'stock_code',
    '有價證券名稱': 'stock_name',
    '國際證券辨識號碼(ISIN Code)': 'isin_code',
    '上市日': 'listing_date',
    '市場別': 'market',
    '產業別': 'industry',
    'CFICode': 'cfi_code',
    '備註': 'remarks'
}, inplace=True)

# 將 "上市日" 轉換為日期格式
stock_list['listing_date'] = pd.to_datetime(stock_list['listing_date'], format='%Y/%m/%d')

# 選擇與資料庫表格對應的欄位
stock_list = stock_list[['stock_code', 'stock_name', 'isin_code', 'listing_date', 'market', 'industry', 'cfi_code', 'remarks']]


# -- stock.stock_info definition
#
# CREATE TABLE `stock_info` (
#   `stock_code` text DEFAULT NULL,
#   `stock_name` text DEFAULT NULL,
#   `isin_code` text DEFAULT NULL,
#   `listing_date` datetime DEFAULT NULL,
#   `market` text DEFAULT NULL,
#   `industry` text DEFAULT NULL,
#   `cfi_code` text DEFAULT NULL,
#   `remarks` text DEFAULT NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


# 連接到 MariaDB 資料庫
user = ''
password = ''
# 对密码进行编码
encoded_password = urllib.parse.quote_plus(password)
host = '192.168.1.138'
database = 'stock'
port = '3307'

engine = create_engine(f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}")

# 將資料寫入資料庫
try:
    stock_list.to_sql('stock_info', con=engine, if_exists='replace', index=False)
    print("Data inserted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# 關閉資料庫連接
engine.dispose()
