# 讀取套件
import sqlite3

import requests
import pandas as pd
import datetime

# 建立資料表
'''CREATE TABLE stocks (
  symbol VARCHAR(255),
  type VARCHAR(255),
  exchange VARCHAR(255),
  market VARCHAR(255),
  date DATE,
  open DECIMAL(18,2),
  high DECIMAL(18,2),
  low DECIMAL(18,2),
  close DECIMAL(18,2),
  volume BIGINT,
  turnover DECIMAL(18,2),
  change DECIMAL(18,2),
  PRIMARY KEY (symbol, date)
);'''

# 連線 SQLite 資料庫
conn = sqlite3.connect('stock.db')
cursor = conn.cursor()


def get_symbols():
    '''
    取得股票代號
    '''
    symbol_link = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data'
    csv = pd.read_csv(symbol_link)
    symbols = csv['證券代號']
    return symbols


def gen_calendar():
    '''
    產生日期表
    '''
    this_year = datetime.datetime.now()
    years = range(2010, this_year.year + 1)  # Fugle提供的資料從2010年
    begin = [int(str(y) + '0101') for y in years]
    end = [int(str(y) + '1231') for y in years]
    calendar = pd.DataFrame({'begin': begin,
                             'end': end})
    calendar['begin'] = pd.to_datetime(calendar['begin'], format='%Y%m%d')
    calendar['end'] = pd.to_datetime(calendar['end'], format='%Y%m%d')
    calendar[['begin', 'end']] = calendar[['begin', 'end']].astype('str')
    return calendar


def get_hist_data(symbols: list):
    '''
    透過富果Fugle API抓取歷史資料
    '''
    if len(symbols) == 0:
        symbols = get_symbols()
    calendar = gen_calendar()
    result = pd.DataFrame()
    for i in range(len(symbols)):
        cur_symbol = symbols[i]
        symbol_result = pd.DataFrame()
        for j in range(len(calendar)):
            cur_begin = calendar.loc[j, 'begin']
            cur_end = calendar.loc[j, 'end']
            # 透過富果Fugle API抓取歷史資料
            data_link = f'https://api.fugle.tw/marketdata/v0.3/candles?symbolId=2884&apiToken=demo&from={cur_begin}&to={cur_end}&fields=open,high,low,close,volume,turnover,change'
            resp = requests.get(url=data_link)
            stock_detail = resp.json()
            data_by_date = stock_detail.pop('data')
            for data in data_by_date:
                for key, value in data.items():
                    if stock_detail.get(key):
                        stock_detail[key].append(value)
                    else:
                        stock_detail[key] = [value]
            new_result = pd.DataFrame.from_dict(stock_detail)
            symbol_result = pd.concat([symbol_result, new_result])
        symbol_result['symbol'] = cur_symbol
        result = pd.concat([result, symbol_result])
    return result


# 定義欄位名稱及其對應的資料類型
columns = {
    'symbol': 'VARCHAR(255)',
    'type': 'VARCHAR(255)',
    'exchange': 'VARCHAR(255)',
    'market': 'VARCHAR(255)',
    'date': 'DATE',
    'open': 'DECIMAL(18,2)',
    'high': 'DECIMAL(18,2)',
    'low': 'DECIMAL(18,2)',
    'close': 'DECIMAL(18,2)',
    'volume': 'BIGINT',
    'turnover': 'DECIMAL(18,2)',
    'change': 'DECIMAL(18,2)'
}


def write_db(_data):
    table_name = 'stocks'
    # 寫入資料
    for _, row in _data.iterrows():
        values_pos = ','.join(['?' for _ in columns])
        sqlite_insert_sql = f"INSERT INTO {table_name} VALUES ({values_pos}) ON CONFLICT(symbol, date) DO NOTHING"
        mysql_insert_sql = f"INSERT IGNORE INTO {table_name} VALUES ({values_pos})"
        cursor.execute(sqlite_insert_sql, tuple(row[columns.keys()]))

    # 提交變更並關閉資料庫連線
    conn.commit()
    conn.close()


# 全部股票歷史資料
# data = get_hist_data()
# 單一個股歷史資料 - 2330台積電
data = get_hist_data(symbols=[])
write_db(data)
