import config

import time
import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas as pd
from datetime import datetime


# CREATE TABLE `historical_data` (
#   `stock_code` varchar(30) NOT NULL COMMENT '股票代號',
#   `trade_date` date NOT NULL COMMENT '交易日期',
#   `open` float NOT NULL COMMENT '開盤價',
#   `high` float NOT NULL COMMENT '當日最高價',
#   `low` float NOT NULL COMMENT '當日最低價',
#   `close` float NOT NULL COMMENT '收盤價',
#   `volume` float NOT NULL COMMENT '成交量',
#   `dividends` float NOT NULL COMMENT '股利',
#   `stock_splits` float NOT NULL COMMENT '股票分割',
#   `update_date` date NOT NULL COMMENT '更新此資料的時間',
#   PRIMARY KEY (`date`,`stock_code`),
#   KEY `idx_date` (`date`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


def get_stock_list() -> list:
    """
    取得股票代碼列表
    """
    df_stock_code = None
    try:
        connection = pymysql.connect(host=config.host,
                                     user=config.user,
                                     password=config.password,
                                     database=config.database,
                                     port=config.port,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        # SQL 查询
        query = """
        SELECT stock_code 
        FROM stock.stock_info si 
        WHERE si.market = '上市' AND si.cfi_code = 'ESVUFR'
        """

        query = """
        SELECT * FROM stock_info WHERE cfi_code = 'ESVUFR' order by stock_code limit 202, 997;
        """

        # 执行查询并获取结果
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            df_stock_code = pd.DataFrame(result)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # 关闭连接
        if 'connection' in locals():
            connection.close()
        stock_code_list = []
        if df_stock_code is not None:
            for index, row in df_stock_code.iterrows():
                stock_code = row['stock_code']
                stock_code_list.append(stock_code)
        return stock_code_list


def get_history_data_write_to_db(stock_code_list: list, api_period: str = "max") -> list:
    """
    從 Yahoo API 取得股票歷史紀錄寫進 DB
    must be one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    """
    if api_period not in ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']:
        raise ValueError("api_period is invalid, must be one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']")

    fail_list = []

    # 使用sqlalchemy创建数据库引擎
    engine = create_engine(f"mysql+pymysql://{config.user}:{config.encoded_password}@{config.host}:{config.port}/{config.database}", pool_pre_ping=True)

    # 迭代股票代码列表
    for stock_code in stock_code_list:
        try:
            stock_code = stock_code.strip()
            stock_id = f"{stock_code}.TW"
            data = yf.Ticker(stock_id)
            df = data.history(period=api_period)
            # 创建列名映射字典
            column_mapping = {col: col.lower().replace(' ', '_') for col in df.columns}
            # 使用 rename 函数重命名列名
            df = df.rename(columns=column_mapping)
            # 使用 reset_index 方法将索引列转换为普通的数据列
            df.reset_index(inplace=True)
            # 将索引列重命名为 "date"
            df.rename(columns={'Date': 'trade_date'}, inplace=True)
            # 新增 stock_code 欄位
            df = df.assign(stock_code=stock_code)
            # 获取当前时间
            current_time = datetime.now()
            # 添加 update_date 列，并将其值设置为当前时间
            df = df.assign(update_date=current_time)

            # 目标列顺序
            columns_order = [
                'stock_code',
                'trade_date',
                'open',
                'high',
                'low',
                'close',
                'volume',
                'dividends',
                'stock_splits',
                'update_date'
            ]

            # 重新排列 DataFrame 的列
            df = df.reindex(columns=columns_order)

            # 检查DataFrame是否为空，如果为空则跳过此次循环
            if df.empty:
                raise ValueError(f"No data found for stock code {stock_code}")

            # 将数据写入数据库表中，插入时忽略已经存在的记录
            temp_table_name = 'temp_historical_data'

            with engine.connect() as connection:
                df.to_sql(temp_table_name, con=connection, if_exists='replace', index=False)
            print("Data inserted successfully")

            # 建立数据库连接
            conn = pymysql.connect(host=config.host, user=config.user, password=config.password, database=config.database, port=config.port)
            try:
                # 获取游标对象
                with conn.cursor() as cursor:

                    # 执行插入查询
                    insert_query = f"""
                        INSERT IGNORE INTO historical_data 
                        SELECT * FROM {temp_table_name}
                    """
                    cursor.execute(insert_query)

                    # 获取插入的记录数
                    num_inserted = cursor.rowcount

                # 提交事务
                conn.commit()
                # 打印成功消息
                print(f"Successfully inserted {num_inserted} records for stock code {stock_code}")
            finally:
                # 关闭连接
                conn.close()
        except Exception as e:
            print(f'e: {e}, stock_code: {stock_code}')
            fail_list.append(stock_code)
        finally:
            # 避免太快
            time.sleep(1)
    return fail_list


if __name__ == "__main__":

    stock_code_list = get_stock_list()
    api_parameter = "5d"
    fail_list = get_history_data_write_to_db(stock_code_list, api_parameter)
    print(f'all failed case: {fail_list}')
    if fail_list:
        retry = 3
        while retry:
            fail_list = get_history_data_write_to_db(fail_list, api_parameter)
            if fail_list:
                print(f'all failed case: {fail_list}')
                retry -= 1
                print(f'retry remain: {retry}')
            else:
                break
