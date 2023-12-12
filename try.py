import pymongo
import pandas as pd
from datetime import datetime

class MongoStockData:
    def __init__(self, hostname, port, username, password, database, collection):
        self.client = pymongo.MongoClient(hostname, port, username=username, password=password)
        self.db = self.client[database]
        self.collection = self.db[collection]

    def connect_to_database(self):
        try:
            self.client.server_info()  # 尝试获取数据库信息以检查连接是否成功
            return True
        except pymongo.errors.ServerSelectionTimeoutError:
            return False

    def get_stock_data(self, symbol, start_date, end_date):
        # 查询某只股票某段时间的行情
        query = {
            "ts_code": symbol,
            "trade_date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        cursor = self.collection.find(query)
        data = list(cursor)
        if data:
            return pd.DataFrame(data)
        else:
            return None

    def get_all_stocks_dates(self):
        # 获取所有股票的起始日期和最新日期
        pipeline = [
            {
                "$group": {
                    "_id": "$ts_code",
                    "start_date": {"$min": "$trade_date"},
                    "end_date": {"$max": "$trade_date"}
                }
            }
        ]
        cursor = self.collection.aggregate(pipeline)
        data = list(cursor)
        return pd.DataFrame(data)

    def get_adjust_factor_data(self):
        # 获取每只股票的当日价格、当日复权因子和最新复权因子
        pipeline = [
            {
                "$group": {
                    "_id": "$ts_code",
                    "date": {"$last": "$tradeDate"},
                    "price": {"$last": "$close"},
                    "adj_factor_today": {"$last": "$adjFactor"},
                    "latest_adj_factor": {"$first": "$adjFactor"}
                }
            }
        ]
        cursor = self.collection.aggregate(pipeline)
        data = list(cursor)
        return pd.DataFrame(data)

    def export_to_csv(self, data, filename):
        data.to_csv(filename, index=False)

    def close_connection(self):
        self.client.close()

if __name__ == "__main__":
    mongo = MongoStockData("jztxtech.tpddns.cn", 27011, "reader", "Aa123456", "AShare", "daily")
    if mongo.connect_to_database():
        print("Successfully connected to MongoDB.")
        
        # 2) 获取某只股票某段时间的行情
        symbol = "YOUR_STOCK_SYMBOL"
        start_date = "20220101"
        end_date = "20220131"
        stock_data = mongo.get_stock_data(symbol, start_date, end_date)
        if stock_data is not None:
            print(f"Retrieved data for {symbol} from {start_date} to {end_date}")
            mongo.export_to_csv(stock_data, f"{symbol}_data.csv")
        else:
            print(f"No data found for {symbol} in the specified date range.")
        
        # 3) 获取全体数据库中所有股票的起始日期和最新日期
        all_stock_dates = mongo.get_all_stocks_dates()
        mongo.export_to_csv(all_stock_dates, "all_stocks_dates.csv")
        
        # 4) 获取每只股票的adjust factor数据
        adjust_factor_data = mongo.get_adjust_factor_data()
        mongo.export_to_csv(adjust_factor_data, "adjust_factor_data.csv")
        
        mongo.close_connection()
    else:
        print("Failed to connect to MongoDB. Please check the connection parameters.")
