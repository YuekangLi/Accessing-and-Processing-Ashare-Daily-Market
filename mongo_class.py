import pymongo
import pandas as pd

class MongoClient:
    def __init__(self, host, port, username, password, database, collection):
        self.client = pymongo.MongoClient(host, port, username=username, password=password)
        self.db = self.client[database]
        self.collection = self.db[collection]

    def test_connection(self):
        try:
            self.client.server_info()
            return True
        except Exception:
            return False

    def get_stock_data(self, symbol, start_date, end_date):
        query = {
            'code': symbol,
            'tradeDate': {'$gte': start_date, '$lte': end_date}
        }
        data = list(self.collection.find(query))
        data=pd.DataFrame(data)
        return data

    def get_all_stocks_date_range(self):
        pipeline = [
            {
                '$group': {
                    '_id': '$code',
                    'open_date': {'$min': '$tradeDate'},
                    'latest_date': {'$max': '$tradeDate'}
                }
            }
        ]
        result = list(self.collection.aggregate(pipeline))
        result=pd.DataFrame(result)
        return result

    def calculate_adjusted_price(self, symbol, start_date, end_date):
        df = self.get_stock_data(symbol, start_date, end_date)
        df["factor"] = df["adjFactor"]
        latest_factor = df["factor"].iloc[-1]
        print(latest_factor)
        df["open_adjust"] = df["open"] * df["factor"] / latest_factor
        df["high_adjust"] = df["high"] * df["factor"] / latest_factor
        df["low_adjust"] = df["low"] * df["factor"] / latest_factor
        df["close_adjust"] = df["close"] * df["factor"] / latest_factor
        adjust=df[["_id","code","tradeDate", "open_adjust","high_adjust","low_adjust","close_adjust"]]

        return adjust
    
    