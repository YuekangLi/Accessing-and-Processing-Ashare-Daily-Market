# A-share-daily-market
Use python language, master pymongo and pandas package, connect to database to obtain A-share daily market data and related processing. Customize the code content as a mongo_class package, including the following features:
1. Connect to the database and return the result of connection success or failure;
2. Access the price of a certain stock for a certain period of time;
3. Access the start date and latest date of all stocks in the entire database;
4. The adjusted price of each stock is calculated according to the adjust factor of each stock, that is, the price of the day x the multiple weight factor of the day/the latest multiple weight factor, return open_adjust, high_adjust, low_adjust, close_adjust, and save the results as a csv file.
