import pandas as pd
import sqlite3
from sql_queries import coins_list_table_query, coins_market_price_table_query
from pycoingecko import CoinGeckoAPI

def get_cryptocoins_data():
    """
    Uses CoinGeckoAPI to download two sets of datasets. First one is list of all coins and the second one is
    market price data of the coins targetted to GBP currency.
    """
    #create a client
    cg=CoinGeckoAPI()
    #check API Server status
    cg.ping()
    #get a list of coins
    coins_list=cg.get_coins_list()
    coins_list_df=pd.DataFrame.from_dict(coins_list)
    #get market data of coins targetted for GBP
    coins_market_data=cg.get_coins_markets(vs_currency='gbp')
    coins_market_data_df=pd.DataFrame(coins_market_data)
    #restrict coins market data to prices
    coins_market_price_df=coins_market_data_df[["id", "symbol", "name", "current_price", "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h"]]
    return coins_list_df, coins_market_price_df

def load_coins_list(conn, coins_list_df):
    """
    Using support of Pandas dataframe's to_sql function to sqlite3 connection
    loading coins_list table
    """
    coins_list_df.to_sql('coins_list', con=conn, if_exists='replace', index=False)
    print(str(coins_list_df.shape[0]) + ' rows loaded into coins_list table')

def load_coins_market_price(conn, coins_market_price_df):
    """
    loading coins_market_price table
    """
    coins_market_price_df.to_sql('coins_market_price', con=conn, if_exists='replace', index=False)
    print(str(coins_market_price_df.shape[0]) + ' rows loaded into coins_market_price table')


def report_coins_list(cur, conn):
    """
    This functions generates report for all the coins not traded in British Pounds
    """
    cur.execute(coins_list_table_query)
    rows=cur.fetchall()
    print('id','\t\t', 'symbol','\t\t','name')
    for row in rows:
        print(row[0],'\t\t',row[1],'\t\t', row[2])

def report_coins_market_price(cur, conn):
    """
    This function generates report for all the coins targetted to British Pounds
    and have percentage price change over last 24 hours greater than 5 for positive and negative numbers.
    """
    cur.execute(coins_market_price_table_query)
    rows=cur.fetchall()
    print('id','\t\t','symbol','\t\t','name', '\t\t', 'price_change_percentage_24h')
    for row in rows:
        print(row[0],'\t\t',row[1],'\t\t', row[2], '\t\t',row[3])
        

def main():
    """
    Initialises connection and cursor objects for sqlite3 database
    Calls get_cryptocoins_data to download the data into two dataframes
    Calls load_coins_list to load one of the dataframe to coins_list table
    """
    conn=sqlite3.connect('pb_db.dbf')
    cur=conn.cursor()
    coins_list_df, coins_market_price_df = get_cryptocoins_data()
    load_coins_list(conn, coins_list_df)
    #report_coins_list(cur, conn)
    load_coins_market_price(conn, coins_market_price_df)
    report_coins_market_price(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()
