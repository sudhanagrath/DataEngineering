# DROP TABLES
coins_list_table_drop = "drop table if exists coins_list;"
coins_market_price_table_drop = "drop table if exists coins_market_price;"


# CREATE TABLES
coins_list_table_create = """CREATE TABLE if not exists coins_list(id text PRIMARY KEY,
                            symbol text,
                            name text
                            );"""

coins_market_price_table_create = """CREATE TABLE if not exists coins_market_price(id text PRIMARY KEY,
                                    symbol text,
                                    name text,
                                    current_price numeric,
                                    high_24h numeric,
                                    low_24h numeric,
                                    price_change_24h numeric,
                                    price_change_percentage_24h numeric
                                    );"""

# SELECT RECORDS
coins_list_table_query = """SELECT id, symbol, name FROM coins_list
                              WHERE id NOT IN(
                                SELECT id FROM coins_market_price)
                            ;"""
coins_market_price_table_query = """SELECT id, symbol, name, price_change_percentage_24h FROM(
                                          SELECT id, symbol, name, current_price, high_24h, low_24h,price_change_24h,
                                            CASE WHEN price_change_24h > 0 AND price_change_percentage_24h > 5.0000 THEN price_change_percentage_24h
                                                   WHEN price_change_24h < 0 AND price_change_percentage_24h > -5.0000 THEN price_change_percentage_24h
                                                   ELSE NULL
                                            END as price_change_percentage_24h 
                                          FROM coins_market_price)
                                    WHERE price_change_percentage_24h is not null;"""

# QUERY LISTS
create_table_queries = [coins_list_table_create, coins_market_price_table_create]
drop_table_queries = [coins_list_table_drop, coins_market_price_table_drop]
