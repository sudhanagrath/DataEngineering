This Readme is about the three scripts:

First one is sql_queries.py containing DDL and SELECT queries as strings. It is used to import into other two scripts.

Second one is create\_tables_test.py which connects to sqlite3 database and creates the two tables into it.

It should be executed first using the command

python create\_tables_test.py

Third one is etl_test.py which gets the data from CoinGeckoAPI and loads into the two tables created into sqlite database.

It also generates the two reports using SELECT queries from the sql_queries.py module.

It should be run after the create\_tables_test.py script using the command

python etl_test.py


