import sqlite3
import re
import datetime


def dict_factory(cursor, row):
    col_names = [col[0] for col in cursor.description]
    return {key: value for key, value in zip(col_names, row)}

con = sqlite3.connect("Data.db")
con.row_factory = dict_factory
cur = con.cursor()

# Import client -> subscription
for row in cur.execute("SELECT AccountNumber, AccountName, Type, Amount Due, FROM print"):
    
