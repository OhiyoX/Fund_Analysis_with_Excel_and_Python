import sqlite3
import pandas as pd
import requests

sql_script = f"""SELECT FUND_INFO.NAME, FUND_RECORD.NETWORTH
FROM FUND_INFO,
     FUND_RECORD
WHERE FUND_INFO.CODE = FUND_RECORD.CODE
  AND FUND_RECORD.CODE = 165523;
"""
sql_script2 = f"""
SELECT NAME FROM FUND_INFO;"""


sql_script3 = f"""
SELECT FUND_INFO.NAME, FUND_RECORD.DATE,FUND_RECORD.NETWORTH
FROM FUND_INFO,
     FUND_RECORD
WHERE FUND_INFO.CODE = FUND_RECORD.CODE
  AND FUND_RECORD.CODE = 165523
  AND (DATE between '2019-12-01' and '2020-06-01');
  """

with sqlite3.connect('funddata.db') as db:
    df = pd.read_sql(sql_script3,con=db)
    print(df)


