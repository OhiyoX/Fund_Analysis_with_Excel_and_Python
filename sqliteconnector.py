import sqlite3
import pandas as pd


def __show_progress(progress,workload):
    print("\r进度： {:.2f}%".format(100 * progress / workload), end='')



"""----------------------------------------------------------------------------"""

class FundSqlite:
    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect("funddata.db")
        return self.conn

    def close(self):
        if self.conn is not None:
            self.conn.close()


def make_fund_info():
    # 从csv导入基金基本信息：代码、名称等
    csv_path = r"D:\Coding\LearnPython\Automatic\Fund-Analysis\resource\fundcode_info\fundcode_search.csv"
    sqlc = FundSqlite()
    sqlc.connect()
    c = sqlc.conn.cursor()
    data = pd.read_csv(csv_path,dtype={'CODE':str})
    for row in data.iterrows():
        id = int(row[1]['CODE'])
        code = row[1]['CODE']
        word = row[1]['WORD']
        name = row[1]['NAME']
        type = row[1]['TYPE']
        exec_script = f"""
        INSERT OR REPLACE INTO FUND_INFO (ID,CODE,WORD,NAME,TYPE)
        VALUES ({id}, '{code}', '{word}', '{name}', '{type}');
        """
        c.execute(exec_script)
    print('Done')
    sqlc.conn.commit()
    sqlc.close()


def make_fund_worth_record():
    #csv导入基金信息记录，净值、收益率等
    sqlc = FundSqlite()
    sqlc.connect()
    c = sqlc.conn.cursor()

    csv_path = r"D:\Coding\LearnPython\Automatic\Fund-Analysis\resource\csv\fund-net-worth-data.csv"
    data = pd.read_csv(csv_path)
    cols = list(data.columns)
    cols = cols[1:]
    date_col = data['DATE']
    for col in cols:
        tmp = data[col]
        for i in range(0, len(date_col)):
            date = date_col[i]
            networth = round(tmp[i],4)
            code =col

            date_f = date.replace('-', '')
            codetime = code + date_f

            exec_script = f"""
            INSERT OR REPLACE INTO FUND_RECORD
            (CODE, DATE, NETWORTH,CODETIME)
            VALUES
            ('{code}','{date}',{networth},{codetime});
            """
            c.execute(exec_script)
    sqlc.conn.commit()
    sqlc.close()

def add_increase_rate():
    # 导入收益率
    sqlc = FundSqlite()
    sqlc.connect()
    c= sqlc.conn.cursor()

    try:
        add_col_script = f"""
        ALTER TABLE FUND_RECORD
        ADD COLUMN INCREASE_RATE REAL;
        """
        c.execute(add_col_script)
    except:
        c.callback()
    sqlc.conn.commit()

    csv_path = r"D:\Coding\LearnPython\Automatic\Fund-Analysis\resource\csv\fund-worth-increase-data.csv"
    data = pd.read_csv(csv_path)
    cols = list(data.columns)
    cols = cols[1:]
    date_col = data['DATE']
    workload = len(date_col)*len(cols)
    progress = 0
    for col in cols:
        tmp = data[col].str.strip('%').astype(float)/100
        for i in range(0,len(date_col)):
            code = col
            date = date_col[i]
            increase_rate = tmp[i]

            # check_sql = f"""
            # SELECT COUNT(*)
            # FROM FUND_RECORD
            # WHERE CODE = '{code}' AND DATE = '{date}'AND INCREASE_RATE IS NULL;
            # """
            # res = c.execute(check_sql).fetchone()
            # if res != 0:
            sql_script = f"""
            UPDATE FUND_RECORD 
            SET INCREASE_RATE = {round(increase_rate,4) if not pd.isna(increase_rate) else 0} 
            WHERE CODE = '{code}' AND DATE = '{date}' AND INCREASE_RATE IS NOT NULL;
            """
            try:
                c.execute(sql_script)
                progress += 1
                __show_progress(progress, workload)
            except Exception as err_info:
                print(err_info)
                print(pd.isna(increase_rate))
                # print(res)
                print(code)
                print(date)
                print(increase_rate)
                print(sql_script)
                exit(0)
    sqlc.conn.commit()
    sqlc.close()

def add_acc_worth():
    # 导入 累计净值
    sqlc = FundSqlite()
    sqlc.connect()
    c= sqlc.conn.cursor()
    try:
        add_col_script = f"""
        ALTER TABLE FUND_RECORD
        ADD COLUMN ACCWORTH REAL;
        """
        c.execute(add_col_script)
    except:
        c.callback()
    sqlc.conn.commit()

    csv_path = r"D:\Coding\LearnPython\Automatic\Fund-Analysis\resource\csv\fund-acc-worth-data.csv"
    data = pd.read_csv(csv_path)
    cols = list(data.columns)
    cols = cols[1:]
    date_col = data['DATE']
    workload = len(date_col)*len(cols)
    progress = 0
    for col in cols:
        tmp = data[col]
        for i in range(0,len(date_col)):
            code = col
            date = date_col[i]
            acc_worth = round(tmp[i],4)

            sql_script = f"""
            UPDATE FUND_RECORD
            SET ACCWORTH = {acc_worth if not pd.isna(acc_worth) else "NULL"} 
            WHERE CODE = '{code}' AND DATE = '{date}' AND ACCWORTH IS NULL;
            """
            try:
                c.execute(sql_script)
                progress += 1
                __show_progress(progress, workload)
            except Exception as err_info:
                print(err_info)
                print(pd.isna(acc_worth))
                # print(res)
                print(code)
                print(date)
                print(acc_worth)
                print(sql_script)
                exit(0)
    sqlc.conn.commit()
    sqlc.close()

if __name__ == '__main__':
    add_acc_worth()