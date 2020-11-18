import sys
import time
import json
import os
from datetime import datetime
from datetime import timedelta

import pandas as pd
from bs4 import BeautifulSoup

import functions


def str_to_date(str):
    try:
        year, mon, day = str.split('-')
        date = datetime(int(year), int(mon), int(day))
        return date
    except Exception:
        print("日期格式可能不规范！")
        sys.exit(1)


class FundWorthData:
    def __init__(self):
        self.code_list = []  # 简化的基金信息列表，用于快速查询
        self.funds_list = {}  # 记录需要查询的基金code和开始和结束时间
        self.worth_data = {}
        self.progress = 0
        self.workload = 0
        self.url = "http://fund.eastmoney.com/f10/F10DataApi.aspx"
        self.net_worth_csv_flag = True
        self.acc_worth_csv_flag = True
        self.worth_inc_csv_flag = True
        self.json_flag = True


        if os.path.exists("resource/csv/fund-net-worth-data.csv"):
            # net worth 数据
            self.nw_df = pd.read_csv(filepath_or_buffer="resource/csv/fund-net-worth-data.csv", encoding="UTF-8",
                                     dtype={"DATE": str})
        else:
            nw_df = []
            self.nw_df = pd.DataFrame[nw_df]
            self.net_worth_csv_flag = False

        if os.path.exists("resource/csv/fund-acc-worth-data.csv"):
            # acc worth 数据
            self.aw_df = pd.read_csv(filepath_or_buffer="resource/csv/fund-acc-worth-data.csv", encoding="UTF-8",
                                     dtype={"DATE": str})
        else:
            aw_df = []
            self.aw_df = pd.DataFrame[aw_df]
            self.acc_worth_csv_flag = False

        if os.path.exists("resource/csv/fund-worth-increase-data.csv"):
            # worth inc 数据
            self.wi_df = pd.read_csv(filepath_or_buffer="resource/csv/fund-worth-increase-data.csv", encoding="UTF-8",
                                     dtype={"DATE": str})
        else:
            wi_df = []
            self.wi_df = pd.DataFrame[wi_df]
            self.worth_inc_csv_flag = False

        if os.path.exists("resource/json/fund-worth-data.json"):
            with open("resource/json/fund-worth-data.json", encoding="UTF-8") as json_file:
                self.worth_data = json.load(json_file)
        else:
            self.json_flag = False

    def list_append_lost(self,df, funds_list):
        # 用于对加载的csv文件进行分析处理，补全缺失数据
        # 使用该函数要求funds_list 和worth_data 都加载了
        len_df = len(df)
        min_date = max_date = min(df["DATE"])
        for index,row in df.iteritems():
            if index == "DATE":
                continue
            hasna = False
            for i in range(len_df):
                is_na = pd.isna(row[i])
                if not is_na:
                    mmm = row[i]
                    datee = df["DATE"][i]
                if is_na:
                    if not hasna:
                        hasna = True
                        min_date = df["DATE"][i]
                    if hasna:
                        max_date = df["DATE"][i]
                if (not is_na) or (i == len_df - 1):
                    if hasna:
                        if index not in funds_list.keys():
                            funds_list[index] = []
                        funds_list[index].append([min_date,max_date])
                        hasna = False
        return True

    def __show_progress(self):
        print("\r进度： {:.2f}%".format(100 * self.progress / self.workload), end='')

    def __initiate(self, key):
        # 对空字典进行初始化
        self.worth_data[key] = {}
        self.worth_data[key]["head"] = ["净值日期", "单位净值", "累计净值", "日增长率", "申购状态", "赎回状态", "分红送配"]
        self.worth_data[key]["data"] = {}

    def sort_by_date(self, df):
        # 对DataFrame按日期字符串进行排序
        date = pd.to_datetime(df['DATE'], format="%Y-%m-%d")
        date = date.tolist()
        df['d'] = date
        df = df.sort_values('d')
        df = df.drop('d', axis=1)
        return df

    def get_page_data(self, data, url, params):
        #从网页中获取数据
        content = functions.get_page(url, params=params).content
        soup = BeautifulSoup(content, "lxml")

        tbody = soup.find(name="tbody").find_all(name="tr")
        for tdays in tbody:
            tds = tdays.find_all(name="td")
            if tds[0].text == "暂无数据!":
                continue
            date = tds[0].text
            data[date] = []
            if date not in self.nw_df["DATE"].tolist():
                self.nw_df = self.nw_df.append({"DATE": date}, ignore_index=True)
            if date not in self.aw_df["DATE"].tolist():
                self.aw_df = self.aw_df.append({"DATE": date}, ignore_index=True)
            if date not in self.wi_df["DATE"].tolist():
                self.wi_df = self.wi_df.append({"DATE": date}, ignore_index=True)

            nw_index = self.nw_df[self.nw_df["DATE"] == date].index.tolist()
            aw_index = self.aw_df[self.aw_df["DATE"] == date].index.tolist()
            wi_index = self.wi_df[self.wi_df["DATE"] == date].index.tolist()
            for i in range(len(tds)):
                data[date].append(tds[i].text)
                flag = False
                if i == 1:
                    self.nw_df.loc[nw_index, params["code"]] = tds[i].text
                    flag = True
                if i == 2:
                    self.aw_df.loc[aw_index, params["code"]] = tds[i].text
                    flag = True
                if i == 3:
                    self.wi_df.loc[wi_index, params["code"]] = tds[i].text
                    flag = True
                if flag:
                    self.progress += 0.3333333
        return data

    def get_worth_data(self, code, page=None, per=None, sdate="", edate=""):
        # code 是基金代码
        # page 页码
        # per的跨度在5-40之间，以5为单位
        # sdate是开始时间
        # edate是结束时间，一般情况
        if code not in self.code_list:
            self.__initiate(code)

        params = {}
        params["type"] = "lsjz"
        params["code"] = code

        flag_more_pages = False
        all_pages = 1
        value_sdate = 0
        value_edate = 0
        interval_days = 0

        if sdate:
            value_sdate = str_to_date(sdate)
            params["sdate"] = sdate
        if edate:
            value_edate = str_to_date(edate)
            params["edate"] = edate
        if value_sdate != 0 and value_edate != 0:
            if value_edate >= value_sdate:
                if (sdate != "") and (edate != ""):
                    interval_days = (value_edate - value_sdate).days
                    # 间距为天数
            else:
                print("日期间距错误！")
                sys.exit(1)

        if page:
            params["page"] = page
        if per and interval_days <= 10:
            pass
        elif interval_days > 0:
            if interval_days <= 40:
                per = interval_days if (interval_days % 10 == 0) else (interval_days / 10 + 1) * 10
            else:
                flag_more_pages = True
                per = 40
                all_pages = interval_days / 40 if (interval_days % 40 == 0) else (int(interval_days / 40) + 1)
        if per:
            params["per"] = per

        """
        四、基金数据来源
    需要获得3类数据，数据均来自天天基金网。
    (1)基金列表
    http://fund.eastmoney.com/js/fundcode_search.js
    格式：["000001","HXCZ","华夏成长","混合型","HUAXIACHENGZHANG"]
    
    (2)基金净值数据
    http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=377240
    http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=160220&page=1
    http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=160220&page=1&per=50
    http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=377240&page=1&per=20&sdate=2017-03-01&edate=2017-03-01
    
    格式：var apidata={ content:"<table class='w782 comm lsjz'><th><tr><th class='first'>净值日期</th><th>单位净值</th><th>累计净值</th><th>日增长率</th><th>申购状态</th><th>赎回状态</th><th class='tor last'>分红送配</th></tr></th><tbody><tr><td>2017-03-01</td><td class='tor bold'>2.1090</td><td class='tor bold'>2.1090</td><td class='tor bold red'>0.29%</td><td>开放申购</td><td>开放赎回</td><td class='red unbold'></td></tr></tbody></table>",records:1,pages:1,curpage:1};
    
    格式化以后：
    净值日期	单位净值	累计净值	日增长率	申购状态	赎回状态	分红送配
    2017-03-01	2.1090	2.1090			0.29%		开放申购	开放赎回
    
    (3)基金增幅排名
    http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=gp&rs=&gs=0&sc=zzf&st=desc&sd=2016-03-29&ed=2017-03-29&qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1&v=0.6370068000914493
    ft： fund type类型 所有-all 股票型-gp 混合型-hh 债券型-zq 指数型-zs 保本型-bb QDII-qdii LOF-lof
    
    
    更多筛选
    http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=3yzf,50&gs=0&sc=3yzf&st=desc&sd=2016-03-29&ed=2017-03-29&qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1&v=0.013834315347261095
    http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=6yzf,20&gs=0&sc=6yzf&st=desc&sd=2016-03-29&ed=2017-03-29&qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1&v=0.5992681832027366
    http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=1nzf,20&gs=0&sc=1nzf&st=desc&sd=2016-03-29&ed=2017-03-29&qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1&v=0.6093838416906625
    
    rs=3yzf,50 近3月涨幅排名前50
    rs=1nzf,20 近1年涨幅排名前20
        """

        if not flag_more_pages:
            self.get_page_data(self.worth_data[code]["data"], self.url, params)
            self.__show_progress()
            time.sleep(0.1)
        else:
            for i in range(1, all_pages + 1):
                params["page"] = i
                self.get_page_data(self.worth_data[code], self.url, params)
                self.__show_progress()
                time.sleep(0.1)


    def save_to_file(self,mode="all"):
        # 将清洗和整理后的数据保存在本地
        today = datetime.today()
        # save_time = datetime.strftime(today,"%Y-%m-%d-%H-%M-%S")
        with open('resource/json/fund-worth-data.json', 'w', encoding="UTF-8") as t_json:
            json.dump(self.worth_data, t_json, ensure_ascii=False)
        with open('resource/json/fund-worth-data-' + datetime.strftime(today,"%Y-%m-%d") + '.json', 'w', encoding="UTF-8") as t_json:
            json.dump(self.worth_data, t_json, ensure_ascii=False)
        if mode =="all" or mode == "networth":
            self.nw_df.to_csv('resource/csv/fund-net-worth-data.csv', index=False, sep=',', encoding="UTF-8",float_format='%.4f')
            self.nw_df.to_csv('resource/csv/fund-net-worth-data-'+datetime.strftime(today,"%Y-%m-%d")+'.csv',
                              index=False, sep=',', encoding="UTF-8", float_format='%.4f')
        if mode =="all" or mode == "accworth":
            self.aw_df.to_csv('resource/csv/fund-acc-worth-data.csv', index=False, sep=',', encoding="UTF-8",float_format='%.4f')
            self.aw_df.to_csv('resource/csv/fund-acc-worth-data-' + datetime.strftime(today, "%Y-%m-%d") + '.csv',
                              index=False, sep=',', encoding="UTF-8",float_format='%.4f')
        if mode =="all" or mode == "worthinc":
            self.wi_df.to_csv('resource/csv/fund-worth-increase-data.csv', index=False, sep=',', encoding="UTF-8",float_format='%.4f')
            self.wi_df.to_csv('resource/csv/fund-worth-increase-data-' + datetime.strftime(today, "%Y-%m-%d") + '.csv',
                              index=False, sep=',', encoding="UTF-8",float_format='%.4f')

    def get_funds_list(self,csv_flag,csv_df):
        # 用于从Fund-List.csv清单中获取基金信息
        with open('Set-Fund-List.csv', encoding="UTF-8") as f_list:
            f_list = pd.read_csv(f_list, encoding="UTF-8", dtype={"基金代码": str})
        f_list = f_list.fillna("")
        funds_list = {}
        for i in range(len(f_list)):
            funds_list[f_list.iat[i, 0]] = []
            if f_list.iat[i,1]:
                start_date = str_to_date(f_list.iat[i,1])
                if start_date.weekday()==5:
                    f_list.iat[i, 1] = datetime.strftime(start_date+timedelta(days=-1),"%Y-%m-%d")
                elif start_date.weekday()==6:
                    f_list.iat[i, 1] = datetime.strftime(start_date+timedelta(days=-2),"%Y-%m-%d")

            if f_list.iat[i, 2] == "":
                today_datetime = datetime.today()
                today = datetime.strftime(today_datetime,"%Y-%m-%d")
                # 避开周末
                if today_datetime.weekday()==5:
                    f_list.iat[i, 2] = datetime.strftime(today_datetime+timedelta(days=-1),"%Y-%m-%d")
                elif today_datetime.weekday()==6:
                    f_list.iat[i, 2] = datetime.strftime(today_datetime+timedelta(days=-2),"%Y-%m-%d")
                else:
                    f_list.iat[i, 2] = today
            funds_list[f_list.iat[i, 0]].append([f_list.iat[i, 1], f_list.iat[i, 2]])

        if csv_flag:
            # 以净值表为参考
            min_csv_date = min(csv_df["DATE"])
            max_csv_date = max(csv_df["DATE"])
            funds_list_keys = list(funds_list)
            # 循环切片基金信息以完成时间整体跨度的获取
            for key in funds_list_keys:
                # 提高检索效率
                flag = False
                if funds_list[key][0][0] < min_csv_date and funds_list[key][0][1] <= max_csv_date:
                    funds_list[key][0][1] = min_csv_date
                    flag = True
                elif funds_list[key][0][1] > max_csv_date and funds_list[key][0][0] >= min_csv_date:
                    funds_list[key][0][0] = max_csv_date
                    flag = True
                elif funds_list[key][0][0] >= min_csv_date and funds_list[key][0][1] <= max_csv_date:
                    del funds_list[key]
                elif funds_list[key][0][0] < min_csv_date and funds_list[key][0][1] > max_csv_date:
                    flag = True
                    temp = []
                    temp.append([funds_list[key][0][0], min_csv_date])
                    temp.append([max_csv_date, funds_list[key][0][1]])
                    funds_list[key] = temp
                if flag == True:
                    self.code_list.append(key)
                    self.__initiate(key)

        self.funds_list = funds_list
        self.calc_workload()
        return self.funds_list

    def calc_workload(self):
        # 计算工作量
        print("计算工作量...")
        for value in self.funds_list.values():
            for seg_interval in value:
                self.workload += (str_to_date(seg_interval[1]) - str_to_date(seg_interval[0])).days
        print("完成，需要{}条查询.".format(self.workload))


