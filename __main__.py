import functions
from fundworthspider import FundWorthData
from ossconnector import OssConnector
from fundanalysis import correlation

if __name__ == '__main__':
    if not functions.test_oss_mode:
        funds = FundWorthData()
        funds.update_fund_code_info()
        funds.get_funds_list(csv_flag=funds.net_worth_csv_flag,csv_df=funds.nw_df)
        # 补充缺失数据
        funds.list_append_lost(funds.nw_df,funds.funds_list)
        for key, values in funds.funds_list.items():
            code = key
            for value in values:
                sdate = value[0]
                edate = value[1]
                funds.get_worth_data(code=code, sdate=sdate, edate=edate)
        print("\r进度： 100.00%")
        # 排序
        funds.nw_df = funds.sort_by_date(funds.nw_df)
        funds.aw_df = funds.sort_by_date(funds.aw_df)
        funds.wi_df = funds.sort_by_date(funds.wi_df)
        # 打印结果
        print(funds.nw_df)
        # 保存文件
        funds.save_to_file()

        correlation()

        #上传文件
        print('-----Uploading files...------')
        ossc = OssConnector()
        ossc.connect()
        ossc.run()
        print("Good Job! OhiyoX.")
    else:
        ossc = OssConnector()
        ossc.connect()
        ossc.run()