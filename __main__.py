import functions
from fundworthspider import FundWorthData
from ossconnector import OssConnector

if __name__ == '__main__':
    if not functions.test_oss_mode:
        funds = FundWorthData()
        funds.get_funds_list()
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
        # 打印结果
        print(funds.nw_df)
        # 保存文件
        funds.save_to_file()

        #上传文件
        print('-----Uploading files...------')
        ossc = OssConnector()
        ossc.connect()
        ossc.run()
    else:
        ossc = OssConnector()
        ossc.connect()
        ossc.run()