import sys
import json
import re
import os
from datetime import datetime

import functions

# Todo 上传csv到指定目录，一份为名字加日期，一份为名字（即覆盖更新）
# 阿里云OSS的SDK
try:
    import oss2
except ModuleNotFoundError:
    print('Oss2 Module not found, try "pip install oss2". ')
    sys.exit(1)

class OssConnector:


    def __init__(self):
        # 与图床有关的
        with open('danger/oss_info.json') as f:
            self.oss_info = json.load(f)
        self.auth = oss2.Auth(self.oss_info['AccessKeyId'], self.oss_info['AccessKeySecret'])
        self.endpoint = self.oss_info['EndPoint']
        self.bucket = None
        self.bucket_domain = 'https://' + self.oss_info['EndPoint'].replace('https://', self.oss_info['Bucket'] + '.')
        self.remote_folder_path = 'labres/fund-data'
        self.bucket_url = self.bucket_domain

    def connect(self):
        self.bucket = oss2.Bucket(self.auth, self.endpoint, self.oss_info['Bucket'])
        if self.bucket:
            print("oss 连接成功！")

    def get_name_from_path(self,path):
            name = re.search('.*/(.*)', path).group(1)
            return name


    def upload_file(self,to_remote_path, file_local_path,file_name):
        # progress_callback为可选参数，用于实现进度条功能
        def percentage(consumed_bytes, total_bytes):
            if total_bytes:
                rate = int(100 * (float(consumed_bytes) / float(total_bytes)))
                print('\r{0}% '.format(rate), end='')
                sys.stdout.flush()

        result = oss2.resumable_upload(self.bucket,
                              to_remote_path,
                              file_local_path,
                              multipart_threshold=200 * 1024,
                              part_size=100 * 1024,
                              num_threads=3,
                              progress_callback=percentage)
        if result:
            print("\n"+file_name + " 上传成功.")

    def run(self):
        today = datetime.today()
        path_list = []
        def add(t):
            if os.path.exists(t):
                path_list.append(t)
        add("resource/csv/fund-net-worth-data.csv")
        add("resource/csv/fund-net-worth-data-"+datetime.strftime(today,"%Y-%m-%d")+".csv")
        add("resource/csv/fund-acc-worth-data.csv")
        add("resource/csv/fund-acc-worth-data-"+datetime.strftime(today,"%Y-%m-%d")+".csv")
        add("resource/json/fund-worth-data.json")
        add("resource/json/fund-worth-data-" + datetime.strftime(today,"%Y-%m-%d") + '.json')

        if path_list:
            for path in path_list:
                file_name = self.get_name_from_path(path)
                to_remote_path = functions.concat('/',self.remote_folder_path,path)
                self.upload_file(to_remote_path,path,file_name)


