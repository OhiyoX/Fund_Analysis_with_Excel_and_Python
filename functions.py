import requests
from requests import RequestException

test_oss_mode = False


def concat(delimiter="", *st):
    """concat by delimiter"""
    foo = []
    for s in st:
        foo.append(s)
    return delimiter.join(foo)

def get_page(url, headers=None,params=None):
    """获得网页"""

    # 设置头
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36'
        }
    # 尝试获取
    flag = 10
    while flag > 0:
        try:
            response = requests.get(url, headers=headers,params=params)
            if response.status_code == 200:
                flag = 0
                return response
        except RequestException:
            flag -= 1
            print('retry')
            if flag <= 0:
                return None